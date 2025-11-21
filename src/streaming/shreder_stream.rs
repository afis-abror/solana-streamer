use crate::common::AnyResult;
use crate::protos::shreder;
use crate::protos::shreder::{
    shreder_service_client::ShrederServiceClient, SubscribeRequestFilterTransactions,
    SubscribeTransactionsRequest,
};
use crate::streaming::blocktime::BlockTimeCache;
use crate::streaming::common::{
    process_shred_transaction, MetricsManager, StreamClientConfig, SubscriptionHandle,
};
use crate::streaming::event_parser::common::filter::EventTypeFilter;
use crate::streaming::event_parser::common::high_performance_clock::get_high_perf_clock;
use crate::streaming::event_parser::{DexEvent, Protocol};
use crate::streaming::shred::factory;
use crate::streaming::storage::TransactionStorage;
use anyhow;
use chrono::{DateTime, Utc};
use futures::SinkExt;
use log::{error, info, warn};
use solana_sdk::{
    hash::Hash,
    message::compiled_instruction::CompiledInstruction,
    message::{Message, MessageHeader, VersionedMessage},
    pubkey::Pubkey,
    signature::Signature,
    transaction::VersionedTransaction,
};
use std::collections::HashMap;
use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;
use tokio::time::{sleep, timeout};
use tonic::transport::{Channel, Endpoint};

/// Comprehensive transaction age metrics
#[derive(Debug, Clone)]
pub struct TransactionAgeMetrics {
    /// When the transaction was received (UTC)
    pub received_at: chrono::DateTime<chrono::Utc>,
    /// Transaction receive timestamp (high-perf clock, microseconds)
    pub receive_timestamp_us: i64,
    /// When Shreder created/sent the transaction (for reference only)
    pub shreder_created_at: Option<chrono::DateTime<chrono::Utc>>,
    /// Observed delay between Shreder timestamp and receive time
    /// WARNING: This includes clock drift and may not reflect actual network latency
    pub observed_delay_us: i64,
    /// Transaction slot number
    pub slot: u64,
    /// Recent blockhash from the transaction
    pub recent_blockhash: Hash,
}

/// Shreder gRPC streaming client for transaction subscriptions
#[derive(Clone)]
pub struct ShrederClient {
    pub shredstream_client: Arc<ShrederServiceClient<Channel>>,
    pub config: StreamClientConfig,
    pub subscription_handle: Arc<Mutex<Option<SubscriptionHandle>>>,
    pub transactions: Arc<TransactionStorage>,
    pub endpoint: String,
    pub local_addr: Option<IpAddr>,
    pub block_time_cache: Option<BlockTimeCache>,
    pub transactions_by_slot: Arc<Mutex<HashMap<u64, Vec<(String, DateTime<Utc>)>>>>,
    pub rpc_endpoint: Option<String>,
}

impl ShrederClient {
    pub async fn new(endpoint: String) -> AnyResult<Self> {
        Self::new_with_config(endpoint, StreamClientConfig::default()).await
    }

    pub async fn new_with_config(endpoint: String, config: StreamClientConfig) -> AnyResult<Self> {
        Self::new_with_config_and_local_addr(endpoint, config, None).await
    }

    pub async fn new_with_local_addr(endpoint: String, local_addr: IpAddr) -> AnyResult<Self> {
        Self::new_with_config_and_local_addr(
            endpoint,
            StreamClientConfig::default(),
            Some(local_addr),
        )
        .await
    }

    pub async fn new_with_config_and_local_addr(
        endpoint: String,
        config: StreamClientConfig,
        local_addr: Option<IpAddr>,
    ) -> AnyResult<Self> {
        let shredstream_client = Self::create_client(&endpoint, local_addr.as_ref()).await?;
        MetricsManager::init(config.enable_metrics);
        Ok(Self {
            shredstream_client: Arc::new(shredstream_client),
            config,
            subscription_handle: Arc::new(Mutex::new(None)),
            transactions: Arc::new(TransactionStorage::new()),
            endpoint,
            local_addr,
            block_time_cache: None,
            transactions_by_slot: Arc::new(Mutex::new(HashMap::new())),
            rpc_endpoint: None,
        })
    }

    pub async fn new_with_storage(
        endpoint: String,
        config: StreamClientConfig,
        storage: Arc<TransactionStorage>,
    ) -> AnyResult<Self> {
        Self::new_with_storage_and_local_addr(endpoint, config, storage, None).await
    }

    pub async fn new_with_storage_and_local_addr(
        endpoint: String,
        config: StreamClientConfig,
        storage: Arc<TransactionStorage>,
        local_addr: Option<IpAddr>,
    ) -> AnyResult<Self> {
        let shredstream_client = Self::create_client(&endpoint, local_addr.as_ref()).await?;
        MetricsManager::init(config.enable_metrics);
        Ok(Self {
            shredstream_client: Arc::new(shredstream_client),
            config,
            subscription_handle: Arc::new(Mutex::new(None)),
            transactions: storage,
            endpoint,
            local_addr,
            block_time_cache: None,
            transactions_by_slot: Arc::new(Mutex::new(HashMap::new())),
            rpc_endpoint: None,
        })
    }

    /// Enable latency monitoring with RPC endpoint
    pub fn enable_latency_monitoring(&mut self, rpc_endpoint: String) {
        self.rpc_endpoint = Some(rpc_endpoint.clone());
        self.block_time_cache = Some(BlockTimeCache::new(&rpc_endpoint));
    }

    async fn create_client(
        endpoint: &str,
        local_addr: Option<&IpAddr>,
    ) -> AnyResult<ShrederServiceClient<Channel>> {
        if let Some(addr) = local_addr {
            let addr_owned = *addr;

            // Use connect_with_connector but do the binding properly
            let channel = Endpoint::from_shared(endpoint.to_string())?
                .connect_with_connector(tower::service_fn(move |uri: tonic::transport::Uri| {
                    async move {
                        let host = uri.host().ok_or_else(|| {
                            std::io::Error::new(std::io::ErrorKind::InvalidInput, "Missing host")
                        })?;
                        let port = uri.port_u16().unwrap_or(50051);

                        // Resolve the hostname to IP
                        let remote_addr: SocketAddr =
                            tokio::net::lookup_host(format!("{}:{}", host, port))
                                .await?
                                .next()
                                .ok_or_else(|| {
                                    std::io::Error::new(
                                        std::io::ErrorKind::NotFound,
                                        "Could not resolve hostname",
                                    )
                                })?;

                        // Create socket with the appropriate domain
                        let domain = if remote_addr.is_ipv4() {
                            socket2::Domain::IPV4
                        } else {
                            socket2::Domain::IPV6
                        };

                        let socket = socket2::Socket::new(
                            domain,
                            socket2::Type::STREAM,
                            Some(socket2::Protocol::TCP),
                        )?;

                        socket.set_reuse_address(true)?;
                        socket.set_nodelay(true)?;

                        // Bind to local address with port 0 (let OS choose)
                        let bind_addr = SocketAddr::new(addr_owned, 0);
                        socket.bind(&bind_addr.into())?;

                        // Connect in blocking mode first
                        socket.connect(&remote_addr.into())?;

                        // Convert to std stream and set non-blocking
                        let std_stream: std::net::TcpStream = socket.into();
                        std_stream.set_nonblocking(true)?;

                        // Convert to tokio stream
                        let tokio_stream = tokio::net::TcpStream::from_std(std_stream)?;

                        // Wrap with hyper_util::rt::TokioIo
                        Ok::<_, std::io::Error>(hyper_util::rt::TokioIo::new(tokio_stream))
                    }
                }))
                .await?;

            Ok(ShrederServiceClient::new(channel))
        } else {
            // No local address specified, use default connection
            Ok(ShrederServiceClient::connect(endpoint.to_string()).await?)
        }
    }

    pub async fn shredstream_subscribe<F>(
        &self,
        protocols: Vec<Protocol>,
        bot_wallet: Option<Pubkey>,
        event_type_filter: Option<EventTypeFilter>,
        callback: F,
    ) -> AnyResult<()>
    where
        F: Fn(DexEvent) + Send + Sync + 'static,
    {
        self.stop().await;

        let mut metrics_handle = None;
        if self.config.enable_metrics {
            metrics_handle = MetricsManager::global().start_auto_monitoring().await;
        }

        // Clone necessary data for the stream task
        let auto_reconnect_config = self.config.auto_reconnect.clone();
        let endpoint = self.endpoint.clone();
        let local_addr = self.local_addr;
        let connection_config = self.config.connection.clone();
        let protocols_clone = protocols.clone();
        let callback = Arc::new(callback);
        let transactions = self.transactions.clone();
        let transactions_by_slot = self.transactions_by_slot.clone();
        let block_time_cache = self.block_time_cache.clone();

        // Start latency monitoring task if enabled
        let latency_handle = if let Some(cache) = block_time_cache.clone() {
            let transactions_by_slot_clone = transactions_by_slot.clone();
            Some(tokio::spawn(async move {
                crate::streaming::blocktime::latency_monitor_task(cache, transactions_by_slot_clone).await;
            }))
        } else {
            None
        };

        // Create the subscription request
        let mut transaction_filters = HashMap::new();
        for protocol in &protocols {
            let program_ids = protocol.get_program_id();
            let program_id_strings: Vec<String> =
                program_ids.iter().map(|pubkey| pubkey.to_string()).collect();

            let filter_name = match protocol {
                Protocol::PumpFun => "pumpfun",
                Protocol::PumpSwap => "pumpswap",
                Protocol::RaydiumAmmV4 => "raydium_amm_v4",
                Protocol::RaydiumClmm => "raydium_clmm",
                Protocol::RaydiumCpmm => "raydium_cpmm",
                Protocol::Bonk => "bonk",
            };

            transaction_filters.insert(
                filter_name.to_owned(),
                SubscribeRequestFilterTransactions {
                    account_exclude: vec![],
                    account_include: vec![],
                    account_required: program_id_strings,
                },
            );
        }

        let request = SubscribeTransactionsRequest { transactions: transaction_filters };

        let stream_task = tokio::spawn(async move {
            let mut retry_attempt = 0u32;

            loop {
                // Try to establish connection
                let client = if retry_attempt == 0 {
                    // First attempt - try to use existing connection or create new one
                    match Self::create_client(&endpoint, local_addr.as_ref()).await {
                        Ok(client) => client,
                        Err(e) => {
                            error!("Failed to connect to shreder service: {:?}", e);
                            if !auto_reconnect_config.enabled {
                                break;
                            }
                            retry_attempt += 1;
                            continue;
                        }
                    }
                } else {
                    // Reconnection attempt
                    if auto_reconnect_config.max_retries > 0
                        && retry_attempt > auto_reconnect_config.max_retries
                    {
                        error!(
                            "Max reconnection attempts ({}) exceeded",
                            auto_reconnect_config.max_retries
                        );
                        break;
                    }

                    let delay_ms = std::cmp::min(
                        (auto_reconnect_config.initial_delay_ms as f64
                            * auto_reconnect_config
                                .backoff_multiplier
                                .powi((retry_attempt - 1) as i32)) as u64,
                        auto_reconnect_config.max_delay_ms,
                    );

                    warn!("Reconnecting in {}ms (attempt {})...", delay_ms, retry_attempt);
                    sleep(Duration::from_millis(delay_ms)).await;

                    match timeout(
                        Duration::from_secs(connection_config.connect_timeout),
                        Self::create_client(&endpoint, local_addr.as_ref()),
                    )
                    .await
                    {
                        Ok(Ok(client)) => {
                            info!("Successfully reconnected to shreder service");
                            client
                        }
                        Ok(Err(e)) => {
                            error!("Failed to reconnect: {:?}", e);
                            retry_attempt += 1;
                            continue;
                        }
                        Err(_) => {
                            error!("Connection timeout during reconnect");
                            retry_attempt += 1;
                            continue;
                        }
                    }
                };

                let mut client = client;
                let (mut subscribe_tx, subscribe_rx) =
                    futures::channel::mpsc::unbounded::<SubscribeTransactionsRequest>();

                // Attempt to create stream
                let stream_result = timeout(
                    Duration::from_secs(connection_config.request_timeout),
                    client.subscribe_transactions(subscribe_rx),
                )
                .await;

                let mut stream = match stream_result {
                    Ok(Ok(response)) => response.into_inner(),
                    Ok(Err(e)) => {
                        error!("Failed to create subscription stream: {:?}", e);
                        if !auto_reconnect_config.enabled {
                            break;
                        }
                        retry_attempt += 1;
                        continue;
                    }
                    Err(_) => {
                        error!("Timeout creating subscription stream");
                        if !auto_reconnect_config.enabled {
                            break;
                        }
                        retry_attempt += 1;
                        continue;
                    }
                };

                // Send the initial request
                if let Err(e) = subscribe_tx.send(request.clone()).await {
                    error!("Failed to send subscription request: {:?}", e);
                    if !auto_reconnect_config.enabled {
                        break;
                    }
                    retry_attempt += 1;
                    continue;
                }

                info!("Successfully connected and subscribed to shreder stream");
                retry_attempt = 0; // Reset retry counter on successful connection

                // Process stream messages
                let stream_broken = loop {
                    match stream.message().await {
                        Ok(Some(message)) => {
                            // Capture receive time IMMEDIATELY with high-perf clock
                            let receive_us = get_high_perf_clock();
                            
                            if let Some(transaction_update) = &message.transaction {
                                let slot = transaction_update.slot;
                                
                                // Prepare log message for latency monitoring
                                if block_time_cache.is_some() {
                                    crate::streaming::blocktime::prepare_log_message(
                                        slot,
                                        &transactions_by_slot,
                                    )
                                    .await;
                                }
                                
                                if let Some(shreder_tx) = transaction_update.transaction.as_ref() {
                                    let versioned_tx =
                                        convert_shreder_to_versioned_transaction(shreder_tx);
                                    let versioned_tx = match versioned_tx {
                                        Ok(vtx) => vtx,
                                        Err(e) => {
                                            error!(
                                                "Failed to convert Shreder transaction: {:?}",
                                                e
                                            );
                                            continue;
                                        }
                                    };

                                    if versioned_tx.signatures.is_empty() {
                                        continue;
                                    }


                                    transactions
                                        .insert(
                                            versioned_tx.signatures[0].to_string(),
                                            versioned_tx.clone(),
                                        )
                                        .await;
                                    let transaction_with_slot =
                                        factory::create_transaction_with_slot_pooled(
                                            versioned_tx,
                                            transaction_update.slot,
                                            receive_us,
                                        );

                                    if let Err(e) = process_shred_transaction(
                                        transaction_with_slot,
                                        &protocols_clone,
                                        event_type_filter.as_ref(),
                                        callback.clone(),
                                        bot_wallet,
                                    )
                                    .await
                                    {
                                        error!("Error handling message: {e:?}");
                                    }
                                }
                            } else {
                                warn!("Received message without transaction data");
                            }
                        }
                        Ok(None) => {
                            warn!("Stream ended unexpectedly");
                            break true; // Stream ended, need to reconnect
                        }
                        Err(e) => {
                            error!("Stream error: {:?}", e);
                            // Check if this is a connection error that warrants reconnection
                            let error_str = e.to_string().to_lowercase();
                            if error_str.contains("broken pipe")
                                || error_str.contains("connection")
                                || error_str.contains("h2 protocol error")
                                || error_str.contains("stream closed")
                            {
                                warn!(
                                    "Connection-related error detected, will attempt to reconnect"
                                );
                                break true; // Connection error, need to reconnect
                            } else {
                                error!("Non-recoverable stream error: {:?}", e);
                                break false; // Non-recoverable error, exit
                            }
                        }
                    }
                };

                if !stream_broken || !auto_reconnect_config.enabled {
                    break;
                }

                retry_attempt += 1;
                warn!("Stream connection lost, preparing to reconnect...");
            }

            info!("Shreder stream task ended");
            
            // Abort latency monitoring task if it exists
            if let Some(handle) = latency_handle {
                handle.abort();
            }
        });

        let subscription_handle = SubscriptionHandle::new(stream_task, None, metrics_handle);
        let mut handle_guard = self.subscription_handle.lock().await;
        *handle_guard = Some(subscription_handle);

        Ok(())
    }

    /// Stop the streaming
    pub async fn stop(&self) {
        let mut handle_guard = self.subscription_handle.lock().await;
        if let Some(handle) = handle_guard.take() {
            handle.stop();
        }
    }

    /// Get transaction from storage
    pub async fn get_transaction(&self, signature: &str) -> Option<VersionedTransaction> {
        self.transactions.get(signature).await
    }
}

fn convert_shreder_to_versioned_transaction(
    shreder_tx: &shreder::Transaction,
) -> AnyResult<solana_sdk::transaction::VersionedTransaction> {
    // Convert signatures
    let mut signatures = Vec::new();
    for sig_bytes in shreder_tx.signatures.clone() {
        if sig_bytes.len() != 64 {
            return Err(anyhow::anyhow!("Invalid signature length: {}", sig_bytes.len()));
        }
        let mut sig_array = [0u8; 64];
        sig_array.copy_from_slice(&sig_bytes);
        signatures.push(Signature::from(sig_array));
    }

    // Extract message
    let shreder_msg = shreder_tx
        .message
        .clone()
        .ok_or_else(|| anyhow::anyhow!("Missing message in transaction"))?;

    // Convert message header
    let header = shreder_msg.header.ok_or_else(|| anyhow::anyhow!("Missing header in message"))?;

    let message_header = MessageHeader {
        num_required_signatures: header.num_required_signatures as u8,
        num_readonly_signed_accounts: header.num_readonly_signed_accounts as u8,
        num_readonly_unsigned_accounts: header.num_readonly_unsigned_accounts as u8,
    };

    // Convert account keys
    let mut account_keys = Vec::new();
    for key_bytes in shreder_msg.account_keys {
        if key_bytes.len() != 32 {
            return Err(anyhow::anyhow!("Invalid pubkey length: {}", key_bytes.len()));
        }
        account_keys.push(Pubkey::new_from_array(key_bytes.try_into().unwrap()));
    }

    // Convert recent blockhash
    if shreder_msg.recent_blockhash.len() != 32 {
        return Err(anyhow::anyhow!(
            "Invalid blockhash length: {}",
            shreder_msg.recent_blockhash.len()
        ));
    }
    let mut hash_array = [0u8; 32];
    hash_array.copy_from_slice(&shreder_msg.recent_blockhash);
    let recent_blockhash = Hash::new_from_array(hash_array);

    // Convert instructions
    let mut instructions = Vec::new();
    for shreder_ix in shreder_msg.instructions {
        let compiled_ix = CompiledInstruction {
            program_id_index: shreder_ix.program_id_index as u8,
            accounts: shreder_ix.accounts,
            data: shreder_ix.data,
        };
        instructions.push(compiled_ix);
    }

    // Create the message based on whether it's versioned
    let versioned_message = if shreder_msg.versioned {
        // For V0 messages with address table lookups
        use solana_sdk::message::v0;

        let mut address_table_lookups = Vec::new();
        for lookup in shreder_msg.address_table_lookups {
            if lookup.account_key.len() != 32 {
                return Err(anyhow::anyhow!("Invalid lookup account key length"));
            }
            let mut key_array = [0u8; 32];
            key_array.copy_from_slice(&lookup.account_key);
            let lookup_key = Pubkey::new_from_array(key_array);
            address_table_lookups.push(v0::MessageAddressTableLookup {
                account_key: lookup_key,
                writable_indexes: lookup.writable_indexes,
                readonly_indexes: lookup.readonly_indexes,
            });
        }

        let v0_message = v0::Message {
            header: message_header,
            account_keys,
            recent_blockhash,
            instructions,
            address_table_lookups,
        };
        VersionedMessage::V0(v0_message)
    } else {
        // Legacy message
        let legacy_message =
            Message { header: message_header, account_keys, recent_blockhash, instructions };
        VersionedMessage::Legacy(legacy_message)
    };

    Ok(VersionedTransaction { signatures, message: versioned_message })
}
