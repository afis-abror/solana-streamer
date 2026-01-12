use crate::common::AnyResult;
use crate::protos::shreder::shreder_service_client::ShrederServiceClient;
use crate::protos::shreder::SubscribeEntriesRequest;
use crate::streaming::common::{
    process_shred_transaction, MetricsManager, StreamClientConfig, SubscriptionHandle,
};
use crate::streaming::event_parser::common::filter::EventTypeFilter;
use crate::streaming::event_parser::common::high_performance_clock::get_high_perf_clock;
use crate::streaming::event_parser::{DexEvent, Protocol};
use crate::streaming::shred::factory;
use crate::streaming::storage::TransactionStorage;
use log::{error, info, warn};
use solana_entry::entry::Entry;
use solana_sdk::{pubkey::Pubkey, transaction::VersionedTransaction};
use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;
use tokio::time::{sleep, timeout};
use tonic::transport::{Channel, Endpoint};

/// Shreder gRPC streaming client for transaction subscriptions
#[derive(Clone)]
pub struct ShrederClient {
    pub shredstream_client: Arc<ShrederServiceClient<Channel>>,
    pub config: StreamClientConfig,
    pub subscription_handle: Arc<Mutex<Option<SubscriptionHandle>>>,
    pub transactions: Arc<TransactionStorage>,
    pub endpoint: String,
    pub local_addr: Option<IpAddr>,
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
        })
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

        // Create the subscription request for entries (no filters, client-side filtering)
        let request = SubscribeEntriesRequest {};

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

                // Attempt to create stream
                let stream_result = timeout(
                    Duration::from_secs(connection_config.request_timeout),
                    client.subscribe_entries(tonic::Request::new(request.clone())),
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

                info!("Successfully connected and subscribed to shreder entries stream");
                retry_attempt = 0; // Reset retry counter on successful connection

                // Process stream messages
                let stream_broken = loop {
                    use futures::StreamExt;
                    match stream.next().await {
                        Some(Ok(message)) => {
                            // Capture receive time
                            let receive_us = get_high_perf_clock();
                            
                            // Deserialize entries from binary data
                            if let Ok(entries) = bincode::deserialize::<Vec<Entry>>(&message.entries) {
                                for entry in entries {
                                    for transaction in entry.transactions {
                                        // Store transaction in storage
                                        if !transaction.signatures.is_empty() {
                                            transactions
                                                .insert(
                                                    transaction.signatures[0].to_string(),
                                                    transaction.clone(),
                                                )
                                                .await;
                                        }
                                        
                                        // Create pooled transaction with slot
                                        let transaction_with_slot =
                                            factory::create_transaction_with_slot_pooled(
                                                transaction.clone(),
                                                message.slot,
                                                receive_us,
                                            );

                                        // Process transaction with filters
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
                                }
                            } else {
                                warn!("Failed to deserialize entries from message");
                            }
                        }
                        Some(Err(e)) => {
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
                        None => {
                            warn!("Stream ended unexpectedly");
                            break true; // Stream ended, need to reconnect
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
