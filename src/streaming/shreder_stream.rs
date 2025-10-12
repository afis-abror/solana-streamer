use std::collections::HashMap;
use std::sync::Arc;
use futures::SinkExt;
use tokio::sync::Mutex;
use tonic::transport::Channel;
use crate::protos::shreder;
use crate::protos::shreder::{
    shreder_service_client::ShrederServiceClient,
    SubscribeRequestFilterTransactions, 
    SubscribeTransactionsRequest,
};
use log::error;
use crate::common::AnyResult;
use crate::streaming::common::{process_shred_transaction, MetricsManager, StreamClientConfig, SubscriptionHandle};
use crate::streaming::event_parser::common::high_performance_clock::get_high_perf_clock;
use crate::streaming::event_parser::{DexEvent, Protocol};
use crate::streaming::event_parser::common::filter::EventTypeFilter;
use crate::streaming::shred::factory;
use solana_sdk::{
    message::{Message, MessageHeader, VersionedMessage},
    transaction::VersionedTransaction,
    signature::Signature,
    pubkey::Pubkey,
    hash::Hash,
    message::compiled_instruction::CompiledInstruction,
};


/// Shreder gRPC streaming client for transaction subscriptions
#[derive(Clone)]
pub struct ShrederClient {
    pub shredstream_client: Arc<ShrederServiceClient<Channel>>,
    pub config: StreamClientConfig,
    pub subscription_handle: Arc<Mutex<Option<SubscriptionHandle>>>,
}

impl ShrederClient {
    pub async fn new(endpoint: String) -> AnyResult<Self> {
        Self::new_with_config(endpoint, StreamClientConfig::default()).await
    }

    pub async fn new_with_config(endpoint: String, config: StreamClientConfig) -> AnyResult<Self> {
        let shredstream_client = ShrederServiceClient::connect(endpoint.clone()).await?;
        MetricsManager::init(config.enable_metrics);
        Ok(Self {
            shredstream_client: Arc::new(shredstream_client),
            config,
            subscription_handle: Arc::new(Mutex::new(None)),
        })
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

        let mut client = (*self.shredstream_client).clone();
        // Create transaction filters based on protocols (dynamic)
        let mut transaction_filters = HashMap::new();
        
        for protocol in &protocols {
            let program_ids = protocol.get_program_id();
            let program_id_strings: Vec<String> = program_ids.iter()
                .map(|pubkey| pubkey.to_string())
                .collect();
            
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

        let request = SubscribeTransactionsRequest {transactions: transaction_filters};
        let (mut subscribe_tx, subscribe_rx) = futures::channel::mpsc::unbounded::<SubscribeTransactionsRequest>();
        let mut stream = client.subscribe_transactions(subscribe_rx).await.unwrap().into_inner();
        
        let callback = Arc::new(callback);

        let stream_task = tokio::spawn(async move {

            // Send the request
            let _ = subscribe_tx.send(request).await;

            while let Some(message) = stream.message().await.unwrap() {
                if let Some(transaction_update) = &message.transaction {
                    if let Some(shreder_tx) = transaction_update.transaction.as_ref() {
                        let versioned_tx = convert_shreder_to_versioned_transaction(shreder_tx);
                        let versioned_tx = match versioned_tx {
                            Ok(vtx) => vtx,
                            Err(e) => {
                                error!("Failed to convert Shreder transaction: {:?}", e);
                                continue;
                            }
                        };
                        if versioned_tx.signatures.is_empty() {
                            continue;
                        }

                        let transaction_with_slot = factory::create_transaction_with_slot_pooled(
                            versioned_tx,
                            transaction_update.slot,
                            get_high_perf_clock(),
                        );

                        if let Err(e) = process_shred_transaction(
                            transaction_with_slot,
                            &protocols,
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
                    log::warn!("Received message without transaction data");
                }
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
}

fn convert_shreder_to_versioned_transaction(shreder_tx: &shreder::Transaction) -> AnyResult<solana_sdk::transaction::VersionedTransaction> {

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
    let shreder_msg = shreder_tx.message.clone().ok_or_else(|| {
        anyhow::anyhow!("Missing message in transaction")
    })?;

    // Convert message header
    let header = shreder_msg.header.ok_or_else(|| {
        anyhow::anyhow!("Missing header in message")
    })?;
    
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
        return Err(anyhow::anyhow!("Invalid blockhash length: {}", shreder_msg.recent_blockhash.len()));
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
        let legacy_message = Message {
            header: message_header,
            account_keys,
            recent_blockhash,
            instructions,
        };
        VersionedMessage::Legacy(legacy_message)
    };

    Ok(VersionedTransaction {
        signatures,
        message: versioned_message,
    })
}