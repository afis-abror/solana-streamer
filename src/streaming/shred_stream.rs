use std::sync::Arc;
use std::time::Duration;

use futures::StreamExt;
use log::{error, info, warn};
use solana_sdk::pubkey::Pubkey;
use tokio::time::{sleep, timeout};

use crate::common::AnyResult;
use crate::protos::shredstream::SubscribeEntriesRequest;
use crate::streaming::common::{process_shred_transaction, SubscriptionHandle};
use crate::streaming::event_parser::common::filter::EventTypeFilter;
use crate::streaming::event_parser::common::high_performance_clock::get_high_perf_clock;
use crate::streaming::event_parser::{DexEvent, Protocol};
use crate::streaming::grpc::MetricsManager;
use crate::streaming::shred::pool::factory;
use solana_entry::entry::Entry;

use super::ShredStreamGrpc;

impl ShredStreamGrpc {
    /// 订阅ShredStream事件（支持批处理和即时处理，带自动重连）
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
        // 如果已有活跃订阅，先停止它
        self.stop().await;

        let mut metrics_handle = None;
        // 启动自动性能监控（如果启用）
        if self.config.enable_metrics {
            metrics_handle = MetricsManager::global().start_auto_monitoring().await;
        }

        // Clone necessary data for the stream task
        let auto_reconnect_config = self.config.auto_reconnect.clone();
        let endpoint = self.endpoint.clone();
        let connection_config = self.config.connection.clone();
        let protocols_clone = protocols.clone();
        let callback = Arc::new(callback);
        let transactions = self.transactions.clone();

        let stream_task = tokio::spawn(async move {
            let mut retry_attempt = 0u32;

            loop {
                // Try to establish connection
                let client = if retry_attempt == 0 {
                    // First attempt - try to use existing connection or create new one
                    match Self::create_client(&endpoint).await {
                        Ok(client) => client,
                        Err(e) => {
                            error!("Failed to connect to shredstream service: {:?}", e);
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
                        Self::create_client(&endpoint),
                    )
                    .await
                    {
                        Ok(Ok(client)) => {
                            info!("Successfully reconnected to shredstream service");
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
                let request = tonic::Request::new(SubscribeEntriesRequest {});
                let stream_result = timeout(
                    Duration::from_secs(connection_config.request_timeout),
                    client.subscribe_entries(request),
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

                info!("Successfully connected and subscribed to shredstream");
                retry_attempt = 0; // Reset retry counter on successful connection

                // Process stream messages
                let stream_broken = loop {
                    match stream.next().await {
                        Some(Ok(msg)) => {
                            if let Ok(entries) = bincode::deserialize::<Vec<Entry>>(&msg.entries) {
                                for entry in entries {
                                    for transaction in entry.transactions {
                                        // Store transaction in storage first (like shreder does)
                                        if !transaction.signatures.is_empty() {
                                            transactions
                                                .insert(
                                                    transaction.signatures[0].to_string(),
                                                    transaction.clone(),
                                                )
                                                .await;
                                        }

                                        let transaction_with_slot =
                                            factory::create_transaction_with_slot_pooled(
                                                transaction.clone(),
                                                msg.slot,
                                                get_high_perf_clock(),
                                            );
                                        // Process transaction - clone Arc and Vec for each call
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

            info!("ShredStream task ended");
        });

        // 保存订阅句柄
        let subscription_handle = SubscriptionHandle::new(stream_task, None, metrics_handle);
        let mut handle_guard = self.subscription_handle.lock().await;
        *handle_guard = Some(subscription_handle);

        Ok(())
    }
}
