use chrono::{DateTime, LocalResult, TimeZone, Utc};
use futures::future::join_all;
use solana_rpc_client::nonblocking::rpc_client::RpcClient;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;

#[derive(Clone)]
pub struct BlockTimeCache {
    rpc_client: Arc<RpcClient>,
    cache: Arc<Mutex<HashMap<u64, i64>>>,
    fetching: Arc<Mutex<HashSet<u64>>>,
}

impl BlockTimeCache {
    pub fn new(rpc_endpoint: &str) -> Self {
        // Create RPC client with custom timeout (2 seconds instead of default 10s)
        let rpc_client = RpcClient::new_with_timeout(
            rpc_endpoint.to_string(),
            Duration::from_secs(2),
        );
        
        Self {
            rpc_client: Arc::new(rpc_client),
            cache: Arc::new(Mutex::new(HashMap::new())),
            fetching: Arc::new(Mutex::new(HashSet::new())),
        }
    }

    pub async fn get_block_time(&self, slot: u64) -> Option<i64> {
        // Check cache first (fast path)
        {
            let cache = self.cache.lock().await;
            if let Some(time) = cache.get(&slot) {
                return Some(*time);
            }
        }

        // If not in cache, check if we're already fetching it
        {
            let fetching = self.fetching.lock().await;
            if fetching.contains(&slot) {
                // Another task is already fetching, return None (ignore for now)
                return None;
            }
        }
        
        // Spawn background task to fetch (non-blocking!)
        let self_clone = self.clone();
        tokio::spawn(async move {
            self_clone.fetch_block_time_background(slot).await;
        });
        
        // Return None immediately - next message will get from cache
        None
    }
    
    async fn fetch_block_time_background(&self, slot: u64) {
        // Mark as fetching
        {
            let mut fetching = self.fetching.lock().await;
            if fetching.contains(&slot) {
                return; // Already being fetched
            }
            fetching.insert(slot);
        }

        // Fetch block time from RPC (this runs in background, doesn't block main stream)
        let rpc_start = std::time::Instant::now();
        let block_time_result = self.rpc_client.get_block_time(slot).await;
        let rpc_duration = rpc_start.elapsed();

        let block_time = match block_time_result {
            Ok(time) => {
                // Only log if RPC was slow (>100ms)
                if rpc_duration.as_millis() > 100 {
                    println!("[{}] ⏱️  RPC slot {} took {}ms", 
                        chrono::Utc::now().format("%H:%M:%S%.3f"),
                        slot, 
                        rpc_duration.as_millis()
                    );
                }
                Some(time)
            }
            Err(err) => {
                if format!("{:?}", err).contains("Block not available") {
                    // Block not ready yet - don't log, this is normal for fresh slots
                    None
                } else {
                    log::error!("Error fetching block time for slot {} (took {:.2}ms): {:?}", slot, rpc_duration.as_micros() as f64 / 1000.0, err);
                    None
                }
            }
        };

        // Cache the result
        {
            let mut cache = self.cache.lock().await;
            if let Some(time) = block_time {
                cache.insert(slot, time);
                if cache.len() > 20 {
                    if let Some(oldest_slot) = cache.keys().next().cloned() {
                        cache.remove(&oldest_slot);
                    }
                }
            }
        }

        // Remove from fetching set
        let mut fetching = self.fetching.lock().await;
        fetching.remove(&slot);
    }
}

pub async fn prepare_log_message(
    slot: u64,
    transactions_by_slot: &Arc<Mutex<HashMap<u64, Vec<(String, DateTime<Utc>)>>>>,
) {
    let received_time = Utc::now();
    let mut map = transactions_by_slot.lock().await;
    
    // Use entry API to atomically insert only if not present
    map.entry(slot).or_insert_with(|| vec![("slot_marker".to_string(), received_time)]);
}

pub async fn latency_monitor_task(
    block_time_cache: BlockTimeCache,
    transactions_by_slot: Arc<Mutex<HashMap<u64, Vec<(String, DateTime<Utc>)>>>>,
) {
    const MAX_LATENCIES: usize = 420;
    let mut latency_buffer = Vec::new();

    loop {
        tokio::time::sleep(std::time::Duration::from_millis(420)).await;

        let slots: Vec<u64> = transactions_by_slot.lock().await.keys().cloned().collect();
        
        if slots.is_empty() {
            continue;
        }

        let block_time_futures = slots.iter().map(|&slot| {
            let value = block_time_cache.clone();
            async move {
                let block_time = value.get_block_time(slot).await;
                (slot, block_time)
            }
        });

        let slot_block_times = join_all(block_time_futures).await;

        for (slot, block_time_unix_opt) in slot_block_times {
            if let Some(block_time_unix) = block_time_unix_opt {
                let block_time = match Utc.timestamp_opt(block_time_unix, 0) {
                    LocalResult::Single(t) => t,
                    LocalResult::None => {
                        log::error!("Invalid block time for slot {}", slot);
                        continue;
                    }
                    _ => {
                        log::error!("Unexpected error fetching block time for slot {}", slot);
                        continue;
                    }
                };

                // Remove slot from tracking after we got the block time
                let txs = transactions_by_slot
                    .lock()
                    .await
                    .remove(&slot)
                    .unwrap_or_default();

                // Only process if we have transactions for this slot
                if !txs.is_empty() {
                    // Use the first transaction's receive time as the slot receive time
                    let recv_time = txs[0].1;
                    
                    let latency = recv_time
                        .signed_duration_since(block_time)
                        .num_milliseconds()
                        .saturating_sub(500);
                    
                    // Only track positive latencies for average calculation
                    if latency > 0 {
                        latency_buffer.push(latency);
                        if latency_buffer.len() > MAX_LATENCIES {
                            latency_buffer.remove(0);
                        }
                    }

                    let avg_latency = if !latency_buffer.is_empty() {
                        latency_buffer.iter().sum::<i64>() as f64 / latency_buffer.len() as f64
                    } else {
                        0.0
                    };

                    log::info!(
                        "Slot: {} | ⏰ BlockTime: {} | 📥 ReceivedAt: {} | 🚀 Latency: {} ms | 📊 Avg (last {}): {:.2} ms",
                        slot,
                        block_time.to_rfc3339_opts(chrono::SecondsFormat::Millis, true),
                        recv_time.to_rfc3339_opts(chrono::SecondsFormat::Millis, true),
                        latency,
                        latency_buffer.len(),
                        avg_latency
                    );
                }
            }
        }
    }
}