use log::info;
use solana_streamer_sdk::streaming::event_parser::Protocol;
use solana_streamer_sdk::streaming::shreder_stream::ShrederClient;
use std::env;
use std::net::IpAddr;
use std::str::FromStr;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    env_logger::init();

    let endpoint = "http://fra1.shreder.xyz:9991".to_string();
    let local_ip = IpAddr::from_str("84.32.100.151")?;

    let rpc_endpoint = env::var("SOLANA_RPC_ENDPOINT")
        .unwrap_or_else(|_| "https://api.mainnet-beta.solana.com".to_string());

    info!("Connecting to {} from local IP {}", endpoint, local_ip);

    let mut client = ShrederClient::new_with_local_addr(endpoint.clone(), local_ip).await?;
    client.enable_latency_monitoring(rpc_endpoint);

    // Subscribe to protocols
    let protocols = vec![Protocol::PumpFun, Protocol::PumpSwap];

    client
        .shredstream_subscribe(protocols, None, None, |event| {
            info!("📊 DEX Event: {:?}", event);
        })
        .await?;

    // Keep running
    info!("Subscribed successfully. Press Ctrl+C to exit...");
    tokio::signal::ctrl_c().await?;
    
    info!("Stopping client...");
    client.stop().await;

    Ok(())
}
