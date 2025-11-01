use log::info;
use solana_streamer_sdk::streaming::event_parser::Protocol;
use solana_streamer_sdk::streaming::shreder_stream::ShrederClient;
use std::net::IpAddr;
use std::str::FromStr;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    env_logger::init();

    // === EXAMPLE 1: Using specific local IP address ===
    // Server akan mendeteksi koneksi dari IP 10.10.10.2
    let endpoint = "http://your-shreder-server.com:50051".to_string();
    let local_ip = IpAddr::from_str("10.10.10.2")?;

    info!("Connecting to {} from local IP {}", endpoint, local_ip);

    let client = ShrederClient::new_with_local_addr(endpoint.clone(), local_ip).await?;

    // Subscribe to protocols
    let protocols = vec![Protocol::PumpFun, Protocol::RaydiumAmmV4];

    client
        .shredstream_subscribe(protocols, None, None, |event| {
            println!("Received event: {:?}", event);
        })
        .await?;

    // === EXAMPLE 2: Using specific local IP with custom config ===
    use solana_streamer_sdk::streaming::common::{
        AutoReconnectConfig, ConnectionConfig, StreamClientConfig,
    };

    let config = StreamClientConfig {
        enable_metrics: true,
        auto_reconnect: AutoReconnectConfig {
            enabled: true,
            max_retries: 10,
            initial_delay_ms: 1000,
            max_delay_ms: 30000,
            backoff_multiplier: 2.0,
        },
        connection: ConnectionConfig {
            connect_timeout: 10,
            request_timeout: 30,
            max_decoding_message_size: 10 * 1024 * 1024, // 10MB
        },
    };

    let endpoint2 = "http://your-shreder-server.com:50051".to_string();
    let local_ip2 = IpAddr::from_str("10.10.10.2")?;

    let _client2 =
        ShrederClient::new_with_config_and_local_addr(endpoint2, config, Some(local_ip2)).await?;
    info!("Client 2 created with custom config and local IP");

    // === EXAMPLE 3: Without specific local IP (uses default route) ===
    let endpoint3 = "http://your-shreder-server.com:50051".to_string();
    let _client3 = ShrederClient::new(endpoint3).await?;
    info!("Client 3 created with default IP");

    // Keep running
    info!("Press Ctrl+C to exit...");
    tokio::signal::ctrl_c().await?;

    Ok(())
}
