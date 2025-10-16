use solana_streamer_sdk::streaming::common::{StreamClientConfig, AutoReconnectConfig};
use solana_streamer_sdk::streaming::shreder_stream::ShrederClient;
use solana_streamer_sdk::streaming::event_parser::Protocol;
use log::info;

/// Example demonstrating auto-reconnect functionality for ShrederClient
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::init();

    // Configure auto-reconnect with custom settings
    let auto_reconnect_config = AutoReconnectConfig {
        enabled: true,
        max_retries: 10,           // 0 for infinite retries
        initial_delay_ms: 1000,    // Start with 1 second delay
        max_delay_ms: 30000,       // Max 30 seconds between retries
        backoff_multiplier: 2.0,   // Exponential backoff
    };

    let config = StreamClientConfig {
        enable_metrics: true,
        auto_reconnect: auto_reconnect_config,
        ..Default::default()
    };

    // Create client with auto-reconnect enabled
    let client = ShrederClient::new_with_config(
        "https://your-shreder-endpoint.com".to_string(),
        config
    ).await?;

    // Subscribe with auto-reconnect handling
    let protocols = vec![Protocol::PumpFun, Protocol::RaydiumAmmV4];
    
    client.shredstream_subscribe(
        protocols,
        None,
        None,
        |event| {
            info!("Received event: {:?}", event);
        }
    ).await?;

    // Keep the program running
    tokio::signal::ctrl_c().await?;
    info!("Shutting down...");
    
    client.stop().await;
    Ok(())
}