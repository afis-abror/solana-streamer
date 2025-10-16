use super::constants::*;

/// Connection configuration
#[derive(Debug, Clone)]
pub struct ConnectionConfig {
    /// Connection timeout in seconds (default: 10)
    pub connect_timeout: u64,
    /// Request timeout in seconds (default: 60)
    pub request_timeout: u64,
    /// Maximum decoding message size in bytes (default: 10MB)
    pub max_decoding_message_size: usize,
}

impl Default for ConnectionConfig {
    fn default() -> Self {
        Self {
            connect_timeout: DEFAULT_CONNECT_TIMEOUT,
            request_timeout: DEFAULT_REQUEST_TIMEOUT,
            max_decoding_message_size: DEFAULT_MAX_DECODING_MESSAGE_SIZE,
        }
    }
}

/// Auto-reconnect configuration
#[derive(Debug, Clone)]
pub struct AutoReconnectConfig {
    /// Whether auto-reconnect is enabled (default: true)
    pub enabled: bool,
    /// Maximum number of retry attempts (default: 5, 0 = infinite)
    pub max_retries: u32,
    /// Initial delay between reconnect attempts in milliseconds (default: 1000)
    pub initial_delay_ms: u64,
    /// Maximum delay between reconnect attempts in milliseconds (default: 30000)
    pub max_delay_ms: u64,
    /// Backoff multiplier for exponential backoff (default: 2.0)
    pub backoff_multiplier: f64,
}

impl Default for AutoReconnectConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            max_retries: 5,
            initial_delay_ms: 1000,
            max_delay_ms: 30000,
            backoff_multiplier: 2.0,
        }
    }
}

/// Common client configuration
#[derive(Debug, Clone)]
pub struct StreamClientConfig {
    /// Connection configuration
    pub connection: ConnectionConfig,
    /// Whether performance monitoring is enabled (default: false)
    pub enable_metrics: bool,
    /// Auto-reconnect configuration
    pub auto_reconnect: AutoReconnectConfig,
}

impl Default for StreamClientConfig {
    fn default() -> Self {
        Self { 
            connection: ConnectionConfig::default(), 
            enable_metrics: false,
            auto_reconnect: AutoReconnectConfig::default(),
        }
    }
}
