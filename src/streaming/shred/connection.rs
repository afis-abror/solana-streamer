use std::sync::Arc;
use tokio::sync::Mutex;
use tonic::transport::Channel;

use crate::common::AnyResult;
use crate::protos::shredstream::shredstream_proxy_client::ShredstreamProxyClient;
use crate::streaming::common::{
    MetricsManager, PerformanceMetrics, StreamClientConfig, SubscriptionHandle,
};
use crate::streaming::storage::TransactionStorage;

/// ShredStream gRPC 客户端
#[derive(Clone)]
pub struct ShredStreamGrpc {
    pub shredstream_client: Arc<ShredstreamProxyClient<Channel>>,
    pub config: StreamClientConfig,
    pub subscription_handle: Arc<Mutex<Option<SubscriptionHandle>>>,
    pub transactions: Arc<TransactionStorage>,
    pub endpoint: String,
}

impl ShredStreamGrpc {
    /// 创建客户端，使用默认配置
    pub async fn new(endpoint: String) -> AnyResult<Self> {
        Self::new_with_config(endpoint, StreamClientConfig::default()).await
    }

    /// 创建客户端，使用自定义配置
    pub async fn new_with_config(endpoint: String, config: StreamClientConfig) -> AnyResult<Self> {
        let storage = Arc::new(TransactionStorage::new());
        Self::new_with_storage(endpoint, config, storage).await
    }

    /// 创建客户端，使用自定义配置和共享的 TransactionStorage
    pub async fn new_with_storage(
        endpoint: String,
        config: StreamClientConfig,
        storage: Arc<TransactionStorage>,
    ) -> AnyResult<Self> {
        let shredstream_client = Self::create_client(&endpoint).await?;
        MetricsManager::init(config.enable_metrics);
        Ok(Self {
            shredstream_client: Arc::new(shredstream_client),
            config,
            subscription_handle: Arc::new(Mutex::new(None)),
            transactions: storage,
            endpoint,
        })
    }

    /// 创建 gRPC 客户端连接
    pub(crate) async fn create_client(endpoint: &str) -> AnyResult<ShredstreamProxyClient<Channel>> {
        Ok(ShredstreamProxyClient::connect(endpoint.to_string()).await?)
    }

    /// 获取当前配置
    pub fn get_config(&self) -> &StreamClientConfig {
        &self.config
    }

    /// 更新配置
    pub fn update_config(&mut self, config: StreamClientConfig) {
        self.config = config;
    }

    /// 获取性能指标
    pub fn get_metrics(&self) -> PerformanceMetrics {
        MetricsManager::global().get_metrics()
    }

    /// 启用或禁用性能监控
    pub fn set_enable_metrics(&mut self, enabled: bool) {
        self.config.enable_metrics = enabled;
    }

    /// 打印性能指标
    pub fn print_metrics(&self) {
        MetricsManager::global().print_metrics();
    }

    /// 启动自动性能监控任务
    pub async fn start_auto_metrics_monitoring(&self) {
        MetricsManager::global().start_auto_monitoring().await;
    }

    /// 停止当前订阅
    pub async fn stop(&self) {
        let mut handle_guard = self.subscription_handle.lock().await;
        if let Some(handle) = handle_guard.take() {
            handle.stop();
        }
    }
}
