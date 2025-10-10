use std::sync::Arc;

use solana_sdk::pubkey::Pubkey;

use crate::common::AnyResult;
use crate::streaming::common::{MetricsEventType, StreamClientConfig as ClientConfig};
use crate::streaming::event_parser::common::filter::EventTypeFilter;
use crate::streaming::event_parser::core::account_event_parser::AccountEventParser;
use crate::streaming::event_parser::core::common_event_parser::CommonEventParser;

use crate::streaming::event_parser::core::event_parser::EventParser;
use crate::streaming::event_parser::{core::traits::UnifiedEvent, Protocol};
use crate::streaming::grpc::{EventPretty, MetricsManager};
use crate::streaming::shred::TransactionWithSlot;
use once_cell::sync::OnceCell;

pub enum EventSource {
    Grpc,
    Shred,
}

/// High-performance Event processor
pub struct EventProcessor {
    pub(crate) config: ClientConfig,
    pub(crate) parser_cache: OnceCell<Arc<EventParser>>,
    pub(crate) protocols: Vec<Protocol>,
    pub(crate) event_type_filter: Option<EventTypeFilter>,
    pub(crate) callback: Option<Arc<dyn Fn(UnifiedEvent) + Send + Sync>>,
}

impl EventProcessor {
    pub fn new(config: ClientConfig) -> Self {
        Self {
            config,
            parser_cache: OnceCell::new(),
            protocols: vec![],
            event_type_filter: None,
            callback: None,
        }
    }

    pub fn set_protocols_and_event_type_filter(
        &mut self,
        _source: EventSource,
        protocols: Vec<Protocol>,
        event_type_filter: Option<EventTypeFilter>,
        callback: Option<Arc<dyn Fn(UnifiedEvent) + Send + Sync>>,
    ) {
        self.protocols = protocols;
        self.event_type_filter = event_type_filter;
        self.callback = callback;
        let protocols_ref = &self.protocols;
        let event_type_filter_ref = self.event_type_filter.as_ref();
        self.parser_cache.get_or_init(|| {
            Arc::new(EventParser::new(protocols_ref.clone(), event_type_filter_ref.cloned()))
        });
    }

    pub fn get_parser(&self) -> Arc<EventParser> {
        self.parser_cache.get().unwrap().clone()
    }

    fn invoke_callback(&self, event: UnifiedEvent) {
        if let Some(callback) = self.callback.as_ref() {
            callback(event);
        }
    }

    pub async fn process_grpc_transaction(
        &self,
        event_pretty: EventPretty,
        bot_wallet: Option<Pubkey>,
    ) -> AnyResult<()> {
        if self.callback.is_none() {
            return Ok(());
        }
        match event_pretty {
            EventPretty::Account(account_pretty) => {
                MetricsManager::global().add_account_process_count();
                let account_event = AccountEventParser::parse_account_event(
                    &self.protocols,
                    account_pretty,
                    self.event_type_filter.as_ref(),
                );
                if let Some(mut event) = account_event {
                    let processing_time_us = event.metadata().handle_us as f64;
                    self.invoke_callback(event);
                    self.update_metrics(MetricsEventType::Account, 1, processing_time_us);
                }
            }
            EventPretty::Transaction(transaction_pretty) => {
                MetricsManager::global().add_tx_process_count();
                let slot = transaction_pretty.slot;
                let signature = transaction_pretty.signature;
                let block_time = transaction_pretty.block_time;
                let recv_us = transaction_pretty.recv_us;
                let transaction_index = transaction_pretty.transaction_index;
                let grpc_tx = transaction_pretty.grpc_tx;

                let parser = self.get_parser();
                let callback = self.callback.clone().unwrap();
                let adapter_callback = Arc::new(move |mut event: UnifiedEvent| {
                    let processing_time_us = event.metadata().handle_us as f64;
                    callback(event);
                    MetricsManager::global().update_metrics(
                        MetricsEventType::Transaction,
                        1,
                        processing_time_us,
                    );
                });

                parser
                    .parse_grpc_transaction_owned(
                        grpc_tx,
                        signature,
                        Some(slot),
                        block_time,
                        recv_us,
                        bot_wallet,
                        transaction_index,
                        adapter_callback,
                    )
                    .await?;
            }
            EventPretty::BlockMeta(block_meta_pretty) => {
                MetricsManager::global().add_block_meta_process_count();
                let block_time_ms = block_meta_pretty
                    .block_time
                    .map(|ts| ts.seconds * 1000 + ts.nanos as i64 / 1_000_000)
                    .unwrap_or_else(|| chrono::Utc::now().timestamp_millis());
                let mut block_meta_event = CommonEventParser::generate_block_meta_event(
                    block_meta_pretty.slot,
                    block_meta_pretty.block_hash,
                    block_time_ms,
                    block_meta_pretty.recv_us,
                );
                let processing_time_us = block_meta_event.metadata().handle_us as f64;
                self.invoke_callback(block_meta_event);
                self.update_metrics(MetricsEventType::BlockMeta, 1, processing_time_us);
            }
        }

        Ok(())
    }

    pub async fn process_shred_transaction(
        &self,
        transaction_with_slot: TransactionWithSlot,
        bot_wallet: Option<Pubkey>,
    ) -> AnyResult<()> {
        if self.callback.is_none() {
            return Ok(());
        }
        MetricsManager::global().add_tx_process_count();
        let tx = transaction_with_slot.transaction;

        let slot = transaction_with_slot.slot;
        if tx.signatures.is_empty() {
            return Ok(());
        }
        let signature = tx.signatures[0];
        let recv_us = transaction_with_slot.recv_us;

        let parser = self.get_parser();
        let callback = self.callback.clone().unwrap();
        let adapter_callback = Arc::new(move |mut event: UnifiedEvent| {
            let processing_time_us = event.metadata().handle_us as f64;
            callback(event);
            MetricsManager::global().update_metrics(
                MetricsEventType::Transaction,
                1,
                processing_time_us,
            );
        });

        parser
            .parse_versioned_transaction_owned(
                tx,
                signature,
                Some(slot),
                None,
                recv_us,
                bot_wallet,
                None,
                &[],
                adapter_callback,
            )
            .await?;

        Ok(())
    }

    fn update_metrics(&self, ty: MetricsEventType, count: u64, time_us: f64) {
        MetricsManager::global().update_metrics(ty, count, time_us);
    }
}

impl Clone for EventProcessor {
    fn clone(&self) -> Self {
        Self {
            config: self.config.clone(),
            parser_cache: self.parser_cache.clone(),
            protocols: self.protocols.clone(),
            event_type_filter: self.event_type_filter.clone(),
            callback: self.callback.clone(),
        }
    }
}
