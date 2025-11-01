use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

use solana_sdk::transaction::VersionedTransaction;
use tokio::sync::Mutex;

#[derive(Clone)]
pub struct TransactionStorage {
    transactions: Arc<Mutex<HashMap<String, VersionedTransaction>>>,
    processed_signatures: Arc<Mutex<HashSet<String>>>,
}

impl TransactionStorage {
    pub fn new() -> Self {
        Self {
            transactions: Arc::new(Mutex::new(HashMap::new())),
            processed_signatures: Arc::new(Mutex::new(HashSet::new())),
        }
    }

    pub async fn insert(&self, key: String, tx: VersionedTransaction) {
        self.transactions.lock().await.insert(key, tx);
    }

    pub async fn get(&self, key: &str) -> Option<VersionedTransaction> {
        self.transactions.lock().await.get(key).cloned()
    }

    pub async fn remove(&self, key: &str) {
        self.transactions.lock().await.remove(key);
    }

    pub async fn is_processed(&self, signature: &str) -> bool {
        self.processed_signatures.lock().await.contains(signature)
    }

    pub async fn mark_processed(&self, signature: String) {
        self.processed_signatures.lock().await.insert(signature);
    }
}
