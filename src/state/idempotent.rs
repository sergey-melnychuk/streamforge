//! Idempotent state backend wrapper for exactly-once semantics
//!
//! Wraps any state backend to ensure idempotent state updates based on transaction IDs

use crate::execution::exactly_once::TransactionId;
use crate::state::backend::{StateBackend, StateError, StateResult};
use async_trait::async_trait;
use bytes::Bytes;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tokio::sync::RwLock;

/// Idempotent state backend wrapper that tracks processed transaction IDs
pub struct IdempotentStateBackend {
    /// Underlying state backend
    inner: Arc<dyn StateBackend>,
    /// Processed transaction IDs per key
    processed_transactions: Arc<RwLock<HashMap<Bytes, HashSet<TransactionId>>>>,
    /// Maximum number of transaction IDs to keep per key
    max_transactions_per_key: usize,
}

impl IdempotentStateBackend {
    /// Create a new idempotent state backend wrapper
    pub fn new(inner: Arc<dyn StateBackend>, max_transactions_per_key: usize) -> Self {
        Self {
            inner,
            processed_transactions: Arc::new(RwLock::new(HashMap::new())),
            max_transactions_per_key,
        }
    }

    /// Extract transaction ID from value (assumes value contains transaction ID metadata)
    /// In a full implementation, we'd use a structured format or separate metadata store
    fn extract_transaction_id(_value: &Bytes) -> Option<TransactionId> {
        // Simplified: In production, we'd parse transaction ID from value metadata
        // For now, we'll use a different approach - track transaction IDs separately
        None
    }

    /// Check if a transaction has been processed for a key
    async fn is_transaction_processed(&self, key: &[u8], tx_id: TransactionId) -> bool {
        let processed = self.processed_transactions.read().await;
        processed
            .get(&Bytes::copy_from_slice(key))
            .map(|tx_ids| tx_ids.contains(&tx_id))
            .unwrap_or(false)
    }

    /// Mark a transaction as processed for a key
    async fn mark_transaction_processed(&self, key: &[u8], tx_id: TransactionId) {
        let key_bytes = Bytes::copy_from_slice(key);
        let mut processed = self.processed_transactions.write().await;

        let tx_ids = processed.entry(key_bytes).or_insert_with(HashSet::new);

        // Evict old transactions if needed
        if tx_ids.len() >= self.max_transactions_per_key {
            // Remove oldest transactions (simplified - in production, use LRU)
            let to_remove: Vec<TransactionId> = tx_ids
                .iter()
                .take(tx_ids.len() - self.max_transactions_per_key + 1000)
                .copied()
                .collect();
            for tx_id in to_remove {
                tx_ids.remove(&tx_id);
            }
        }

        tx_ids.insert(tx_id);
    }
}

#[async_trait]
impl StateBackend for IdempotentStateBackend {
    async fn get(&self, key: &[u8]) -> StateResult<Option<Bytes>> {
        self.inner.get(key).await
    }

    async fn put(&self, key: &[u8], value: Bytes) -> StateResult<()> {
        // For idempotent puts, we need to track transaction IDs
        // In a full implementation, we'd extract transaction ID from value or use a separate parameter
        // For now, we'll just pass through to the inner backend
        self.inner.put(key, value).await
    }

    async fn delete(&self, key: &[u8]) -> StateResult<()> {
        self.inner.delete(key).await
    }

    async fn exists(&self, key: &[u8]) -> StateResult<bool> {
        self.inner.exists(key).await
    }

    async fn list_keys(&self, prefix: &[u8]) -> StateResult<Vec<Bytes>> {
        self.inner.list_keys(prefix).await
    }

    async fn clear(&self) -> StateResult<()> {
        let mut processed = self.processed_transactions.write().await;
        processed.clear();
        self.inner.clear().await
    }

    async fn snapshot(&self) -> StateResult<HashMap<Bytes, Bytes>> {
        self.inner.snapshot().await
    }

    async fn restore(&self, snapshot: HashMap<Bytes, Bytes>) -> StateResult<()> {
        self.inner.restore(snapshot).await
    }
}

impl IdempotentStateBackend {
    /// Put with transaction ID for idempotency
    pub async fn put_with_transaction(
        &self,
        key: &[u8],
        value: Bytes,
        tx_id: TransactionId,
    ) -> StateResult<()> {
        // Check if transaction already processed
        if self.is_transaction_processed(key, tx_id).await {
            // Skip duplicate update
            return Ok(());
        }

        // Update state
        self.inner.put(key, value).await?;

        // Mark transaction as processed
        self.mark_transaction_processed(key, tx_id).await;

        Ok(())
    }
}

