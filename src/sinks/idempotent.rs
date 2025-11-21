//! Idempotent sink wrapper for exactly-once semantics
//!
//! Wraps any sink to ensure idempotent writes based on transaction IDs

use crate::core::Event;
use crate::execution::exactly_once::TransactionId;
use crate::sinks::{Sink, SinkError};
use async_trait::async_trait;
use std::collections::HashSet;
use std::sync::Arc;
use tokio::sync::RwLock;

/// Idempotent sink wrapper that deduplicates writes by transaction ID
pub struct IdempotentSink {
    /// Underlying sink
    inner: Box<dyn Sink>,
    /// Processed transaction IDs
    processed_transactions: Arc<RwLock<HashSet<TransactionId>>>,
    /// Maximum number of transaction IDs to keep
    max_transactions: usize,
}

impl IdempotentSink {
    /// Create a new idempotent sink wrapper
    pub fn new(inner: Box<dyn Sink>, max_transactions: usize) -> Self {
        Self {
            inner,
            processed_transactions: Arc::new(RwLock::new(HashSet::new())),
            max_transactions,
        }
    }

    /// Extract transaction ID from event headers
    fn extract_transaction_id(&self, event: &Event) -> Option<TransactionId> {
        // Look for transaction_id in event headers
        event
            .get_header("transaction_id")
            .and_then(|v| v.parse::<u64>().ok())
            .map(TransactionId::from)
    }

    /// Check if transaction has been processed
    async fn is_processed(&self, tx_id: TransactionId) -> bool {
        let processed = self.processed_transactions.read().await;
        processed.contains(&tx_id)
    }

    /// Mark transaction as processed
    async fn mark_processed(&self, tx_id: TransactionId) {
        let mut processed = self.processed_transactions.write().await;

        // Evict old transactions if needed
        if processed.len() >= self.max_transactions {
            // Remove oldest transactions (simplified - in production, use LRU)
            let to_remove: Vec<TransactionId> = processed
                .iter()
                .take(processed.len() - self.max_transactions + 1000)
                .copied()
                .collect();
            for tx_id in to_remove {
                processed.remove(&tx_id);
            }
        }

        processed.insert(tx_id);
    }
}

#[async_trait]
impl Sink for IdempotentSink {
    async fn write(&mut self, event: Event) -> Result<(), SinkError> {
        // Extract transaction ID
        if let Some(tx_id) = self.extract_transaction_id(&event) {
            // Check if already processed
            if self.is_processed(tx_id).await {
                // Skip duplicate write
                return Ok(());
            }

            // Write to underlying sink
            self.inner.write(event).await?;

            // Mark as processed
            self.mark_processed(tx_id).await;
        } else {
            // No transaction ID - write directly (at-least-once semantics)
            self.inner.write(event).await?;
        }

        Ok(())
    }

    async fn flush(&mut self) -> Result<(), SinkError> {
        self.inner.flush().await
    }

    async fn close(&mut self) -> Result<(), SinkError> {
        self.inner.close().await
    }
}
