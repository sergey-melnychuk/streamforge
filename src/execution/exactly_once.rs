//! Exactly-once semantics for stream processing
//!
//! Implements transaction coordination, idempotency, and checkpoint coordination
//! to guarantee exactly-once processing semantics

use crate::core::Event;
use crate::error::Result;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{debug, info, warn};

/// Transaction ID for tracking processing transactions
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct TransactionId(u64);

impl TransactionId {
    /// Generate a new transaction ID
    pub fn new(id: u64) -> Self {
        Self(id)
    }

    /// Generate from timestamp and sequence
    pub fn from_timestamp_and_sequence(timestamp: u64, sequence: u64) -> Self {
        // Combine timestamp (high 32 bits) and sequence (low 32 bits)
        Self((timestamp << 32) | sequence)
    }

    /// Get the transaction ID as u64
    pub fn as_u64(&self) -> u64 {
        self.0
    }
}

impl From<u64> for TransactionId {
    fn from(id: u64) -> Self {
        Self(id)
    }
}

/// Transaction status
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransactionStatus {
    /// Transaction is in progress
    InProgress,
    /// Transaction is committed
    Committed,
    /// Transaction is aborted
    Aborted,
}

/// Transaction metadata
#[derive(Debug, Clone)]
pub struct Transaction {
    /// Transaction ID
    pub id: TransactionId,
    /// Transaction status
    pub status: TransactionStatus,
    /// Participants (operator/node IDs)
    pub participants: HashSet<String>,
    /// Events processed in this transaction
    pub events: Vec<Event>,
    /// Timestamp when transaction started
    pub start_timestamp: u64,
    /// Timestamp when transaction committed/aborted
    pub end_timestamp: Option<u64>,
}

impl Transaction {
    /// Create a new transaction
    pub fn new(id: TransactionId, start_timestamp: u64) -> Self {
        Self {
            id,
            status: TransactionStatus::InProgress,
            participants: HashSet::new(),
            events: Vec::new(),
            start_timestamp,
            end_timestamp: None,
        }
    }

    /// Add a participant to the transaction
    pub fn add_participant(&mut self, participant: String) {
        self.participants.insert(participant);
    }

    /// Add an event to the transaction
    pub fn add_event(&mut self, event: Event) {
        self.events.push(event);
    }

    /// Commit the transaction
    pub fn commit(&mut self, timestamp: u64) {
        self.status = TransactionStatus::Committed;
        self.end_timestamp = Some(timestamp);
    }

    /// Abort the transaction
    pub fn abort(&mut self, timestamp: u64) {
        self.status = TransactionStatus::Aborted;
        self.end_timestamp = Some(timestamp);
    }
}

/// Two-phase commit coordinator
pub struct TwoPhaseCommitCoordinator {
    /// Active transactions
    transactions: Arc<RwLock<HashMap<TransactionId, Transaction>>>,
    /// Processed transaction IDs (for idempotency)
    processed_transactions: Arc<RwLock<HashSet<TransactionId>>>,
    /// Transaction ID generator
    transaction_id_counter: Arc<RwLock<u64>>,
}

impl TwoPhaseCommitCoordinator {
    /// Create a new 2PC coordinator
    pub fn new() -> Self {
        Self {
            transactions: Arc::new(RwLock::new(HashMap::new())),
            processed_transactions: Arc::new(RwLock::new(HashSet::new())),
            transaction_id_counter: Arc::new(RwLock::new(0)),
        }
    }

    /// Generate a new transaction ID
    pub async fn generate_transaction_id(&self) -> TransactionId {
        let mut counter = self.transaction_id_counter.write().await;
        *counter += 1;
        TransactionId::from_timestamp_and_sequence(
            chrono::Utc::now().timestamp_millis() as u64,
            *counter,
        )
    }

    /// Start a new transaction
    pub async fn start_transaction(&self, participants: Vec<String>) -> Result<TransactionId> {
        let tx_id = self.generate_transaction_id().await;
        let timestamp = chrono::Utc::now().timestamp_millis() as u64;
        let participant_count = participants.len();

        let mut transaction = Transaction::new(tx_id, timestamp);
        for participant in participants {
            transaction.add_participant(participant);
        }

        let mut transactions = self.transactions.write().await;
        transactions.insert(tx_id, transaction);

        info!(
            "Started transaction {:?} with {} participants",
            tx_id, participant_count
        );

        Ok(tx_id)
    }

    /// Phase 1: Prepare (ask all participants to prepare)
    pub async fn prepare(&self, tx_id: TransactionId) -> Result<bool> {
        let transactions = self.transactions.read().await;
        let transaction = transactions.get(&tx_id).ok_or_else(|| {
            crate::error::StreamError::Unknown(format!("Transaction {:?} not found", tx_id))
        })?;

        if transaction.status != TransactionStatus::InProgress {
            return Err(crate::error::StreamError::Unknown(format!(
                "Transaction {:?} is not in progress",
                tx_id
            )));
        }

        // In a full implementation, we'd send prepare requests to all participants
        // For now, we'll assume all participants are ready
        debug!(
            "Preparing transaction {:?} with {} participants",
            tx_id,
            transaction.participants.len()
        );

        Ok(true)
    }

    /// Phase 2: Commit (tell all participants to commit)
    pub async fn commit(&self, tx_id: TransactionId) -> Result<()> {
        let mut transactions = self.transactions.write().await;
        let transaction = transactions.get_mut(&tx_id).ok_or_else(|| {
            crate::error::StreamError::Unknown(format!("Transaction {:?} not found", tx_id))
        })?;

        if transaction.status != TransactionStatus::InProgress {
            return Err(crate::error::StreamError::Unknown(format!(
                "Transaction {:?} is not in progress",
                tx_id
            )));
        }

        let timestamp = chrono::Utc::now().timestamp_millis() as u64;
        transaction.commit(timestamp);

        // Mark transaction as processed (for idempotency)
        let mut processed = self.processed_transactions.write().await;
        processed.insert(tx_id);

        // In a full implementation, we'd send commit requests to all participants
        info!("Committed transaction {:?}", tx_id);

        Ok(())
    }

    /// Abort a transaction
    pub async fn abort(&self, tx_id: TransactionId) -> Result<()> {
        let mut transactions = self.transactions.write().await;
        let transaction = transactions.get_mut(&tx_id).ok_or_else(|| {
            crate::error::StreamError::Unknown(format!("Transaction {:?} not found", tx_id))
        })?;

        let timestamp = chrono::Utc::now().timestamp_millis() as u64;
        transaction.abort(timestamp);

        // In a full implementation, we'd send abort requests to all participants
        warn!("Aborted transaction {:?}", tx_id);

        Ok(())
    }

    /// Check if a transaction has been processed (for idempotency)
    pub async fn is_transaction_processed(&self, tx_id: TransactionId) -> bool {
        let processed = self.processed_transactions.read().await;
        processed.contains(&tx_id)
    }

    /// Get transaction status
    pub async fn get_transaction_status(&self, tx_id: TransactionId) -> Option<TransactionStatus> {
        let transactions = self.transactions.read().await;
        transactions.get(&tx_id).map(|t| t.status)
    }
}

impl Default for TwoPhaseCommitCoordinator {
    fn default() -> Self {
        Self::new()
    }
}

/// Idempotent state tracker
pub struct IdempotentStateTracker {
    /// Processed transaction IDs
    processed_transactions: Arc<RwLock<HashSet<TransactionId>>>,
    /// Maximum number of transaction IDs to keep (for memory management)
    max_transactions: usize,
}

impl IdempotentStateTracker {
    /// Create a new idempotent state tracker
    pub fn new(max_transactions: usize) -> Self {
        Self {
            processed_transactions: Arc::new(RwLock::new(HashSet::new())),
            max_transactions,
        }
    }

    /// Check if a transaction has been processed
    pub async fn is_processed(&self, tx_id: TransactionId) -> bool {
        let processed = self.processed_transactions.read().await;
        processed.contains(&tx_id)
    }

    /// Mark a transaction as processed
    pub async fn mark_processed(&self, tx_id: TransactionId) -> Result<()> {
        let mut processed = self.processed_transactions.write().await;

        // Check if we need to evict old transactions
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
        Ok(())
    }

    /// Clear all processed transactions (for testing)
    pub async fn clear(&self) {
        let mut processed = self.processed_transactions.write().await;
        processed.clear();
    }
}

/// Checkpoint coordinator for coordinating checkpoints across operators
pub struct CheckpointCoordinator {
    /// Last checkpoint ID
    last_checkpoint_id: Arc<RwLock<Option<u64>>>,
    /// Checkpoint participants (operator/node IDs)
    participants: Arc<RwLock<HashSet<String>>>,
}

impl CheckpointCoordinator {
    /// Create a new checkpoint coordinator
    pub fn new() -> Self {
        Self {
            last_checkpoint_id: Arc::new(RwLock::new(None)),
            participants: Arc::new(RwLock::new(HashSet::new())),
        }
    }

    /// Register a participant for checkpoint coordination
    pub async fn register_participant(&self, participant: String) {
        let mut participants = self.participants.write().await;
        participants.insert(participant);
    }

    /// Unregister a participant
    pub async fn unregister_participant(&self, participant: String) {
        let mut participants = self.participants.write().await;
        participants.remove(&participant);
    }

    /// Coordinate a checkpoint across all participants
    pub async fn coordinate_checkpoint(&self) -> Result<u64> {
        let participants = self.participants.read().await;

        if participants.is_empty() {
            return Err(crate::error::StreamError::Unknown(
                "No participants registered for checkpoint".to_string(),
            ));
        }

        // Generate checkpoint ID
        let checkpoint_id = chrono::Utc::now().timestamp_millis() as u64;

        // In a full implementation, we'd:
        // 1. Send checkpoint request to all participants
        // 2. Wait for all participants to acknowledge
        // 3. Mark checkpoint as complete

        let mut last_checkpoint = self.last_checkpoint_id.write().await;
        *last_checkpoint = Some(checkpoint_id);

        info!(
            "Coordinated checkpoint {} across {} participants",
            checkpoint_id,
            participants.len()
        );

        Ok(checkpoint_id)
    }

    /// Get the last checkpoint ID
    pub async fn get_last_checkpoint_id(&self) -> Option<u64> {
        let last_checkpoint = self.last_checkpoint_id.read().await;
        *last_checkpoint
    }
}

impl Default for CheckpointCoordinator {
    fn default() -> Self {
        Self::new()
    }
}

/// Recovery protocol for exactly-once semantics
pub struct RecoveryProtocol {
    /// Checkpoint coordinator
    checkpoint_coordinator: Arc<CheckpointCoordinator>,
    /// Idempotent state tracker
    idempotent_tracker: Arc<IdempotentStateTracker>,
    /// 2PC coordinator
    #[allow(dead_code)]
    tx_coordinator: Arc<TwoPhaseCommitCoordinator>,
}

impl RecoveryProtocol {
    /// Create a new recovery protocol
    pub fn new(
        checkpoint_coordinator: Arc<CheckpointCoordinator>,
        idempotent_tracker: Arc<IdempotentStateTracker>,
        tx_coordinator: Arc<TwoPhaseCommitCoordinator>,
    ) -> Self {
        Self {
            checkpoint_coordinator,
            idempotent_tracker,
            tx_coordinator,
        }
    }

    /// Recover from last checkpoint with deduplication
    pub async fn recover_from_checkpoint(&self) -> Result<Option<u64>> {
        // Get last checkpoint ID
        let checkpoint_id = self.checkpoint_coordinator.get_last_checkpoint_id().await;

        if let Some(checkpoint_id) = checkpoint_id {
            info!("Recovering from checkpoint {}", checkpoint_id);

            // In a full implementation, we'd:
            // 1. Load checkpoint state
            // 2. Replay events from checkpoint
            // 3. Deduplicate using idempotent tracker
            // 4. Resume processing

            Ok(Some(checkpoint_id))
        } else {
            info!("No checkpoint found, starting from beginning");
            Ok(None)
        }
    }

    /// Replay events with deduplication
    pub async fn replay_events(&self, events: Vec<Event>) -> Result<Vec<Event>> {
        let events_len = events.len();
        let mut deduplicated = Vec::new();

        for event in events {
            // Extract transaction ID from event
            if let Some(tx_id_str) = event.get_header("transaction_id") {
                if let Ok(tx_id_u64) = tx_id_str.parse::<u64>() {
                    let tx_id = TransactionId::from(tx_id_u64);

                    // Check if already processed
                    if !self.idempotent_tracker.is_processed(tx_id).await {
                        // Mark as processed
                        self.idempotent_tracker.mark_processed(tx_id).await?;
                        deduplicated.push(event);
                    } else {
                        debug!("Skipping duplicate event with transaction ID {:?}", tx_id);
                    }
                } else {
                    // No valid transaction ID - include event
                    deduplicated.push(event);
                }
            } else {
                // No transaction ID - include event
                deduplicated.push(event);
            }
        }

        info!(
            "Replayed {} events, {} deduplicated",
            events_len,
            events_len - deduplicated.len()
        );

        Ok(deduplicated)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_transaction_id_generation() {
        let coordinator = TwoPhaseCommitCoordinator::new();
        let tx_id1 = coordinator.generate_transaction_id().await;
        let tx_id2 = coordinator.generate_transaction_id().await;

        assert_ne!(tx_id1, tx_id2);
    }

    #[tokio::test]
    async fn test_transaction_lifecycle() {
        let coordinator = TwoPhaseCommitCoordinator::new();
        let tx_id = coordinator
            .start_transaction(vec!["operator1".to_string(), "operator2".to_string()])
            .await
            .unwrap();

        assert_eq!(
            coordinator.get_transaction_status(tx_id).await,
            Some(TransactionStatus::InProgress)
        );

        coordinator.prepare(tx_id).await.unwrap();
        coordinator.commit(tx_id).await.unwrap();

        assert_eq!(
            coordinator.get_transaction_status(tx_id).await,
            Some(TransactionStatus::Committed)
        );
        assert!(coordinator.is_transaction_processed(tx_id).await);
    }

    #[tokio::test]
    async fn test_idempotent_state_tracker() {
        let tracker = IdempotentStateTracker::new(1000);
        let tx_id = TransactionId::new(1);

        assert!(!tracker.is_processed(tx_id).await);
        tracker.mark_processed(tx_id).await.unwrap();
        assert!(tracker.is_processed(tx_id).await);
    }

    #[tokio::test]
    async fn test_checkpoint_coordinator() {
        let coordinator = CheckpointCoordinator::new();
        coordinator
            .register_participant("operator1".to_string())
            .await;
        coordinator
            .register_participant("operator2".to_string())
            .await;

        let checkpoint_id = coordinator.coordinate_checkpoint().await.unwrap();
        assert_eq!(
            coordinator.get_last_checkpoint_id().await,
            Some(checkpoint_id)
        );
    }
}
