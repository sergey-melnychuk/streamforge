//! State backend configuration

use serde::{Deserialize, Serialize};
use std::path::PathBuf;

/// State backend configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StateConfig {
    /// State backend type
    pub backend_type: StateBackendType,
    /// State directory path
    pub state_dir: PathBuf,
    /// Checkpoint configuration
    pub checkpoint: CheckpointConfig,
}

/// State backend type
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum StateBackendType {
    /// In-memory backend (fast, non-persistent)
    Memory,
    /// RocksDB backend (persistent)
    RocksDB,
}

/// Checkpoint configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CheckpointConfig {
    /// Enable checkpointing
    pub enabled: bool,
    /// Checkpoint interval (seconds)
    pub interval: u64,
    /// Checkpoint directory
    pub checkpoint_dir: PathBuf,
    /// Maximum number of checkpoints to keep
    pub max_checkpoints: usize,
}

impl Default for StateConfig {
    fn default() -> Self {
        Self {
            backend_type: StateBackendType::Memory,
            state_dir: PathBuf::from("./state"),
            checkpoint: CheckpointConfig::default(),
        }
    }
}

impl Default for CheckpointConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            interval: 60,
            checkpoint_dir: PathBuf::from("./checkpoints"),
            max_checkpoints: 10,
        }
    }
}
