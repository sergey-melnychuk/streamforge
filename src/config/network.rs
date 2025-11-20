//! Network configuration

use serde::{Deserialize, Serialize};
use std::time::Duration;

/// Network configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NetworkConfig {
    /// TCP keepalive (seconds)
    pub tcp_keepalive: u64,
    /// Connection timeout (seconds)
    pub connection_timeout: u64,
    /// Max connections
    pub max_connections: usize,
    /// Send buffer size (bytes)
    pub send_buffer_size: usize,
    /// Receive buffer size (bytes)
    pub recv_buffer_size: usize,
}

impl Default for NetworkConfig {
    fn default() -> Self {
        Self {
            tcp_keepalive: 60,
            connection_timeout: 30,
            max_connections: 1000,
            send_buffer_size: 64 * 1024,  // 64KB
            recv_buffer_size: 64 * 1024,  // 64KB
        }
    }
}

