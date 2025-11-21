//! Security configuration

use crate::security::secrets::SecretsConfig;
use crate::security::tls::TlsConfig;
use serde::{Deserialize, Serialize};

/// Node authentication configuration
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct NodeAuthConfig {
    /// Shared secret for node authentication
    pub shared_secret: Option<String>,
    /// Allowed node IDs (whitelist, if None, all nodes allowed)
    pub allowed_nodes: Option<Vec<u64>>,
}

/// User authentication configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UserAuthConfig {
    /// Token TTL in seconds
    pub token_ttl_seconds: u64,
    /// Enable password authentication
    pub enable_password_auth: bool,
    /// Enable API key authentication
    pub enable_api_key_auth: bool,
}

impl Default for UserAuthConfig {
    fn default() -> Self {
        Self {
            token_ttl_seconds: 3600, // 1 hour
            enable_password_auth: true,
            enable_api_key_auth: true,
        }
    }
}

/// Security configuration for StreamForge
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct SecurityConfig {
    /// TLS configuration
    pub tls: TlsConfig,
    /// Node authentication configuration
    pub node_auth: NodeAuthConfig,
    /// User authentication configuration
    pub user_auth: UserAuthConfig,
    /// Secrets management configuration
    pub secrets: SecretsConfig,
    /// Enable RBAC
    pub enable_rbac: bool,
    /// Audit log path (optional)
    pub audit_log_path: Option<String>,
}

impl SecurityConfig {
    /// Create a new security configuration
    pub fn new() -> Self {
        Self::default()
    }

    /// Enable TLS
    pub fn with_tls(mut self, tls: TlsConfig) -> Self {
        self.tls = tls;
        self
    }

    /// Set secrets configuration
    pub fn with_secrets(mut self, secrets: SecretsConfig) -> Self {
        self.secrets = secrets;
        self
    }
}
