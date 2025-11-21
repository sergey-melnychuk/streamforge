//! Security module for StreamForge
//!
//! Provides TLS/SSL encryption, authentication, and authorization

pub mod auth;
pub mod config;
pub mod rbac;
pub mod secrets;
pub mod tls;

pub use auth::{has_permission, AuthError, AuthToken, NodeAuth, Permission, User, UserAuth};
pub use config::{NodeAuthConfig, SecurityConfig, UserAuthConfig};
pub use rbac::{RbacManager, Role};
pub use secrets::{
    Secret, SecretsBackend, SecretsBackendType, SecretsConfig, SecretsError, SecretsManager,
    SecretsResult,
};
pub use tls::{TlsConfig, TlsTransport};
