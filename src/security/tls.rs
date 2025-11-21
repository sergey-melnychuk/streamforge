//! TLS/SSL support for encrypted network communication

use rustls::{ClientConfig, ServerConfig};
use rustls_pemfile::{certs, pkcs8_private_keys};
use serde::{Deserialize, Serialize};
use std::fs::File;
use std::io::{BufReader, Error as IoError};
use std::path::Path;
use std::sync::Arc;
use thiserror::Error;

/// Error type for TLS operations
#[derive(Debug, Error)]
pub enum TlsError {
    #[error("IO error: {0}")]
    Io(#[from] IoError),
    #[error("TLS configuration error: {0}")]
    Config(String),
    #[error("Certificate error: {0}")]
    Certificate(String),
    #[error("Private key error: {0}")]
    PrivateKey(String),
}

/// TLS configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TlsConfig {
    /// Enable TLS
    pub enabled: bool,
    /// Path to certificate file (PEM format)
    pub cert_file: Option<String>,
    /// Path to private key file (PEM format)
    pub key_file: Option<String>,
    /// Path to CA certificate file for client verification (optional)
    pub ca_file: Option<String>,
    /// Require client certificates (mutual TLS)
    pub require_client_cert: bool,
    /// Minimum TLS version (1.2 or 1.3)
    #[serde(default)]
    pub min_tls_version: TlsVersion,
}

/// TLS version
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
pub enum TlsVersion {
    #[default]
    V1_2,
    V1_3,
}

impl Default for TlsConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            cert_file: None,
            key_file: None,
            ca_file: None,
            require_client_cert: false,
            min_tls_version: TlsVersion::V1_2,
        }
    }
}

impl TlsConfig {
    /// Create a new TLS configuration
    pub fn new() -> Self {
        Self::default()
    }

    /// Enable TLS
    pub fn enable(mut self) -> Self {
        self.enabled = true;
        self
    }

    /// Set certificate file
    pub fn with_cert_file(mut self, cert_file: impl Into<String>) -> Self {
        self.cert_file = Some(cert_file.into());
        self
    }

    /// Set private key file
    pub fn with_key_file(mut self, key_file: impl Into<String>) -> Self {
        self.key_file = Some(key_file.into());
        self
    }

    /// Set CA certificate file
    pub fn with_ca_file(mut self, ca_file: impl Into<String>) -> Self {
        self.ca_file = Some(ca_file.into());
        self
    }

    /// Require client certificates (mutual TLS)
    pub fn require_client_cert(mut self, require: bool) -> Self {
        self.require_client_cert = require;
        self
    }

    /// Build server TLS configuration
    pub fn build_server_config(&self) -> Result<Arc<ServerConfig>, TlsError> {
        if !self.enabled {
            return Err(TlsError::Config("TLS is not enabled".to_string()));
        }

        let cert_file = self
            .cert_file
            .as_ref()
            .ok_or_else(|| TlsError::Config("Certificate file not specified".to_string()))?;
        let key_file = self
            .key_file
            .as_ref()
            .ok_or_else(|| TlsError::Config("Private key file not specified".to_string()))?;

        // Load certificate
        let certs = load_certs(cert_file)?;
        if certs.is_empty() {
            return Err(TlsError::Certificate("No certificates found".to_string()));
        }

        // Load private key
        let key = load_private_key(key_file)?;

        // Build server config
        let config = if self.require_client_cert {
            if let Some(ca_file) = &self.ca_file {
                let mut root_store = rustls::RootCertStore::empty();
                let ca_certs = load_certs(ca_file)?;
                for cert in ca_certs {
                    root_store.add(&cert).map_err(|e| {
                        TlsError::Certificate(format!("Failed to add CA cert: {}", e))
                    })?;
                }
                ServerConfig::builder()
                    .with_safe_defaults()
                    .with_client_cert_verifier(Arc::new(
                        rustls::server::AllowAnyAuthenticatedClient::new(root_store),
                    ))
                    .with_single_cert(certs, key)
                    .map_err(|e| {
                        TlsError::Config(format!(
                            "Failed to build server config with client auth: {}",
                            e
                        ))
                    })?
            } else {
                return Err(TlsError::Config(
                    "CA file required for client certificate verification".to_string(),
                ));
            }
        } else {
            ServerConfig::builder()
                .with_safe_defaults()
                .with_no_client_auth()
                .with_single_cert(certs, key)
                .map_err(|e| TlsError::Config(format!("Failed to build server config: {}", e)))?
        };

        Ok(Arc::new(config))
    }

    /// Build client TLS configuration
    pub fn build_client_config(&self) -> Result<Arc<ClientConfig>, TlsError> {
        if !self.enabled {
            return Err(TlsError::Config("TLS is not enabled".to_string()));
        }

        let mut root_store = rustls::RootCertStore::empty();

        // Load CA certificates if provided
        if let Some(ca_file) = &self.ca_file {
            let ca_certs = load_certs(ca_file)?;
            for cert in ca_certs {
                root_store
                    .add(&cert)
                    .map_err(|e| TlsError::Certificate(format!("Failed to add CA cert: {}", e)))?;
            }
        } else {
            // Use system root certificates
            let native_certs = rustls_native_certs::load_native_certs().map_err(|e| {
                TlsError::Certificate(format!("Failed to load system certs: {}", e))
            })?;
            for cert in native_certs {
                root_store.add(&rustls::Certificate(cert.0)).map_err(|e| {
                    TlsError::Certificate(format!("Failed to add system cert: {}", e))
                })?;
            }
        }

        // Build client config
        let config = if let (Some(cert_file), Some(key_file)) = (&self.cert_file, &self.key_file) {
            // Client certificate provided (for mutual TLS)
            let certs = load_certs(cert_file)?;
            let key = load_private_key(key_file)?;
            ClientConfig::builder()
                .with_safe_defaults()
                .with_root_certificates(root_store)
                .with_client_auth_cert(certs, key)
                .map_err(|e| {
                    TlsError::Config(format!("Failed to build client config with cert: {}", e))
                })?
        } else {
            // No client certificate
            ClientConfig::builder()
                .with_safe_defaults()
                .with_root_certificates(root_store)
                .with_no_client_auth()
        };

        Ok(Arc::new(config))
    }
}

/// Load certificates from a PEM file
fn load_certs(path: impl AsRef<Path>) -> Result<Vec<rustls::Certificate>, TlsError> {
    let file = File::open(path)?;
    let mut reader = BufReader::new(file);
    let certs = certs(&mut reader)
        .map_err(|e| TlsError::Certificate(format!("Failed to parse certificates: {}", e)))?
        .into_iter()
        .map(rustls::Certificate)
        .collect();
    Ok(certs)
}

/// Load private key from a PEM file
fn load_private_key(path: impl AsRef<Path>) -> Result<rustls::PrivateKey, TlsError> {
    let file = File::open(path)?;
    let mut reader = BufReader::new(file);
    let keys = pkcs8_private_keys(&mut reader)
        .map_err(|e| TlsError::PrivateKey(format!("Failed to parse private key: {}", e)))?;

    if keys.is_empty() {
        return Err(TlsError::PrivateKey("No private keys found".to_string()));
    }

    Ok(rustls::PrivateKey(keys[0].clone()))
}

/// TLS transport wrapper (placeholder for future TLS transport implementation)
pub struct TlsTransport {
    // This will wrap the actual TLS connection
    // For now, it's a placeholder
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_tls_config_default() {
        let config = TlsConfig::default();
        assert!(!config.enabled);
        assert!(config.cert_file.is_none());
        assert!(config.key_file.is_none());
    }

    #[test]
    fn test_tls_config_builder() {
        let config = TlsConfig::new()
            .enable()
            .with_cert_file("cert.pem")
            .with_key_file("key.pem");

        assert!(config.enabled);
        assert_eq!(config.cert_file, Some("cert.pem".to_string()));
        assert_eq!(config.key_file, Some("key.pem".to_string()));
    }
}
