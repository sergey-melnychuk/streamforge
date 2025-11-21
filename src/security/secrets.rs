//! Secrets management for secure credential storage and retrieval

use async_trait::async_trait;
use std::collections::HashMap;
use std::sync::Arc;
use thiserror::Error;
use tokio::sync::RwLock;

/// Error type for secrets operations
#[derive(Debug, Error)]
pub enum SecretsError {
    #[error("Secret not found: {0}")]
    NotFound(String),
    #[error("Access denied: {0}")]
    AccessDenied(String),
    #[error("Invalid secret format: {0}")]
    InvalidFormat(String),
    #[error("Secrets backend error: {0}")]
    BackendError(String),
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
}

/// Result type for secrets operations
pub type SecretsResult<T> = Result<T, SecretsError>;

/// A secret value (encrypted in memory)
#[derive(Debug, Clone)]
pub struct Secret {
    /// Secret key/name
    pub key: String,
    /// Secret value (should be encrypted at rest)
    pub value: Vec<u8>,
    /// Metadata about the secret
    pub metadata: HashMap<String, String>,
}

impl Secret {
    /// Create a new secret
    pub fn new(key: impl Into<String>, value: Vec<u8>) -> Self {
        Self {
            key: key.into(),
            value,
            metadata: HashMap::new(),
        }
    }

    /// Create a secret from a string
    pub fn from_string(key: impl Into<String>, value: impl Into<String>) -> Self {
        Self::new(key, value.into().into_bytes())
    }

    /// Get the secret value as a string
    pub fn as_string(&self) -> SecretsResult<String> {
        String::from_utf8(self.value.clone())
            .map_err(|e| SecretsError::InvalidFormat(format!("Invalid UTF-8: {}", e)))
    }

    /// Get the secret value as bytes
    pub fn as_bytes(&self) -> &[u8] {
        &self.value
    }
}

/// Trait for secrets management backends
#[async_trait]
pub trait SecretsBackend: Send + Sync {
    /// Get a secret by key
    async fn get(&self, key: &str) -> SecretsResult<Secret>;

    /// Store a secret
    async fn put(&self, secret: Secret) -> SecretsResult<()>;

    /// Delete a secret
    async fn delete(&self, key: &str) -> SecretsResult<()>;

    /// List all secret keys (optional, may not be supported by all backends)
    async fn list(&self) -> SecretsResult<Vec<String>>;

    /// Check if a secret exists
    async fn exists(&self, key: &str) -> bool {
        self.get(key).await.is_ok()
    }
}

/// In-memory secrets backend (for development/testing)
pub struct MemorySecretsBackend {
    secrets: Arc<RwLock<HashMap<String, Secret>>>,
}

impl MemorySecretsBackend {
    /// Create a new in-memory secrets backend
    pub fn new() -> Self {
        Self {
            secrets: Arc::new(RwLock::new(HashMap::new())),
        }
    }
}

impl Default for MemorySecretsBackend {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl SecretsBackend for MemorySecretsBackend {
    async fn get(&self, key: &str) -> SecretsResult<Secret> {
        let secrets = self.secrets.read().await;
        secrets
            .get(key)
            .cloned()
            .ok_or_else(|| SecretsError::NotFound(key.to_string()))
    }

    async fn put(&self, secret: Secret) -> SecretsResult<()> {
        let mut secrets = self.secrets.write().await;
        secrets.insert(secret.key.clone(), secret);
        Ok(())
    }

    async fn delete(&self, key: &str) -> SecretsResult<()> {
        let mut secrets = self.secrets.write().await;
        secrets
            .remove(key)
            .ok_or_else(|| SecretsError::NotFound(key.to_string()))
            .map(|_| ())
    }

    async fn list(&self) -> SecretsResult<Vec<String>> {
        let secrets = self.secrets.read().await;
        Ok(secrets.keys().cloned().collect())
    }
}

/// File-based secrets backend (for simple deployments)
pub struct FileSecretsBackend {
    base_path: std::path::PathBuf,
}

impl FileSecretsBackend {
    /// Create a new file-based secrets backend
    pub fn new(base_path: impl AsRef<std::path::Path>) -> Self {
        Self {
            base_path: base_path.as_ref().to_path_buf(),
        }
    }

    fn secret_path(&self, key: &str) -> std::path::PathBuf {
        // Sanitize key to prevent path traversal
        let sanitized = key.replace("..", "").replace("/", "_");
        self.base_path.join(sanitized)
    }
}

#[async_trait]
impl SecretsBackend for FileSecretsBackend {
    async fn get(&self, key: &str) -> SecretsResult<Secret> {
        let path = self.secret_path(key);
        let data = tokio::fs::read(&path).await?;

        // Simple format: first line is metadata (JSON), rest is value
        let content = String::from_utf8(data)
            .map_err(|e| SecretsError::InvalidFormat(format!("Invalid UTF-8: {}", e)))?;

        let mut lines = content.lines();
        let metadata_json = lines
            .next()
            .ok_or_else(|| SecretsError::InvalidFormat("Missing metadata".to_string()))?;
        let value = lines.collect::<Vec<_>>().join("\n");

        let metadata: HashMap<String, String> =
            serde_json::from_str(metadata_json).unwrap_or_default();

        Ok(Secret {
            key: key.to_string(),
            value: value.into_bytes(),
            metadata,
        })
    }

    async fn put(&self, secret: Secret) -> SecretsResult<()> {
        // Ensure directory exists
        tokio::fs::create_dir_all(&self.base_path).await?;

        let path = self.secret_path(&secret.key);
        let metadata_json = serde_json::to_string(&secret.metadata)
            .map_err(|e| SecretsError::InvalidFormat(format!("Invalid metadata: {}", e)))?;

        let value_str = String::from_utf8(secret.value.clone())
            .map_err(|e| SecretsError::InvalidFormat(format!("Invalid UTF-8: {}", e)))?;

        let content = format!("{}\n{}", metadata_json, value_str);
        tokio::fs::write(&path, content).await?;
        Ok(())
    }

    async fn delete(&self, key: &str) -> SecretsResult<()> {
        let path = self.secret_path(key);
        tokio::fs::remove_file(&path).await?;
        Ok(())
    }

    async fn list(&self) -> SecretsResult<Vec<String>> {
        let mut entries = tokio::fs::read_dir(&self.base_path).await?;
        let mut keys = Vec::new();

        while let Some(entry) = entries.next_entry().await? {
            if entry.file_type().await?.is_file() {
                if let Some(name) = entry.file_name().to_str() {
                    keys.push(name.to_string());
                }
            }
        }

        Ok(keys)
    }
}

/// Secrets manager that wraps a backend
pub struct SecretsManager {
    backend: Arc<dyn SecretsBackend>,
}

impl SecretsManager {
    /// Create a new secrets manager with a backend
    pub fn new(backend: Arc<dyn SecretsBackend>) -> Self {
        Self { backend }
    }

    /// Create a memory-based secrets manager
    pub fn memory() -> Self {
        Self::new(Arc::new(MemorySecretsBackend::new()))
    }

    /// Create a file-based secrets manager
    pub fn file(base_path: impl AsRef<std::path::Path>) -> Self {
        Self::new(Arc::new(FileSecretsBackend::new(base_path)))
    }

    /// Get a secret
    pub async fn get(&self, key: &str) -> SecretsResult<Secret> {
        self.backend.get(key).await
    }

    /// Get a secret as a string
    pub async fn get_string(&self, key: &str) -> SecretsResult<String> {
        let secret = self.get(key).await?;
        secret.as_string()
    }

    /// Store a secret
    pub async fn put(&self, secret: Secret) -> SecretsResult<()> {
        self.backend.put(secret).await
    }

    /// Store a secret from a string
    pub async fn put_string(
        &self,
        key: impl Into<String>,
        value: impl Into<String>,
    ) -> SecretsResult<()> {
        let secret = Secret::from_string(key, value);
        self.put(secret).await
    }

    /// Delete a secret
    pub async fn delete(&self, key: &str) -> SecretsResult<()> {
        self.backend.delete(key).await
    }

    /// List all secret keys
    pub async fn list(&self) -> SecretsResult<Vec<String>> {
        self.backend.list().await
    }

    /// Check if a secret exists
    pub async fn exists(&self, key: &str) -> bool {
        self.backend.exists(key).await
    }
}

/// Configuration for secrets management
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, Default)]
pub struct SecretsConfig {
    /// Backend type
    pub backend_type: SecretsBackendType,
    /// Backend-specific configuration
    pub backend_config: HashMap<String, String>,
}

/// Type of secrets backend
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize, Default)]
pub enum SecretsBackendType {
    /// In-memory backend (development only)
    #[default]
    Memory,
    /// File-based backend
    File,
    /// HashiCorp Vault (requires feature flag)
    #[cfg(feature = "vault")]
    Vault,
    /// AWS Secrets Manager (requires feature flag)
    #[cfg(feature = "aws-secrets")]
    AwsSecretsManager,
}

impl SecretsConfig {
    /// Create a memory-based config
    pub fn memory() -> Self {
        Self {
            backend_type: SecretsBackendType::Memory,
            backend_config: HashMap::new(),
        }
    }

    /// Create a file-based config
    pub fn file(base_path: impl Into<String>) -> Self {
        let mut config = HashMap::new();
        config.insert("base_path".to_string(), base_path.into());
        Self {
            backend_type: SecretsBackendType::File,
            backend_config: config,
        }
    }

    /// Build a secrets manager from this config
    pub fn build_manager(&self) -> SecretsResult<SecretsManager> {
        match self.backend_type {
            SecretsBackendType::Memory => Ok(SecretsManager::memory()),
            SecretsBackendType::File => {
                let base_path = self.backend_config.get("base_path").ok_or_else(|| {
                    SecretsError::BackendError("Missing base_path for file backend".to_string())
                })?;
                Ok(SecretsManager::file(base_path))
            }
            #[cfg(feature = "vault")]
            SecretsBackendType::Vault => Err(SecretsError::BackendError(
                "Vault backend requires 'vault' feature flag".to_string(),
            )),
            #[cfg(feature = "aws-secrets")]
            SecretsBackendType::AwsSecretsManager => Err(SecretsError::BackendError(
                "AWS Secrets Manager backend requires 'aws-secrets' feature flag".to_string(),
            )),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_memory_backend() {
        let backend = MemorySecretsBackend::new();

        let secret = Secret::from_string("test-key", "test-value");
        backend.put(secret).await.unwrap();

        let retrieved = backend.get("test-key").await.unwrap();
        assert_eq!(retrieved.as_string().unwrap(), "test-value");

        let keys = backend.list().await.unwrap();
        assert!(keys.contains(&"test-key".to_string()));

        backend.delete("test-key").await.unwrap();
        assert!(!backend.exists("test-key").await);
    }

    #[tokio::test]
    async fn test_secrets_manager() {
        let manager = SecretsManager::memory();

        manager.put_string("key1", "value1").await.unwrap();
        let value = manager.get_string("key1").await.unwrap();
        assert_eq!(value, "value1");

        assert!(manager.exists("key1").await);
        assert!(!manager.exists("key2").await);

        let keys = manager.list().await.unwrap();
        assert!(keys.contains(&"key1".to_string()));
    }

    #[tokio::test]
    async fn test_file_backend() {
        let temp_dir = std::env::temp_dir().join("streamforge_secrets_test");
        let backend = FileSecretsBackend::new(&temp_dir);

        let secret = Secret::from_string("file-key", "file-value");
        backend.put(secret).await.unwrap();

        let retrieved = backend.get("file-key").await.unwrap();
        assert_eq!(retrieved.as_string().unwrap(), "file-value");

        backend.delete("file-key").await.unwrap();

        // Cleanup
        let _ = tokio::fs::remove_dir_all(&temp_dir).await;
    }

    #[tokio::test]
    async fn test_secrets_config() {
        let config = SecretsConfig::memory();
        let manager = config.build_manager().unwrap();
        manager
            .put_string("config-key", "config-value")
            .await
            .unwrap();
        assert_eq!(
            manager.get_string("config-key").await.unwrap(),
            "config-value"
        );
    }
}
