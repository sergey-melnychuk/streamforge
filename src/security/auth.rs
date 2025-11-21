//! Authentication and authorization

use crate::distributed::node::NodeId;
use hmac::{Hmac, Mac};
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use thiserror::Error;
use tokio::sync::RwLock;

/// Error type for authentication operations
#[derive(Debug, Error)]
pub enum AuthError {
    #[error("Authentication failed: {0}")]
    AuthenticationFailed(String),
    #[error("Authorization failed: {0}")]
    AuthorizationFailed(String),
    #[error("Invalid credentials")]
    InvalidCredentials,
    #[error("Token expired")]
    TokenExpired,
    #[error("Permission denied")]
    PermissionDenied,
    #[error("Node not found")]
    NodeNotFound,
    #[error("User not found")]
    UserNotFound,
}

/// Authentication token
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AuthToken {
    /// Token value
    pub value: String,
    /// Expiration timestamp (Unix epoch seconds)
    pub expires_at: u64,
    /// User ID or node ID this token belongs to
    pub subject: String,
    /// Permissions/roles associated with this token
    pub permissions: Vec<String>,
}

impl AuthToken {
    /// Check if token is expired
    pub fn is_expired(&self) -> bool {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();
        self.expires_at <= now
    }
}

/// Node authentication (for cluster nodes)
pub struct NodeAuth {
    /// Shared secret for node authentication
    shared_secret: Arc<RwLock<Option<String>>>,
    /// Allowed node IDs (if None, all nodes are allowed)
    allowed_nodes: Arc<RwLock<Option<Vec<NodeId>>>>,
}

impl Default for NodeAuth {
    fn default() -> Self {
        NodeAuth {
            shared_secret: Arc::new(RwLock::new(None)),
            allowed_nodes: Arc::new(RwLock::new(None)),
        }
    }
}

impl NodeAuth {
    /// Create a new node authenticator
    pub fn new() -> Self {
        Self::default()
    }

    /// Create with a shared secret
    pub fn with_secret(secret: impl Into<String>) -> Self {
        Self {
            shared_secret: Arc::new(RwLock::new(Some(secret.into()))),
            allowed_nodes: Arc::new(RwLock::new(None)),
        }
    }

    /// Set the shared secret
    pub async fn set_secret(&self, secret: impl Into<String>) {
        *self.shared_secret.write().await = Some(secret.into());
    }

    /// Set allowed node IDs (whitelist)
    pub async fn set_allowed_nodes(&self, nodes: Vec<NodeId>) {
        *self.allowed_nodes.write().await = Some(nodes);
    }

    /// Authenticate a node using shared secret
    pub async fn authenticate(
        &self,
        node_id: NodeId,
        credentials: &[u8],
    ) -> Result<bool, AuthError> {
        // Check if node is in whitelist (if whitelist is set)
        {
            let allowed = self.allowed_nodes.read().await;
            if let Some(ref allowed_nodes) = *allowed {
                if !allowed_nodes.contains(&node_id) {
                    return Err(AuthError::NodeNotFound);
                }
            }
        }

        // Check shared secret
        let secret = self.shared_secret.read().await;
        if let Some(ref secret) = *secret {
            // Compute HMAC-SHA256 of node_id with secret
            let mut mac = Hmac::<Sha256>::new_from_slice(secret.as_bytes())
                .map_err(|e| AuthError::AuthenticationFailed(format!("Invalid secret: {}", e)))?;
            mac.update(node_id.to_string().as_bytes());
            let expected = mac.finalize().into_bytes();

            // Compare with provided credentials
            if expected.as_slice() == credentials {
                Ok(true)
            } else {
                Err(AuthError::InvalidCredentials)
            }
        } else {
            // No secret configured, allow all (insecure, for development)
            Ok(true)
        }
    }

    /// Generate credentials for a node (HMAC of node_id with secret)
    pub async fn generate_credentials(&self, node_id: NodeId) -> Result<Vec<u8>, AuthError> {
        let secret = self.shared_secret.read().await;
        if let Some(ref secret) = *secret {
            let mut mac = Hmac::<Sha256>::new_from_slice(secret.as_bytes())
                .map_err(|e| AuthError::AuthenticationFailed(format!("Invalid secret: {}", e)))?;
            mac.update(node_id.to_string().as_bytes());
            Ok(mac.finalize().into_bytes().to_vec())
        } else {
            Err(AuthError::AuthenticationFailed(
                "No secret configured".to_string(),
            ))
        }
    }
}

/// User information
#[derive(Debug, Clone)]
pub struct User {
    /// User ID
    pub id: String,
    /// Username
    pub username: String,
    /// Password hash (bcrypt or similar)
    pub password_hash: Option<String>,
    /// API keys associated with this user
    pub api_keys: Vec<String>,
    /// Roles assigned to this user
    pub roles: Vec<String>,
    /// Permissions assigned to this user
    pub permissions: Vec<String>,
}

/// User authentication (for API users)
pub struct UserAuth {
    /// Users by username
    users: Arc<RwLock<HashMap<String, User>>>,
    /// Users by API key
    users_by_api_key: Arc<RwLock<HashMap<String, String>>>,
    /// Active tokens
    tokens: Arc<RwLock<HashMap<String, AuthToken>>>,
    /// Token expiration duration
    token_ttl: Duration,
}

impl Default for UserAuth {
    fn default() -> Self {
        Self {
            users: Arc::new(RwLock::new(HashMap::new())),
            users_by_api_key: Arc::new(RwLock::new(HashMap::new())),
            tokens: Arc::new(RwLock::new(HashMap::new())),
            token_ttl: Duration::from_secs(3600), // 1 hour default
        }
    }
}

impl UserAuth {
    /// Create a new user authenticator
    pub fn new() -> Self {
        Self::default()
    }

    /// Create with custom token TTL
    pub fn with_token_ttl(ttl: Duration) -> Self {
        Self {
            users: Arc::new(RwLock::new(HashMap::new())),
            users_by_api_key: Arc::new(RwLock::new(HashMap::new())),
            tokens: Arc::new(RwLock::new(HashMap::new())),
            token_ttl: ttl,
        }
    }

    /// Add a user
    pub async fn add_user(&self, user: User) {
        let mut users = self.users.write().await;
        let mut users_by_key = self.users_by_api_key.write().await;

        // Index user by API keys
        for api_key in &user.api_keys {
            users_by_key.insert(api_key.clone(), user.id.clone());
        }

        users.insert(user.username.clone(), user);
    }

    /// Create a user with password
    pub async fn create_user(
        &self,
        id: impl Into<String>,
        username: impl Into<String>,
        password: impl Into<String>,
    ) -> Result<(), AuthError> {
        let id = id.into();
        let username = username.into();
        let password = password.into();

        // Hash password (simple SHA256 for now, should use bcrypt/argon2 in production)
        let mut hasher = Sha256::new();
        hasher.update(password.as_bytes());
        let password_hash = hex::encode(hasher.finalize());

        let user = User {
            id: id.clone(),
            username: username.clone(),
            password_hash: Some(password_hash),
            api_keys: Vec::new(),
            roles: Vec::new(),
            permissions: Vec::new(),
        };

        self.add_user(user).await;
        Ok(())
    }

    /// Generate an API key for a user
    pub async fn generate_api_key(&self, username: &str) -> Result<String, AuthError> {
        let mut users = self.users.write().await;
        let user = users.get_mut(username).ok_or(AuthError::UserNotFound)?;

        // Generate random API key
        let api_key = generate_random_key();
        user.api_keys.push(api_key.clone());

        // Index by API key
        let mut users_by_key = self.users_by_api_key.write().await;
        users_by_key.insert(api_key.clone(), user.id.clone());

        Ok(api_key)
    }

    /// Authenticate a user with username and password
    pub async fn authenticate(
        &self,
        username: &str,
        password: &str,
    ) -> Result<AuthToken, AuthError> {
        let users = self.users.read().await;
        let user = users.get(username).ok_or(AuthError::InvalidCredentials)?;

        // Verify password
        if let Some(ref password_hash) = user.password_hash {
            let mut hasher = Sha256::new();
            hasher.update(password.as_bytes());
            let computed_hash = hex::encode(hasher.finalize());

            if computed_hash != *password_hash {
                return Err(AuthError::InvalidCredentials);
            }
        } else {
            return Err(AuthError::InvalidCredentials);
        }

        // Generate token
        self.create_token(&user.id, &user.permissions).await
    }

    /// Authenticate with API key
    pub async fn authenticate_with_api_key(&self, api_key: &str) -> Result<AuthToken, AuthError> {
        let users_by_key = self.users_by_api_key.read().await;
        let user_id = users_by_key
            .get(api_key)
            .ok_or(AuthError::InvalidCredentials)?
            .clone();
        drop(users_by_key);

        // Get user to get permissions
        let users = self.users.read().await;
        let user = users
            .values()
            .find(|u| u.id == user_id)
            .ok_or(AuthError::UserNotFound)?;

        // Generate token
        self.create_token(&user.id, &user.permissions).await
    }

    /// Verify a token
    pub async fn verify_token(&self, token: &str) -> Result<AuthToken, AuthError> {
        let tokens = self.tokens.read().await;
        let auth_token = tokens
            .get(token)
            .ok_or(AuthError::InvalidCredentials)?
            .clone();
        drop(tokens);

        if auth_token.is_expired() {
            // Remove expired token
            let mut tokens = self.tokens.write().await;
            tokens.remove(token);
            return Err(AuthError::TokenExpired);
        }

        Ok(auth_token)
    }

    /// Create a token for a user
    async fn create_token(
        &self,
        user_id: &str,
        permissions: &[String],
    ) -> Result<AuthToken, AuthError> {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();
        let expires_at = now + self.token_ttl.as_secs();

        // Generate token (simple random string, should use JWT in production)
        let token_value = generate_random_key();

        let token = AuthToken {
            value: token_value.clone(),
            expires_at,
            subject: user_id.to_string(),
            permissions: permissions.to_vec(),
        };

        // Store token
        let mut tokens = self.tokens.write().await;
        tokens.insert(token_value.clone(), token.clone());

        Ok(token)
    }

    /// Revoke a token
    pub async fn revoke_token(&self, token: &str) {
        let mut tokens = self.tokens.write().await;
        tokens.remove(token);
    }

    /// Get user by username
    pub async fn get_user(&self, username: &str) -> Option<User> {
        let users = self.users.read().await;
        users.get(username).cloned()
    }

    /// Add role to user
    pub async fn add_role(&self, username: &str, role: impl Into<String>) -> Result<(), AuthError> {
        let mut users = self.users.write().await;
        let user = users.get_mut(username).ok_or(AuthError::UserNotFound)?;
        user.roles.push(role.into());
        Ok(())
    }

    /// Add permission to user
    pub async fn add_permission(
        &self,
        username: &str,
        permission: impl Into<String>,
    ) -> Result<(), AuthError> {
        let mut users = self.users.write().await;
        let user = users.get_mut(username).ok_or(AuthError::UserNotFound)?;
        user.permissions.push(permission.into());
        Ok(())
    }
}

/// Generate a random key (for API keys and tokens)
fn generate_random_key() -> String {
    use rand::Rng;
    let mut rng = rand::thread_rng();
    let bytes: Vec<u8> = (0..32).map(|_| rng.gen()).collect();
    hex::encode(bytes)
}

/// Role-based access control
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Permission {
    /// Read operations
    Read,
    /// Write operations
    Write,
    /// Admin operations
    Admin,
    /// Custom permission
    Custom(String),
}

impl Permission {
    /// Check if a permission string matches this permission
    pub fn matches(&self, permission: &str) -> bool {
        match self {
            Permission::Read => permission == "read" || permission == "*",
            Permission::Write => permission == "write" || permission == "*",
            Permission::Admin => permission == "admin" || permission == "*",
            Permission::Custom(custom) => custom == permission || permission == "*",
        }
    }
}

/// Check if a user has a specific permission
pub fn has_permission(token: &AuthToken, permission: &str) -> bool {
    // Check for wildcard permission
    if token.permissions.contains(&"*".to_string()) {
        return true;
    }

    // Check for exact match
    if token.permissions.contains(&permission.to_string()) {
        return true;
    }

    // Check for role-based permissions
    for perm in &token.permissions {
        if perm.starts_with("role:") {
            // Role-based permission (e.g., "role:admin" grants admin permissions)
            let role = perm.strip_prefix("role:").unwrap();
            if role == "admin" {
                return true; // Admin has all permissions
            }
        }
    }

    false
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_node_auth_with_secret() {
        let auth = NodeAuth::with_secret("my-secret-key");
        let node_id = NodeId::new(1);

        // Generate credentials
        let credentials = auth.generate_credentials(node_id).await.unwrap();

        // Authenticate
        let result = auth.authenticate(node_id, &credentials).await;
        assert!(result.is_ok());
        assert!(result.unwrap());
    }

    #[tokio::test]
    async fn test_node_auth_invalid_credentials() {
        let auth = NodeAuth::with_secret("my-secret-key");
        let node_id = NodeId::new(1);

        // Try with wrong credentials
        let result = auth.authenticate(node_id, b"wrong-credentials").await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_user_auth() {
        let auth = UserAuth::new();

        // Create user
        auth.create_user("user1", "alice", "password123")
            .await
            .unwrap();

        // Authenticate
        let token = auth.authenticate("alice", "password123").await.unwrap();
        assert!(!token.is_expired());
        assert_eq!(token.subject, "user1");

        // Verify token
        let verified = auth.verify_token(&token.value).await.unwrap();
        assert_eq!(verified.subject, "user1");
    }

    #[tokio::test]
    async fn test_api_key_auth() {
        let auth = UserAuth::new();

        // Create user
        auth.create_user("user1", "alice", "password123")
            .await
            .unwrap();

        // Generate API key
        let api_key = auth.generate_api_key("alice").await.unwrap();

        // Authenticate with API key
        let token = auth.authenticate_with_api_key(&api_key).await.unwrap();
        assert_eq!(token.subject, "user1");
    }

    #[tokio::test]
    async fn test_permissions() {
        let auth = UserAuth::new();

        // Create user with permissions
        auth.create_user("user1", "alice", "password123")
            .await
            .unwrap();
        auth.add_permission("alice", "read").await.unwrap();
        auth.add_permission("alice", "write").await.unwrap();

        // Authenticate
        let token = auth.authenticate("alice", "password123").await.unwrap();

        // Check permissions
        assert!(has_permission(&token, "read"));
        assert!(has_permission(&token, "write"));
        assert!(!has_permission(&token, "admin"));
    }

    #[tokio::test]
    async fn test_token_expiration() {
        let auth = UserAuth::with_token_ttl(Duration::from_secs(1));

        auth.create_user("user1", "alice", "password123")
            .await
            .unwrap();
        let token = auth.authenticate("alice", "password123").await.unwrap();

        // Token should be valid
        assert!(!token.is_expired());

        // Wait for expiration
        tokio::time::sleep(Duration::from_secs(2)).await;

        // Token should be expired
        assert!(token.is_expired());
        let result = auth.verify_token(&token.value).await;
        assert!(result.is_err());
    }
}
