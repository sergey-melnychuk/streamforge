//! Role-Based Access Control (RBAC)

use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

/// Role definition
#[derive(Debug, Clone)]
pub struct Role {
    /// Role name
    pub name: String,
    /// Permissions granted by this role
    pub permissions: Vec<String>,
}

impl Role {
    /// Create a new role
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            permissions: Vec::new(),
        }
    }

    /// Add a permission to this role
    pub fn with_permission(mut self, permission: impl Into<String>) -> Self {
        self.permissions.push(permission.into());
        self
    }

    /// Check if role has a permission
    pub fn has_permission(&self, permission: &str) -> bool {
        // Wildcard grants all permissions
        if self.permissions.contains(&"*".to_string()) {
            return true;
        }
        self.permissions.contains(&permission.to_string())
    }
}

/// RBAC manager
pub struct RbacManager {
    /// Roles by name
    roles: Arc<RwLock<HashMap<String, Role>>>,
    /// User roles (user_id -> role_names)
    user_roles: Arc<RwLock<HashMap<String, Vec<String>>>>,
}

impl Default for RbacManager {
    fn default() -> Self {
        Self {
            roles: Arc::new(RwLock::new(HashMap::new())),
            user_roles: Arc::new(RwLock::new(HashMap::new())),
        }
    }
}

impl RbacManager {
    /// Create a new RBAC manager
    pub fn new() -> Self {
        Self::default()
    }

    /// Create with default roles (admin, reader, writer)
    pub async fn with_default_roles() -> Self {
        let manager = Self::new();

        // Admin role (all permissions)
        manager
            .add_role(Role::new("admin").with_permission("*"))
            .await;

        // Reader role
        manager
            .add_role(Role::new("reader").with_permission("read"))
            .await;

        // Writer role
        manager
            .add_role(
                Role::new("writer")
                    .with_permission("read")
                    .with_permission("write"),
            )
            .await;

        manager
    }

    /// Add a role
    pub async fn add_role(&self, role: Role) {
        let mut roles = self.roles.write().await;
        roles.insert(role.name.clone(), role);
    }

    /// Get a role
    pub async fn get_role(&self, name: &str) -> Option<Role> {
        let roles = self.roles.read().await;
        roles.get(name).cloned()
    }

    /// Assign a role to a user
    pub async fn assign_role(&self, user_id: &str, role_name: &str) -> Result<(), String> {
        // Verify role exists
        {
            let roles = self.roles.read().await;
            if !roles.contains_key(role_name) {
                return Err(format!("Role '{}' does not exist", role_name));
            }
        }

        // Assign role
        let mut user_roles = self.user_roles.write().await;
        let roles = user_roles
            .entry(user_id.to_string())
            .or_insert_with(Vec::new);

        if !roles.contains(&role_name.to_string()) {
            roles.push(role_name.to_string());
        }

        Ok(())
    }

    /// Remove a role from a user
    pub async fn remove_role(&self, user_id: &str, role_name: &str) {
        let mut user_roles = self.user_roles.write().await;
        if let Some(roles) = user_roles.get_mut(user_id) {
            roles.retain(|r| r != role_name);
        }
    }

    /// Get roles for a user
    pub async fn get_user_roles(&self, user_id: &str) -> Vec<String> {
        let user_roles = self.user_roles.read().await;
        user_roles.get(user_id).cloned().unwrap_or_default()
    }

    /// Check if user has a permission (via roles)
    pub async fn has_permission(&self, user_id: &str, permission: &str) -> bool {
        let user_roles = self.get_user_roles(user_id).await;
        let roles = self.roles.read().await;

        for role_name in user_roles {
            if let Some(role) = roles.get(&role_name) {
                if role.has_permission(permission) {
                    return true;
                }
            }
        }

        false
    }

    /// Get all permissions for a user (from all their roles)
    pub async fn get_user_permissions(&self, user_id: &str) -> Vec<String> {
        let user_roles = self.get_user_roles(user_id).await;
        let roles = self.roles.read().await;
        let mut permissions = Vec::new();

        for role_name in user_roles {
            if let Some(role) = roles.get(&role_name) {
                permissions.extend(role.permissions.clone());
            }
        }

        // Remove duplicates
        permissions.sort();
        permissions.dedup();
        permissions
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_role_creation() {
        let role = Role::new("admin")
            .with_permission("read")
            .with_permission("write")
            .with_permission("admin");

        assert_eq!(role.name, "admin");
        assert!(role.has_permission("read"));
        assert!(role.has_permission("write"));
        assert!(role.has_permission("admin"));
        assert!(!role.has_permission("delete"));
    }

    #[tokio::test]
    async fn test_wildcard_permission() {
        let role = Role::new("admin").with_permission("*");
        assert!(role.has_permission("read"));
        assert!(role.has_permission("write"));
        assert!(role.has_permission("admin"));
        assert!(role.has_permission("anything"));
    }

    #[tokio::test]
    async fn test_rbac_manager() {
        let manager = RbacManager::with_default_roles().await;

        // Assign role to user
        manager.assign_role("user1", "reader").await.unwrap();
        manager.assign_role("user1", "writer").await.unwrap();

        // Check permissions
        assert!(manager.has_permission("user1", "read").await);
        assert!(manager.has_permission("user1", "write").await);
        assert!(!manager.has_permission("user1", "admin").await);

        // Admin user
        manager.assign_role("admin1", "admin").await.unwrap();
        assert!(manager.has_permission("admin1", "read").await);
        assert!(manager.has_permission("admin1", "write").await);
        assert!(manager.has_permission("admin1", "admin").await);
        assert!(manager.has_permission("admin1", "anything").await);
    }

    #[tokio::test]
    async fn test_get_user_permissions() {
        let manager = RbacManager::with_default_roles().await;
        manager.assign_role("user1", "reader").await.unwrap();
        manager.assign_role("user1", "writer").await.unwrap();

        let permissions = manager.get_user_permissions("user1").await;
        assert!(permissions.contains(&"read".to_string()));
        assert!(permissions.contains(&"write".to_string()));
    }
}
