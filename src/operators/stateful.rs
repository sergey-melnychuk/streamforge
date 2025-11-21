//! Stateful operators that can use state backends

use crate::core::Event;
use crate::error::Result;
use crate::operators::StreamOperator;
use crate::state::{StateBackend, StateResult};
use std::sync::Arc;

/// Trait for operators that maintain state
pub trait StatefulOperator: StreamOperator {
    /// Get the state backend used by this operator
    fn state_backend(&self) -> Option<Arc<dyn StateBackend>>;

    /// Set the state backend for this operator
    fn set_state_backend(&mut self, backend: Arc<dyn StateBackend>);
}

/// State access helper for operators
pub struct OperatorState {
    backend: Arc<dyn StateBackend>,
    namespace: String,
}

impl OperatorState {
    /// Create a new operator state with a namespace
    pub fn new(backend: Arc<dyn StateBackend>, namespace: impl Into<String>) -> Self {
        Self {
            backend,
            namespace: namespace.into(),
        }
    }

    /// Get a state value by key
    pub async fn get(&self, key: &[u8]) -> StateResult<Option<Vec<u8>>> {
        let full_key = self.make_key(key);
        let result = self.backend.get(&full_key).await?;
        Ok(result.map(|b| b.to_vec()))
    }

    /// Set a state value by key
    pub async fn set(&self, key: &[u8], value: Vec<u8>) -> StateResult<()> {
        let full_key = self.make_key(key);
        self.backend.put(&full_key, value.into()).await
    }

    /// Delete a state value by key
    pub async fn delete(&self, key: &[u8]) -> StateResult<()> {
        let full_key = self.make_key(key);
        self.backend.delete(&full_key).await
    }

    /// List all keys in this operator's namespace
    pub async fn list_keys(&self) -> StateResult<Vec<Vec<u8>>> {
        let prefix = format!("{}:", self.namespace);
        let all_keys = self.backend.list_keys(prefix.as_bytes()).await?;

        Ok(all_keys
            .into_iter()
            .filter_map(|k| {
                let k_str = String::from_utf8_lossy(&k);
                if k_str.starts_with(&prefix) {
                    Some(k_str[prefix.len()..].as_bytes().to_vec())
                } else {
                    None
                }
            })
            .collect())
    }

    /// Make a namespaced key
    fn make_key(&self, key: &[u8]) -> Vec<u8> {
        let mut full_key = format!("{}:", self.namespace).into_bytes();
        full_key.extend_from_slice(key);
        full_key
    }
}

/// Example stateful operator: Count operator that maintains counts per key
pub struct CountOperator {
    state: Option<OperatorState>,
}

impl Default for CountOperator {
    fn default() -> Self {
        Self::new()
    }
}

impl CountOperator {
    /// Create a new count operator
    pub fn new() -> Self {
        Self { state: None }
    }

    /// Create with state backend
    pub fn with_state(backend: Arc<dyn StateBackend>) -> Self {
        Self {
            state: Some(OperatorState::new(backend, "count")),
        }
    }
}

impl StreamOperator for CountOperator {
    fn process(&mut self, event: Event) -> Result<Option<Event>> {
        // For now, just pass through
        // In a full implementation, this would increment counts in state
        Ok(Some(event))
    }
}

impl StatefulOperator for CountOperator {
    fn state_backend(&self) -> Option<Arc<dyn StateBackend>> {
        self.state.as_ref().map(|s| s.backend.clone())
    }

    fn set_state_backend(&mut self, backend: Arc<dyn StateBackend>) {
        self.state = Some(OperatorState::new(backend, "count"));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::state::MemoryStateBackend;

    #[tokio::test]
    async fn test_operator_state() {
        let backend = Arc::new(MemoryStateBackend::new());
        let state = OperatorState::new(backend, "test-operator");

        // Set a value
        state.set(b"key1", b"value1".to_vec()).await.unwrap();

        // Get the value
        let value = state.get(b"key1").await.unwrap();
        assert_eq!(value, Some(b"value1".to_vec()));

        // List keys
        let keys = state.list_keys().await.unwrap();
        assert_eq!(keys, vec![b"key1".to_vec()]);

        // Delete the value
        state.delete(b"key1").await.unwrap();
        let value = state.get(b"key1").await.unwrap();
        assert_eq!(value, None);
    }

    #[tokio::test]
    async fn test_count_operator() {
        let backend = Arc::new(MemoryStateBackend::new());
        let mut op = CountOperator::with_state(backend);

        let event = Event::new(
            crate::core::EventKey::from_str("test"),
            crate::core::EventValue::String("value".into()),
            1234567890,
        );

        let result = op.process(event).unwrap();
        assert!(result.is_some());
    }
}
