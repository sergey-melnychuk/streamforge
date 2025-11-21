//! Trace context for distributed tracing

use serde::{Deserialize, Serialize};
use std::fmt;

/// Trace ID (128-bit identifier for a trace)
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct TraceId {
    /// High 64 bits
    pub high: u64,
    /// Low 64 bits
    pub low: u64,
}

impl TraceId {
    /// Generate a new random trace ID
    pub fn new() -> Self {
        use std::time::{SystemTime, UNIX_EPOCH};
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos() as u64;
        let random = rand::random::<u64>();
        Self {
            high: now,
            low: random,
        }
    }

    /// Create from a string representation (hex)
    pub fn from_hex(hex: &str) -> Option<Self> {
        if hex.len() != 32 {
            return None;
        }
        let high = u64::from_str_radix(&hex[0..16], 16).ok()?;
        let low = u64::from_str_radix(&hex[16..32], 16).ok()?;
        Some(Self { high, low })
    }

    /// Convert to hex string
    pub fn to_hex(&self) -> String {
        format!("{:016x}{:016x}", self.high, self.low)
    }
}

impl Default for TraceId {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Display for TraceId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.to_hex())
    }
}

/// Span ID (64-bit identifier for a span)
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct SpanId(pub u64);

impl SpanId {
    /// Generate a new random span ID
    pub fn new() -> Self {
        Self(rand::random::<u64>())
    }

    /// Create from a string representation (hex)
    pub fn from_hex(hex: &str) -> Option<Self> {
        u64::from_str_radix(hex, 16).ok().map(Self)
    }

    /// Convert to hex string
    pub fn to_hex(&self) -> String {
        format!("{:016x}", self.0)
    }
}

impl Default for SpanId {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Display for SpanId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.to_hex())
    }
}

/// Trace context for propagation across nodes
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TraceContext {
    /// Trace ID (same for all spans in a trace)
    pub trace_id: TraceId,
    /// Current span ID
    pub span_id: SpanId,
    /// Parent span ID (if this is a child span)
    pub parent_span_id: Option<SpanId>,
    /// Trace flags (sampling, etc.)
    pub flags: u8,
    /// Additional trace attributes
    pub attributes: std::collections::HashMap<String, String>,
}

impl TraceContext {
    /// Create a new root trace context
    pub fn new() -> Self {
        Self {
            trace_id: TraceId::new(),
            span_id: SpanId::new(),
            parent_span_id: None,
            flags: 1, // Sampled
            attributes: std::collections::HashMap::new(),
        }
    }

    /// Create a child span context
    pub fn child(&self) -> Self {
        Self {
            trace_id: self.trace_id,
            span_id: SpanId::new(),
            parent_span_id: Some(self.span_id),
            flags: self.flags,
            attributes: self.attributes.clone(),
        }
    }

    /// Check if this trace is sampled
    pub fn is_sampled(&self) -> bool {
        self.flags & 1 != 0
    }

    /// Set a trace attribute
    pub fn set_attribute(&mut self, key: String, value: String) {
        self.attributes.insert(key, value);
    }

    /// Get a trace attribute
    pub fn get_attribute(&self, key: &str) -> Option<&String> {
        self.attributes.get(key)
    }
}

impl Default for TraceContext {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_trace_id_generation() {
        let id1 = TraceId::new();
        let id2 = TraceId::new();
        assert_ne!(id1, id2);
    }

    #[test]
    fn test_trace_id_hex() {
        let id = TraceId {
            high: 0x1234567890abcdef,
            low: 0xfedcba0987654321,
        };
        let hex = id.to_hex();
        assert_eq!(hex.len(), 32);
        let parsed = TraceId::from_hex(&hex).unwrap();
        assert_eq!(parsed, id);
    }

    #[test]
    fn test_span_id_generation() {
        let id1 = SpanId::new();
        let id2 = SpanId::new();
        assert_ne!(id1, id2);
    }

    #[test]
    fn test_span_id_hex() {
        let id = SpanId(0x1234567890abcdef);
        let hex = id.to_hex();
        assert_eq!(hex.len(), 16);
        let parsed = SpanId::from_hex(&hex).unwrap();
        assert_eq!(parsed, id);
    }

    #[test]
    fn test_trace_context_child() {
        let parent = TraceContext::new();
        let child = parent.child();

        assert_eq!(child.trace_id, parent.trace_id);
        assert_ne!(child.span_id, parent.span_id);
        assert_eq!(child.parent_span_id, Some(parent.span_id));
    }

    #[test]
    fn test_trace_context_attributes() {
        let mut ctx = TraceContext::new();
        ctx.set_attribute("service".to_string(), "streamforge".to_string());
        assert_eq!(
            ctx.get_attribute("service"),
            Some(&"streamforge".to_string())
        );
    }
}
