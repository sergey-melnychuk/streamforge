//! Event model for stream processing
//!
//! Events are the fundamental unit of data in StreamForge.
//! They are immutable and contain:
//! - A key for partitioning
//! - A value payload
//! - A timestamp for event-time processing

use bytes::Bytes;
use std::fmt;
use std::hash::{Hash, Hasher};
use std::sync::Arc;

/// Timestamp in milliseconds since Unix epoch
pub type Timestamp = i64;

/// Event key used for partitioning and joins
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[derive(Default)]
pub enum EventKey {
    /// No key (null key)
    #[default]
    None,
    /// String key
    String(Arc<str>),
    /// Integer key
    Int(i64),
    /// Binary key
    Bytes(Bytes),
}

impl EventKey {
    pub fn from_str(s: impl Into<String>) -> Self {
        EventKey::String(s.into().into())
    }

    pub fn from_int(i: i64) -> Self {
        EventKey::Int(i)
    }

    pub fn from_bytes(b: impl Into<Bytes>) -> Self {
        EventKey::Bytes(b.into())
    }

    pub fn is_none(&self) -> bool {
        matches!(self, EventKey::None)
    }
}


impl fmt::Display for EventKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            EventKey::None => write!(f, "null"),
            EventKey::String(s) => write!(f, "{}", s),
            EventKey::Int(i) => write!(f, "{}", i),
            EventKey::Bytes(b) => write!(f, "bytes[{}]", b.len()),
        }
    }
}

/// Event value payload
#[derive(Debug, Clone, PartialEq)]
#[derive(Default)]
pub enum EventValue {
    /// Null value
    #[default]
    Null,
    /// Boolean value
    Bool(bool),
    /// Integer value
    Int(i64),
    /// Float value
    Float(f64),
    /// String value
    String(Arc<str>),
    /// Binary value
    Bytes(Bytes),
    /// JSON-like structured data
    Json(serde_json::Value),
}

impl EventValue {
    pub fn from_str(s: impl Into<String>) -> Self {
        EventValue::String(s.into().into())
    }

    pub fn from_int(i: i64) -> Self {
        EventValue::Int(i)
    }

    pub fn from_float(f: f64) -> Self {
        EventValue::Float(f)
    }

    pub fn from_bool(b: bool) -> Self {
        EventValue::Bool(b)
    }

    pub fn from_bytes(b: impl Into<Bytes>) -> Self {
        EventValue::Bytes(b.into())
    }

    pub fn is_null(&self) -> bool {
        matches!(self, EventValue::Null)
    }

    /// Try to convert to i64
    pub fn as_int(&self) -> Option<i64> {
        match self {
            EventValue::Int(i) => Some(*i),
            EventValue::Float(f) => Some(*f as i64),
            EventValue::Bool(b) => Some(if *b { 1 } else { 0 }),
            _ => None,
        }
    }

    /// Try to convert to f64
    pub fn as_float(&self) -> Option<f64> {
        match self {
            EventValue::Float(f) => Some(*f),
            EventValue::Int(i) => Some(*i as f64),
            _ => None,
        }
    }

    /// Try to convert to string
    pub fn as_str(&self) -> Option<&str> {
        match self {
            EventValue::String(s) => Some(s),
            _ => None,
        }
    }
}


impl fmt::Display for EventValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            EventValue::Null => write!(f, "null"),
            EventValue::Bool(b) => write!(f, "{}", b),
            EventValue::Int(i) => write!(f, "{}", i),
            EventValue::Float(fl) => write!(f, "{}", fl),
            EventValue::String(s) => write!(f, "{}", s),
            EventValue::Bytes(b) => write!(f, "bytes[{}]", b.len()),
            EventValue::Json(j) => write!(f, "{}", j),
        }
    }
}

impl From<i64> for EventValue {
    fn from(i: i64) -> Self {
        EventValue::Int(i)
    }
}

impl From<f64> for EventValue {
    fn from(f: f64) -> Self {
        EventValue::Float(f)
    }
}

impl From<bool> for EventValue {
    fn from(b: bool) -> Self {
        EventValue::Bool(b)
    }
}

impl From<String> for EventValue {
    fn from(s: String) -> Self {
        EventValue::String(s.into())
    }
}

impl From<&str> for EventValue {
    fn from(s: &str) -> Self {
        EventValue::String(s.into())
    }
}

/// An immutable event in the stream
///
/// Events are the fundamental unit of processing. They contain:
/// - A key for partitioning and grouping
/// - A value payload
/// - A timestamp for event-time processing
/// - Optional headers for metadata
#[derive(Debug, Clone)]
pub struct Event {
    /// Event key for partitioning
    pub key: EventKey,
    /// Event value payload
    pub value: EventValue,
    /// Event timestamp (milliseconds since Unix epoch)
    pub timestamp: Timestamp,
    /// Optional headers for metadata
    pub headers: Vec<(String, String)>,
}

impl Event {
    /// Create a new event with the given key, value, and timestamp
    pub fn new(key: EventKey, value: EventValue, timestamp: Timestamp) -> Self {
        Self {
            key,
            value,
            timestamp,
            headers: Vec::new(),
        }
    }

    /// Create a new event with the current timestamp
    pub fn now(key: EventKey, value: EventValue) -> Self {
        Self::new(key, value, current_timestamp())
    }

    /// Create an event with no key
    pub fn with_value(value: EventValue) -> Self {
        Self::now(EventKey::None, value)
    }

    /// Add a header to the event
    pub fn with_header(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.headers.push((key.into(), value.into()));
        self
    }

    /// Get a header value by key
    pub fn get_header(&self, key: &str) -> Option<&str> {
        self.headers
            .iter()
            .find(|(k, _)| k == key)
            .map(|(_, v)| v.as_str())
    }

    /// Create a new event with a different key
    pub fn with_key(mut self, key: EventKey) -> Self {
        self.key = key;
        self
    }

    /// Create a new event with a different value
    pub fn with_value_changed(mut self, value: EventValue) -> Self {
        self.value = value;
        self
    }

    /// Create a new event with a different timestamp
    pub fn with_timestamp(mut self, timestamp: Timestamp) -> Self {
        self.timestamp = timestamp;
        self
    }
}

impl PartialEq for Event {
    fn eq(&self, other: &Self) -> bool {
        self.key == other.key && self.value == other.value && self.timestamp == other.timestamp
    }
}

impl Eq for Event {}

impl Hash for Event {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.key.hash(state);
        self.timestamp.hash(state);
    }
}

impl fmt::Display for Event {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "Event{{key: {}, value: {}, ts: {}}}",
            self.key, self.value, self.timestamp
        )
    }
}

/// Get the current timestamp in milliseconds since Unix epoch
pub fn current_timestamp() -> Timestamp {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("Time went backwards")
        .as_millis() as i64
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_event_creation() {
        let event = Event::new(
            EventKey::from_str("key1"),
            EventValue::from_int(42),
            1000,
        );

        assert_eq!(event.key, EventKey::String("key1".into()));
        assert_eq!(event.value, EventValue::Int(42));
        assert_eq!(event.timestamp, 1000);
    }

    #[test]
    fn test_event_with_headers() {
        let event = Event::now(EventKey::None, EventValue::from_int(100))
            .with_header("source", "sensor-1")
            .with_header("region", "us-west");

        assert_eq!(event.get_header("source"), Some("sensor-1"));
        assert_eq!(event.get_header("region"), Some("us-west"));
        assert_eq!(event.get_header("missing"), None);
    }

    #[test]
    fn test_event_value_conversions() {
        let int_val = EventValue::from_int(42);
        assert_eq!(int_val.as_int(), Some(42));
        assert_eq!(int_val.as_float(), Some(42.0));

        let float_val = EventValue::from_float(3.14);
        assert_eq!(float_val.as_float(), Some(3.14));

        let str_val = EventValue::from_str("hello");
        assert_eq!(str_val.as_str(), Some("hello"));
    }

    #[test]
    fn test_event_key_types() {
        let str_key = EventKey::from_str("test");
        assert!(!str_key.is_none());

        let int_key = EventKey::from_int(123);
        assert!(!int_key.is_none());

        let none_key = EventKey::None;
        assert!(none_key.is_none());
    }
}
