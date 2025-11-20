//! Time abstractions for stream processing

use crate::core::Timestamp;

/// Time characteristic determines how time is interpreted in stream processing
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TimeCharacteristic {
    /// Use event timestamps for time-based operations
    EventTime,
    /// Use processing time (wall clock) for time-based operations
    ProcessingTime,
}

/// Event time is the time embedded in the event data
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct EventTime(pub Timestamp);

impl EventTime {
    pub fn new(timestamp: Timestamp) -> Self {
        Self(timestamp)
    }

    pub fn as_millis(&self) -> Timestamp {
        self.0
    }
}

/// Processing time is the wall clock time when the event is processed
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ProcessingTime(pub Timestamp);

impl ProcessingTime {
    pub fn now() -> Self {
        Self(crate::core::event::current_timestamp())
    }

    pub fn as_millis(&self) -> Timestamp {
        self.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_event_time() {
        let et = EventTime::new(1000);
        assert_eq!(et.as_millis(), 1000);
    }

    #[test]
    fn test_processing_time() {
        let pt1 = ProcessingTime::now();
        let pt2 = ProcessingTime::now();
        assert!(pt2.as_millis() >= pt1.as_millis());
    }
}
