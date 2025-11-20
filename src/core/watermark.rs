//! Watermark handling for event-time processing
//!
//! Watermarks are special markers that flow through the stream indicating
//! that no more events with timestamps less than the watermark will arrive.

use crate::core::Timestamp;
use std::fmt;

/// A watermark indicates progress in event time
///
/// Watermarks are used to trigger time-based operations like
/// window computations. A watermark with timestamp T means that
/// all events with timestamp < T have been seen.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Watermark {
    timestamp: Timestamp,
}

impl Watermark {
    /// Create a new watermark with the given timestamp
    pub fn new(timestamp: Timestamp) -> Self {
        Self { timestamp }
    }

    /// Get the watermark timestamp
    pub fn timestamp(&self) -> Timestamp {
        self.timestamp
    }

    /// Check if this watermark indicates that all events up to the given time have arrived
    pub fn passed(&self, event_time: Timestamp) -> bool {
        self.timestamp >= event_time
    }

    /// Advance the watermark to a later time
    pub fn advance(&mut self, new_timestamp: Timestamp) {
        if new_timestamp > self.timestamp {
            self.timestamp = new_timestamp;
        }
    }

    /// Create a watermark that represents the beginning of time
    pub fn min() -> Self {
        Self {
            timestamp: Timestamp::MIN,
        }
    }

    /// Create a watermark that represents the end of time
    pub fn max() -> Self {
        Self {
            timestamp: Timestamp::MAX,
        }
    }
}

impl fmt::Display for Watermark {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Watermark({})", self.timestamp)
    }
}

impl Default for Watermark {
    fn default() -> Self {
        Self::min()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_watermark_creation() {
        let wm = Watermark::new(1000);
        assert_eq!(wm.timestamp(), 1000);
    }

    #[test]
    fn test_watermark_passed() {
        let wm = Watermark::new(1000);
        assert!(wm.passed(999));
        assert!(wm.passed(1000));
        assert!(!wm.passed(1001));
    }

    #[test]
    fn test_watermark_advance() {
        let mut wm = Watermark::new(1000);
        wm.advance(1500);
        assert_eq!(wm.timestamp(), 1500);

        // Should not go backwards
        wm.advance(1200);
        assert_eq!(wm.timestamp(), 1500);
    }

    #[test]
    fn test_watermark_min_max() {
        let min = Watermark::min();
        let max = Watermark::max();
        assert!(min < max);
        assert!(!min.passed(0)); // MIN watermark hasn't passed any positive timestamp
        assert!(max.passed(i64::MAX - 1)); // MAX watermark has passed everything
    }
}
