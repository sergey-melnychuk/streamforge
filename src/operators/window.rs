//! Window operators for time-based aggregations
//!
//! Windows divide a stream into finite chunks for aggregation.
//! This module provides different window types and window assigners.

use crate::core::{Event, Timestamp};
use std::time::Duration;

/// Window types for stream processing
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WindowType {
    /// Fixed-size, non-overlapping windows
    Tumbling,
    /// Fixed-size, overlapping windows
    Sliding,
    /// Dynamic windows based on event patterns
    Session,
}

/// A window represents a time range for aggregation
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Window {
    /// Start time of the window (inclusive)
    pub start: Timestamp,
    /// End time of the window (exclusive)
    pub end: Timestamp,
}

impl Window {
    pub fn new(start: Timestamp, end: Timestamp) -> Self {
        Self { start, end }
    }

    /// Check if a timestamp falls within this window
    pub fn contains(&self, timestamp: Timestamp) -> bool {
        timestamp >= self.start && timestamp < self.end
    }

    /// Get the window duration in milliseconds
    pub fn duration(&self) -> i64 {
        self.end - self.start
    }

    /// Check if this window is before another window
    pub fn is_before(&self, other: &Window) -> bool {
        self.end <= other.start
    }

    /// Check if two windows overlap
    pub fn overlaps(&self, other: &Window) -> bool {
        self.start < other.end && other.start < self.end
    }

    /// Merge two overlapping windows
    pub fn merge(&self, other: &Window) -> Option<Window> {
        if self.overlaps(other) {
            Some(Window::new(
                self.start.min(other.start),
                self.end.max(other.end),
            ))
        } else {
            None
        }
    }
}

/// Window assigner determines which window(s) an event belongs to
pub trait WindowAssigner: Send + Sync {
    /// Assign an event to one or more windows
    fn assign_windows(&self, event: &Event) -> Vec<Window>;

    /// Get the window type
    fn window_type(&self) -> WindowType;
}

/// Tumbling window assigner
///
/// Creates fixed-size, non-overlapping windows.
/// Example: 5-minute windows starting at the hour
#[derive(Debug, Clone)]
pub struct TumblingWindow {
    size: Duration,
    offset: Duration,
}

impl TumblingWindow {
    /// Create a tumbling window with the given size
    pub fn of(size: Duration) -> Self {
        Self {
            size,
            offset: Duration::from_millis(0),
        }
    }

    /// Create a tumbling window with size and offset
    pub fn with_offset(size: Duration, offset: Duration) -> Self {
        Self { size, offset }
    }

    fn get_window_start(&self, timestamp: Timestamp) -> Timestamp {
        let size_ms = self.size.as_millis() as i64;
        let offset_ms = self.offset.as_millis() as i64;

        // Adjust for offset
        let adjusted = timestamp - offset_ms;

        // Floor to window boundary
        

        (adjusted / size_ms) * size_ms + offset_ms
    }
}

impl WindowAssigner for TumblingWindow {
    fn assign_windows(&self, event: &Event) -> Vec<Window> {
        let start = self.get_window_start(event.timestamp);
        let end = start + self.size.as_millis() as i64;
        vec![Window::new(start, end)]
    }

    fn window_type(&self) -> WindowType {
        WindowType::Tumbling
    }
}

/// Sliding window assigner
///
/// Creates fixed-size, overlapping windows.
/// Example: 10-minute windows sliding every 5 minutes
#[derive(Debug, Clone)]
pub struct SlidingWindow {
    size: Duration,
    slide: Duration,
    offset: Duration,
}

impl SlidingWindow {
    /// Create a sliding window with size and slide interval
    pub fn of(size: Duration, slide: Duration) -> Self {
        Self {
            size,
            slide,
            offset: Duration::from_millis(0),
        }
    }

    /// Create a sliding window with size, slide, and offset
    pub fn with_offset(size: Duration, slide: Duration, offset: Duration) -> Self {
        Self { size, slide, offset }
    }
}

impl WindowAssigner for SlidingWindow {
    fn assign_windows(&self, event: &Event) -> Vec<Window> {
        let size_ms = self.size.as_millis() as i64;
        let slide_ms = self.slide.as_millis() as i64;
        let offset_ms = self.offset.as_millis() as i64;

        let timestamp = event.timestamp;

        // Calculate the first window that could contain this timestamp
        // This is the window that starts at or before (timestamp - size + 1)
        let first_window_start = ((timestamp - offset_ms - size_ms + 1) / slide_ms) * slide_ms + offset_ms;

        // Generate all windows that contain this timestamp
        let mut windows = Vec::new();
        let mut window_start = first_window_start;

        // Keep adding windows while they can still contain the timestamp
        loop {
            let window_end = window_start + size_ms;
            if timestamp >= window_start && timestamp < window_end {
                windows.push(Window::new(window_start, window_end));
            }

            // Move to next window
            window_start += slide_ms;

            // Stop if next window starts after the timestamp
            if window_start > timestamp {
                break;
            }
        }

        windows
    }

    fn window_type(&self) -> WindowType {
        WindowType::Sliding
    }
}

/// Session window assigner
///
/// Creates dynamic windows based on inactivity gaps.
/// A session ends when no events arrive for the gap duration.
#[derive(Debug, Clone)]
pub struct SessionWindow {
    gap: Duration,
}

impl SessionWindow {
    /// Create a session window with the given inactivity gap
    pub fn with_gap(gap: Duration) -> Self {
        Self { gap }
    }
}

impl WindowAssigner for SessionWindow {
    fn assign_windows(&self, event: &Event) -> Vec<Window> {
        let gap_ms = self.gap.as_millis() as i64;
        // Initial window: event timestamp to timestamp + gap
        vec![Window::new(event.timestamp, event.timestamp + gap_ms)]
    }

    fn window_type(&self) -> WindowType {
        WindowType::Session
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::{EventKey, EventValue};

    #[test]
    fn test_window_contains() {
        let window = Window::new(1000, 2000);
        assert!(window.contains(1000));
        assert!(window.contains(1500));
        assert!(!window.contains(2000));
        assert!(!window.contains(999));
    }

    #[test]
    fn test_window_overlaps() {
        let w1 = Window::new(1000, 2000);
        let w2 = Window::new(1500, 2500);
        let w3 = Window::new(2000, 3000);

        assert!(w1.overlaps(&w2));
        assert!(w2.overlaps(&w1));
        assert!(!w1.overlaps(&w3));
        assert!(!w3.overlaps(&w1));
    }

    #[test]
    fn test_window_merge() {
        let w1 = Window::new(1000, 2000);
        let w2 = Window::new(1500, 2500);

        let merged = w1.merge(&w2).unwrap();
        assert_eq!(merged.start, 1000);
        assert_eq!(merged.end, 2500);
    }

    #[test]
    fn test_tumbling_window() {
        let assigner = TumblingWindow::of(Duration::from_secs(60));
        let event = Event::new(
            EventKey::None,
            EventValue::from_int(1),
            1500, // 1.5 seconds
        );

        let windows = assigner.assign_windows(&event);
        assert_eq!(windows.len(), 1);
        assert_eq!(windows[0].start, 0);
        assert_eq!(windows[0].end, 60000);
    }

    #[test]
    fn test_tumbling_window_with_offset() {
        let assigner = TumblingWindow::with_offset(
            Duration::from_secs(60),
            Duration::from_secs(15),
        );

        let event = Event::new(
            EventKey::None,
            EventValue::from_int(1),
            20000, // 20 seconds
        );

        let windows = assigner.assign_windows(&event);
        assert_eq!(windows.len(), 1);
        assert_eq!(windows[0].start, 15000); // Offset by 15 seconds
        assert_eq!(windows[0].end, 75000);
    }

    #[test]
    fn test_sliding_window() {
        let assigner = SlidingWindow::of(
            Duration::from_secs(60), // 60s windows
            Duration::from_secs(30), // sliding every 30s
        );

        let event = Event::new(
            EventKey::None,
            EventValue::from_int(1),
            45000, // 45 seconds
        );

        let windows = assigner.assign_windows(&event);
        assert_eq!(windows.len(), 2);

        // Should be in windows [0, 60000) and [30000, 90000)
        assert_eq!(windows[0].start, 0);
        assert_eq!(windows[0].end, 60000);
        assert_eq!(windows[1].start, 30000);
        assert_eq!(windows[1].end, 90000);
    }

    #[test]
    fn test_session_window() {
        let assigner = SessionWindow::with_gap(Duration::from_secs(30));

        let event = Event::new(
            EventKey::None,
            EventValue::from_int(1),
            10000,
        );

        let windows = assigner.assign_windows(&event);
        assert_eq!(windows.len(), 1);
        assert_eq!(windows[0].start, 10000);
        assert_eq!(windows[0].end, 40000); // 10s + 30s gap
    }

    #[test]
    fn test_window_duration() {
        let window = Window::new(1000, 5000);
        assert_eq!(window.duration(), 4000);
    }

    #[test]
    fn test_window_is_before() {
        let w1 = Window::new(1000, 2000);
        let w2 = Window::new(2000, 3000);
        let w3 = Window::new(1500, 2500);

        assert!(w1.is_before(&w2));
        assert!(!w2.is_before(&w1));
        assert!(!w1.is_before(&w3));
    }
}
