//! Backpressure detection and flow control
//!
//! Implements backpressure mechanisms to prevent system overload
//! when downstream operators cannot keep up with the input rate.

use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{mpsc, RwLock, Semaphore};
use tracing::{debug, warn};

/// Backpressure signal indicating system state
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BackpressureLevel {
    /// No backpressure - system is healthy
    None,
    /// Mild backpressure - slight slowdown
    Mild,
    /// Moderate backpressure - significant slowdown
    Moderate,
    /// Severe backpressure - system is overwhelmed
    Severe,
}

impl BackpressureLevel {
    /// Get the numeric level (0 = none, 3 = severe)
    pub fn as_u8(&self) -> u8 {
        match self {
            BackpressureLevel::None => 0,
            BackpressureLevel::Mild => 1,
            BackpressureLevel::Moderate => 2,
            BackpressureLevel::Severe => 3,
        }
    }

    /// Create from numeric level
    pub fn from_u8(level: u8) -> Self {
        match level {
            0 => BackpressureLevel::None,
            1 => BackpressureLevel::Mild,
            2 => BackpressureLevel::Moderate,
            _ => BackpressureLevel::Severe,
        }
    }
}

/// Backpressure detector that monitors queue sizes and processing rates
pub struct BackpressureDetector {
    /// Maximum queue size before backpressure kicks in
    max_queue_size: usize,
    /// Warning threshold (mild backpressure)
    warning_threshold: usize,
    /// Critical threshold (severe backpressure)
    critical_threshold: usize,
    /// Current queue size
    current_size: Arc<RwLock<usize>>,
    /// Processing rate tracker
    rate_tracker: Arc<RwLock<RateTracker>>,
}

impl BackpressureDetector {
    /// Create a new backpressure detector
    pub fn new(max_queue_size: usize) -> Self {
        let warning_threshold = (max_queue_size as f64 * 0.5) as usize;
        let critical_threshold = (max_queue_size as f64 * 0.8) as usize;
        
        Self {
            max_queue_size,
            warning_threshold,
            critical_threshold,
            current_size: Arc::new(RwLock::new(0)),
            rate_tracker: Arc::new(RwLock::new(RateTracker::new())),
        }
    }

    /// Update the current queue size
    pub async fn update_size(&self, size: usize) {
        *self.current_size.write().await = size;
    }

    /// Get the current backpressure level
    pub async fn get_level(&self) -> BackpressureLevel {
        let size = *self.current_size.read().await;
        
        if size >= self.critical_threshold {
            BackpressureLevel::Severe
        } else if size >= self.warning_threshold {
            BackpressureLevel::Moderate
        } else if size > self.max_queue_size / 4 {
            BackpressureLevel::Mild
        } else {
            BackpressureLevel::None
        }
    }

    /// Record a processed event for rate tracking
    pub async fn record_processed(&self) {
        self.rate_tracker.write().await.record_event();
    }

    /// Get the current processing rate (events per second)
    pub async fn get_processing_rate(&self) -> f64 {
        self.rate_tracker.read().await.get_rate()
    }

    /// Check if we should throttle based on processing rate
    pub async fn should_throttle(&self, input_rate: f64) -> bool {
        let processing_rate = self.get_processing_rate().await;
        let queue_size = *self.current_size.read().await;
        
        // Throttle if input rate significantly exceeds processing rate
        // or if queue is getting too large
        input_rate > processing_rate * 1.5 || queue_size > self.warning_threshold
    }
}

/// Tracks processing rate over time
struct RateTracker {
    /// Timestamps of recent events
    events: Vec<Instant>,
    /// Window size for rate calculation (seconds)
    window_secs: f64,
}

impl RateTracker {
    fn new() -> Self {
        Self {
            events: Vec::new(),
            window_secs: 1.0, // 1 second window
        }
    }

    fn record_event(&mut self) {
        let now = Instant::now();
        self.events.push(now);
        
        // Remove events outside the window
        let cutoff = now - Duration::from_secs_f64(self.window_secs);
        self.events.retain(|&t| t > cutoff);
    }

    fn get_rate(&self) -> f64 {
        if self.events.is_empty() {
            return 0.0;
        }

        let now = Instant::now();
        let cutoff = now - Duration::from_secs_f64(self.window_secs);
        let recent_events: usize = self.events.iter().filter(|&&t| t > cutoff).count();
        
        recent_events as f64 / self.window_secs
    }
}

/// Flow controller that manages event flow based on backpressure
pub struct FlowController {
    /// Backpressure detector
    detector: Arc<BackpressureDetector>,
    /// Semaphore for controlling concurrent processing
    semaphore: Arc<Semaphore>,
    /// Initial permit count
    initial_permits: usize,
}

impl FlowController {
    /// Create a new flow controller
    pub fn new(max_concurrent: usize, max_queue_size: usize) -> Self {
        Self {
            detector: Arc::new(BackpressureDetector::new(max_queue_size)),
            semaphore: Arc::new(Semaphore::new(max_concurrent)),
            initial_permits: max_concurrent,
        }
    }

    /// Get the backpressure detector
    pub fn detector(&self) -> Arc<BackpressureDetector> {
        Arc::clone(&self.detector)
    }

    /// Acquire a permit to process an event
    pub async fn acquire_permit(&self) -> tokio::sync::SemaphorePermit<'_> {
        self.semaphore.acquire().await.unwrap()
    }

    /// Adjust permits based on backpressure level
    pub async fn adjust_permits(&self) {
        let level = self.detector.get_level().await;
        
        // Reduce permits under backpressure
        let target_permits = match level {
            BackpressureLevel::None => self.initial_permits,
            BackpressureLevel::Mild => (self.initial_permits as f64 * 0.75) as usize,
            BackpressureLevel::Moderate => (self.initial_permits as f64 * 0.5) as usize,
            BackpressureLevel::Severe => (self.initial_permits as f64 * 0.25) as usize,
        };

        let current_permits = self.semaphore.available_permits();
        
        if target_permits > current_permits {
            // Add permits
            let to_add = target_permits - current_permits;
            self.semaphore.add_permits(to_add);
        } else if target_permits < current_permits {
            // Remove permits (by acquiring and not releasing)
            let to_remove = current_permits - target_permits;
            for _ in 0..to_remove {
                let _permit = self.semaphore.acquire().await.unwrap();
                // Permit is dropped here, effectively reducing available permits
            }
        }
    }
}

/// Backpressure-aware channel that monitors queue size
/// 
/// This is a simplified implementation. In production, you'd want
/// a more sophisticated approach that properly tracks queue size.
pub struct BackpressureChannel<T> {
    /// Underlying channel sender
    sender: mpsc::Sender<T>,
    /// Backpressure detector
    detector: Arc<BackpressureDetector>,
    /// Current queue size (approximate, tracked manually)
    queue_size: Arc<RwLock<usize>>,
}

impl<T: Send + 'static> BackpressureChannel<T> {
    /// Create a new backpressure-aware channel
    /// 
    /// Note: This is a simplified implementation. Queue size tracking
    /// is approximate and based on manual tracking.
    pub fn new(capacity: usize) -> (Self, mpsc::Receiver<T>) {
        let (sender, receiver) = mpsc::channel(capacity);
        let detector = Arc::new(BackpressureDetector::new(capacity));
        let queue_size = Arc::new(RwLock::new(0));
        
        let channel = Self {
            sender,
            detector: Arc::clone(&detector),
            queue_size,
        };
        
        (channel, receiver)
    }

    /// Send an item, checking for backpressure
    pub async fn send(&self, item: T) -> Result<(), mpsc::error::SendError<T>> {
        // Check backpressure level before sending
        let level = self.detector.get_level().await;
        if level == BackpressureLevel::Severe {
            warn!("Severe backpressure detected, may drop event");
        }
        
        // Try to send
        match self.sender.try_send(item) {
            Ok(()) => {
                // Update queue size
                let mut size = self.queue_size.write().await;
                *size += 1;
                self.detector.update_size(*size).await;
                Ok(())
            }
            Err(mpsc::error::TrySendError::Full(item)) => {
                // Channel is full, use async send which will block
                let result = self.sender.send(item).await;
                if result.is_ok() {
                    let mut size = self.queue_size.write().await;
                    *size += 1;
                    self.detector.update_size(*size).await;
                }
                result
            }
            Err(mpsc::error::TrySendError::Closed(item)) => {
                // Channel is closed
                Err(mpsc::error::SendError(item))
            }
        }
    }

    /// Try to send without blocking
    pub fn try_send(&self, item: T) -> Result<(), mpsc::error::TrySendError<T>> {
        match self.sender.try_send(item) {
            Ok(()) => {
                // Update queue size asynchronously
                let size = Arc::clone(&self.queue_size);
                let detector = Arc::clone(&self.detector);
                tokio::spawn(async move {
                    let mut s = size.write().await;
                    *s += 1;
                    detector.update_size(*s).await;
                });
                Ok(())
            }
            Err(e) => Err(e),
        }
    }
    
    /// Notify that an item was consumed (call this when receiving)
    pub async fn notify_consumed(&self) {
        let mut size = self.queue_size.write().await;
        if *size > 0 {
            *size -= 1;
        }
        self.detector.update_size(*size).await;
    }

    /// Get the backpressure detector
    pub fn detector(&self) -> Arc<BackpressureDetector> {
        Arc::clone(&self.detector)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_backpressure_detector() {
        let detector = BackpressureDetector::new(100);
        
        // Initially no backpressure
        assert_eq!(detector.get_level().await, BackpressureLevel::None);
        
        // Update to mild backpressure
        detector.update_size(30).await;
        assert_eq!(detector.get_level().await, BackpressureLevel::Mild);
        
        // Update to moderate backpressure
        detector.update_size(60).await;
        assert_eq!(detector.get_level().await, BackpressureLevel::Moderate);
        
        // Update to severe backpressure
        detector.update_size(90).await;
        assert_eq!(detector.get_level().await, BackpressureLevel::Severe);
    }

    #[tokio::test]
    async fn test_rate_tracker() {
        let detector = BackpressureDetector::new(100);
        
        // Record some events
        for _ in 0..10 {
            detector.record_processed().await;
        }
        
        // Should have some rate
        let rate = detector.get_processing_rate().await;
        assert!(rate > 0.0);
    }

    #[tokio::test]
    async fn test_flow_controller() {
        let controller = FlowController::new(10, 100);
        
        // Should be able to acquire permits
        let _permit1 = controller.acquire_permit().await;
        let _permit2 = controller.acquire_permit().await;
        
        // Available permits should be reduced
        assert!(controller.semaphore.available_permits() < 10);
    }
}

