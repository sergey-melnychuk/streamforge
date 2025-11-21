//! Stream sources for reading data from external systems
//!
//! Sources are the entry points for data into StreamForge streams.

pub mod file;
pub mod http;
pub mod iterator;

pub use file::FileSource;
pub use http::{HttpSource, HttpSourceConfig};
pub use iterator::IteratorSource;

use crate::core::Event;
use async_trait::async_trait;
use std::pin::Pin;
use std::task::{Context, Poll};

/// Trait for stream sources
#[async_trait]
pub trait Source: Send + Sync {
    /// Read the next event from the source
    async fn read(&mut self) -> Option<Event>;

    /// Check if the source is exhausted
    fn is_exhausted(&self) -> bool;
}

/// A stream source that can be converted to a Stream
pub struct SourceStream {
    source: Box<dyn Source>,
}

impl SourceStream {
    pub fn new(source: Box<dyn Source>) -> Self {
        Self { source }
    }
}

impl futures::Stream for SourceStream {
    type Item = Event;

    fn poll_next(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        // For now, we'll use a simple approach
        // In a full implementation, this would properly handle async I/O
        if self.source.is_exhausted() {
            return Poll::Ready(None);
        }

        // This is a placeholder - real implementation would use proper async
        Poll::Pending
    }
}
