//! Iterator source for converting iterators to sources

use crate::core::Event;
use crate::sources::Source;
use async_trait::async_trait;
use std::collections::VecDeque;

/// Iterator source that wraps an iterator
pub struct IteratorSource {
    events: VecDeque<Event>,
}

impl IteratorSource {
    /// Create from an iterator of events
    #[allow(clippy::should_implement_trait)]
    pub fn from_iter<I>(iter: I) -> Self
    where
        I: IntoIterator<Item = Event>,
    {
        Self {
            events: iter.into_iter().collect(),
        }
    }
}

#[async_trait]
impl Source for IteratorSource {
    async fn read(&mut self) -> Option<Event> {
        self.events.pop_front()
    }

    fn is_exhausted(&self) -> bool {
        self.events.is_empty()
    }
}
