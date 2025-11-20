//! Stream abstraction and fluent API

use crate::core::{Event, EventKey, EventValue};
use crate::error::Result;
use crate::execution::{JoinBuilder, WindowedStream};
use crate::operators::{FilterOp, FlatMapOp, FlatMapOperator, MapOp, StreamOperator, WindowAssigner};
use futures::stream::{self, StreamExt};
use futures::FutureExt;
use std::pin::Pin;
use tokio::sync::mpsc;

type EventStream = Pin<Box<dyn futures::Stream<Item = Event> + Send>>;

/// A stream of events that can be transformed with operators
pub struct Stream {
    inner: EventStream,
}

impl Stream {
    /// Create a stream from an iterator of events
    pub fn from_iter<I>(iter: I) -> Self
    where
        I: IntoIterator<Item = Event> + Send + 'static,
        I::IntoIter: Send,
    {
        Self {
            inner: Box::pin(stream::iter(iter)),
        }
    }

    /// Create a stream from a Vec of values
    pub fn from_values<V>(values: Vec<V>) -> Self
    where
        V: Into<EventValue> + Send + 'static,
    {
        let events: Vec<_> = values
            .into_iter()
            .map(|v| Event::with_value(v.into()))
            .collect();
        Self::from_iter(events)
    }

    /// Create an empty stream
    pub fn empty() -> Self {
        Self {
            inner: Box::pin(stream::empty()),
        }
    }

    /// Create a stream from a channel receiver
    pub fn from_channel(mut rx: mpsc::Receiver<Event>) -> Self {
        Self {
            inner: Box::pin(stream::poll_fn(move |cx| rx.poll_recv(cx))),
        }
    }

    /// Create a stream from a source
    pub fn from_source<S>(source: S) -> Self
    where
        S: crate::sources::Source + 'static,
    {
        Self {
            inner: Box::pin(stream::unfold(source, |mut source| async move {
                if source.is_exhausted() {
                    None
                } else {
                    let event = source.read().await;
                    event.map(|e| (e, source))
                }
            })),
        }
    }

    /// Apply a filter operator to the stream
    pub fn filter<F>(self, predicate: F) -> Self
    where
        F: Fn(&Event) -> bool + Send + Sync + 'static,
    {
        let mut op = FilterOp::new(predicate);
        Self {
            inner: Box::pin(self.inner.filter_map(move |event| {
                let result = op.process(event);
                async move { result.ok().flatten() }
            })),
        }
    }

    /// Apply a map operator to the stream
    pub fn map<F>(self, mapper: F) -> Self
    where
        F: Fn(Event) -> Event + Send + Sync + 'static,
    {
        let mut op = MapOp::new(mapper);
        Self {
            inner: Box::pin(self.inner.filter_map(move |event| {
                let result = op.process(event);
                async move { result.ok().flatten() }
            })),
        }
    }

    /// Apply a flatmap operator to the stream
    pub fn flat_map<F>(self, mapper: F) -> Self
    where
        F: Fn(Event) -> Vec<Event> + Send + Sync + 'static,
    {
        let mut op = FlatMapOp::new(mapper);
        Self {
            inner: Box::pin(self.inner.flat_map(move |event| {
                let result = op.process_flat(event);
                stream::iter(result.unwrap_or_default())
            })),
        }
    }

    /// Take only the first n events
    pub fn take(self, n: usize) -> Self {
        Self {
            inner: Box::pin(self.inner.take(n)),
        }
    }

    /// Skip the first n events
    pub fn skip(self, n: usize) -> Self {
        Self {
            inner: Box::pin(self.inner.skip(n)),
        }
    }

    /// Re-key the stream with a new key extraction function
    pub fn key_by<F>(self, key_fn: F) -> Self
    where
        F: Fn(&Event) -> EventKey + Send + Sync + 'static,
    {
        self.map(move |event| {
            let new_key = key_fn(&event);
            event.with_key(new_key)
        })
    }

    /// Apply windowing to the stream
    pub async fn window<W>(self, assigner: W) -> Result<WindowedStream<W>>
    where
        W: WindowAssigner + 'static,
    {
        let events = self.collect().await?;
        Ok(WindowedStream::new(events, assigner))
    }

    /// Join this stream with another stream
    pub async fn join(self, other: Stream) -> Result<JoinBuilder> {
        let left_events = self.collect().await?;
        let right_events = other.collect().await?;
        Ok(JoinBuilder::new(left_events, right_events))
    }

    /// Collect all events into a Vec
    pub async fn collect(self) -> Result<Vec<Event>> {
        Ok(self.inner.collect::<Vec<_>>().await)
    }

    /// Count the number of events in the stream
    pub async fn count(self) -> Result<usize> {
        Ok(self.inner.count().await)
    }

    /// Write events to a sink
    pub async fn sink<S>(self, mut sink: S) -> Result<()>
    where
        S: crate::sinks::Sink,
    {
        use futures::StreamExt;
        let mut stream = self.inner;
        
        while let Some(event) = stream.next().await {
            sink.write(event)
                .await
                .map_err(|e| crate::error::StreamError::Unknown(format!("Sink error: {}", e)))?;
        }
        
        sink.close()
            .await
            .map_err(|e| crate::error::StreamError::Unknown(format!("Sink close error: {}", e)))?;
        
        Ok(())
    }

    /// Execute a function for each event (side effects)
    pub async fn for_each<F>(self, mut f: F) -> Result<()>
    where
        F: FnMut(Event) + Send,
    {
        let mut stream = self.inner;
        while let Some(event) = stream.next().await {
            f(event);
        }
        Ok(())
    }

    /// Fold the stream into a single value
    pub async fn fold<T, F>(self, init: T, mut f: F) -> Result<T>
    where
        T: Send,
        F: FnMut(T, Event) -> T + Send,
    {
        let mut stream = self.inner;
        let mut accumulator = init;
        while let Some(event) = stream.next().await {
            accumulator = f(accumulator, event);
        }
        Ok(accumulator)
    }

    /// Get the first event from the stream
    pub async fn first(mut self) -> Result<Option<Event>> {
        Ok(self.inner.next().await)
    }

    /// Get the last event from the stream
    pub async fn last(self) -> Result<Option<Event>> {
        let events = self.collect().await?;
        Ok(events.into_iter().last())
    }

    /// Check if any event matches the predicate
    pub async fn any<F>(self, mut predicate: F) -> Result<bool>
    where
        F: FnMut(&Event) -> bool + Send,
    {
        let mut stream = self.inner;
        while let Some(event) = stream.next().await {
            if predicate(&event) {
                return Ok(true);
            }
        }
        Ok(false)
    }

    /// Check if all events match the predicate
    pub async fn all<F>(self, mut predicate: F) -> Result<bool>
    where
        F: FnMut(&Event) -> bool + Send,
    {
        let mut stream = self.inner;
        while let Some(event) = stream.next().await {
            if !predicate(&event) {
                return Ok(false);
            }
        }
        Ok(true)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_stream_from_values() {
        let stream = Stream::from_values(vec![1, 2, 3, 4, 5]);
        let events = stream.collect().await.unwrap();
        assert_eq!(events.len(), 5);
    }

    #[tokio::test]
    async fn test_stream_filter() {
        let stream = Stream::from_values(vec![1, 2, 3, 4, 5])
            .filter(|e| e.value.as_int().unwrap_or(0) > 2);

        let events = stream.collect().await.unwrap();
        assert_eq!(events.len(), 3);
        assert_eq!(events[0].value.as_int(), Some(3));
        assert_eq!(events[1].value.as_int(), Some(4));
        assert_eq!(events[2].value.as_int(), Some(5));
    }

    #[tokio::test]
    async fn test_stream_map() {
        let stream = Stream::from_values(vec![1, 2, 3])
            .map(|e| {
                let val = e.value.as_int().unwrap_or(0) * 2;
                e.with_value_changed(EventValue::from_int(val))
            });

        let events = stream.collect().await.unwrap();
        assert_eq!(events[0].value.as_int(), Some(2));
        assert_eq!(events[1].value.as_int(), Some(4));
        assert_eq!(events[2].value.as_int(), Some(6));
    }

    #[tokio::test]
    async fn test_stream_flatmap() {
        let stream = Stream::from_values(vec![2, 3])
            .flat_map(|e| {
                let n = e.value.as_int().unwrap_or(0);
                (0..n)
                    .map(|i| Event::with_value(EventValue::from_int(i)))
                    .collect()
            });

        let events = stream.collect().await.unwrap();
        assert_eq!(events.len(), 5); // 2 + 3 = 5 events
    }

    #[tokio::test]
    async fn test_stream_chaining() {
        let stream = Stream::from_values(vec![1, 2, 3, 4, 5, 6])
            .filter(|e| e.value.as_int().unwrap_or(0) % 2 == 0)
            .map(|e| {
                let val = e.value.as_int().unwrap_or(0) * 10;
                e.with_value_changed(EventValue::from_int(val))
            });

        let events = stream.collect().await.unwrap();
        assert_eq!(events.len(), 3);
        assert_eq!(events[0].value.as_int(), Some(20));
        assert_eq!(events[1].value.as_int(), Some(40));
        assert_eq!(events[2].value.as_int(), Some(60));
    }

    #[tokio::test]
    async fn test_stream_count() {
        let stream = Stream::from_values(vec![1, 2, 3, 4, 5]);
        let count = stream.count().await.unwrap();
        assert_eq!(count, 5);
    }

    #[tokio::test]
    async fn test_stream_fold() {
        let stream = Stream::from_values(vec![1, 2, 3, 4, 5]);
        let sum = stream
            .fold(0, |acc, e| acc + e.value.as_int().unwrap_or(0))
            .await
            .unwrap();
        assert_eq!(sum, 15);
    }

    #[tokio::test]
    async fn test_stream_any_all() {
        let stream = Stream::from_values(vec![2, 4, 6]);
        let has_even = stream.any(|e| e.value.as_int().unwrap_or(0) % 2 == 0).await.unwrap();
        assert!(has_even);

        let stream = Stream::from_values(vec![2, 4, 6]);
        let all_even = stream.all(|e| e.value.as_int().unwrap_or(0) % 2 == 0).await.unwrap();
        assert!(all_even);
    }

    #[tokio::test]
    async fn test_stream_key_by() {
        let stream = Stream::from_values(vec![1, 2, 3])
            .key_by(|e| EventKey::from_int(e.value.as_int().unwrap_or(0) * 100));

        let events = stream.collect().await.unwrap();
        assert_eq!(events[0].key, EventKey::from_int(100));
        assert_eq!(events[1].key, EventKey::from_int(200));
        assert_eq!(events[2].key, EventKey::from_int(300));
    }
}
