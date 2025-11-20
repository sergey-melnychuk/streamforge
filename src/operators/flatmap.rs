//! FlatMap operator for one-to-many event transformations

use crate::core::Event;
use crate::error::Result;
use crate::operators::FlatMapOperator;

/// FlatMap operator that can produce zero or more events from each input
pub struct FlatMapOp<F>
where
    F: Fn(Event) -> Vec<Event> + Send + Sync,
{
    mapper: F,
}

impl<F> FlatMapOp<F>
where
    F: Fn(Event) -> Vec<Event> + Send + Sync,
{
    pub fn new(mapper: F) -> Self {
        Self { mapper }
    }
}

impl<F> FlatMapOperator for FlatMapOp<F>
where
    F: Fn(Event) -> Vec<Event> + Send + Sync,
{
    fn process_flat(&mut self, event: Event) -> Result<Vec<Event>> {
        Ok((self.mapper)(event))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::{EventKey, EventValue};

    #[test]
    fn test_flatmap_operator() {
        let mut flatmap = FlatMapOp::new(|e| {
            let val = e.value.as_int().unwrap_or(0);
            (0..val)
                .map(|i| Event::new(e.key.clone(), EventValue::from_int(i), e.timestamp))
                .collect()
        });

        let event = Event::new(EventKey::None, EventValue::from_int(3), 1000);
        let results = flatmap.process_flat(event).unwrap();

        assert_eq!(results.len(), 3);
        assert_eq!(results[0].value.as_int(), Some(0));
        assert_eq!(results[1].value.as_int(), Some(1));
        assert_eq!(results[2].value.as_int(), Some(2));
    }

    #[test]
    fn test_flatmap_empty_result() {
        let mut flatmap = FlatMapOp::new(|_| vec![]);

        let event = Event::new(EventKey::None, EventValue::from_int(1), 1000);
        let results = flatmap.process_flat(event).unwrap();

        assert_eq!(results.len(), 0);
    }

    #[test]
    fn test_flatmap_duplicates() {
        let mut flatmap = FlatMapOp::new(|e| vec![e.clone(), e.clone(), e]);

        let event = Event::new(EventKey::from_str("key"), EventValue::from_int(42), 1000);
        let results = flatmap.process_flat(event).unwrap();

        assert_eq!(results.len(), 3);
        assert!(results.iter().all(|e| e.value.as_int() == Some(42)));
    }
}
