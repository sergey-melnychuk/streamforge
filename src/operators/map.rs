//! Map operator for transforming events

use crate::core::Event;
use crate::error::Result;
use crate::operators::StreamOperator;

/// Map operator that transforms each event
pub struct MapOp<F>
where
    F: Fn(Event) -> Event + Send + Sync,
{
    mapper: F,
}

impl<F> MapOp<F>
where
    F: Fn(Event) -> Event + Send + Sync,
{
    pub fn new(mapper: F) -> Self {
        Self { mapper }
    }
}

impl<F> StreamOperator for MapOp<F>
where
    F: Fn(Event) -> Event + Send + Sync,
{
    fn process(&mut self, event: Event) -> Result<Option<Event>> {
        Ok(Some((self.mapper)(event)))
    }

    fn process_batch(&mut self, events: Vec<Event>) -> Result<Vec<Event>> {
        Ok(events.into_iter().map(|e| (self.mapper)(e)).collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::{EventKey, EventValue};

    #[test]
    fn test_map_operator() {
        let mut mapper = MapOp::new(|e| {
            let new_val = e.value.as_int().unwrap_or(0) * 2;
            e.with_value_changed(EventValue::from_int(new_val))
        });

        let event = Event::new(EventKey::None, EventValue::from_int(5), 1000);
        let result = mapper.process(event).unwrap().unwrap();

        assert_eq!(result.value.as_int(), Some(10));
    }

    #[test]
    fn test_map_batch() {
        let mut mapper = MapOp::new(|e| {
            let new_val = e.value.as_int().unwrap_or(0) + 1;
            e.with_value_changed(EventValue::from_int(new_val))
        });

        let events = vec![
            Event::new(EventKey::None, EventValue::from_int(1), 1000),
            Event::new(EventKey::None, EventValue::from_int(2), 1000),
            Event::new(EventKey::None, EventValue::from_int(3), 1000),
        ];

        let results = mapper.process_batch(events).unwrap();
        assert_eq!(results.len(), 3);
        assert_eq!(results[0].value.as_int(), Some(2));
        assert_eq!(results[1].value.as_int(), Some(3));
        assert_eq!(results[2].value.as_int(), Some(4));
    }
}
