//! Filter operator for selectively passing events

use crate::core::Event;
use crate::error::Result;
use crate::operators::StreamOperator;

/// Filter operator that only passes events matching a predicate
pub struct FilterOp<F>
where
    F: Fn(&Event) -> bool + Send + Sync,
{
    predicate: F,
}

impl<F> FilterOp<F>
where
    F: Fn(&Event) -> bool + Send + Sync,
{
    pub fn new(predicate: F) -> Self {
        Self { predicate }
    }
}

impl<F> StreamOperator for FilterOp<F>
where
    F: Fn(&Event) -> bool + Send + Sync,
{
    fn process(&mut self, event: Event) -> Result<Option<Event>> {
        if (self.predicate)(&event) {
            Ok(Some(event))
        } else {
            Ok(None)
        }
    }

    fn process_batch(&mut self, events: Vec<Event>) -> Result<Vec<Event>> {
        Ok(events.into_iter().filter(|e| (self.predicate)(e)).collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::{EventKey, EventValue};

    #[test]
    fn test_filter_operator() {
        let mut filter = FilterOp::new(|e| e.value.as_int().unwrap_or(0) > 5);

        let event1 = Event::new(EventKey::None, EventValue::from_int(10), 1000);
        let event2 = Event::new(EventKey::None, EventValue::from_int(3), 1000);

        assert!(filter.process(event1).unwrap().is_some());
        assert!(filter.process(event2).unwrap().is_none());
    }

    #[test]
    fn test_filter_batch() {
        let mut filter = FilterOp::new(|e| e.value.as_int().unwrap_or(0) % 2 == 0);

        let events = vec![
            Event::new(EventKey::None, EventValue::from_int(2), 1000),
            Event::new(EventKey::None, EventValue::from_int(3), 1000),
            Event::new(EventKey::None, EventValue::from_int(4), 1000),
            Event::new(EventKey::None, EventValue::from_int(5), 1000),
        ];

        let result = filter.process_batch(events).unwrap();
        assert_eq!(result.len(), 2);
        assert_eq!(result[0].value.as_int(), Some(2));
        assert_eq!(result[1].value.as_int(), Some(4));
    }
}
