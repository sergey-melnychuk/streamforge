//! Query executor
//!
//! Executes queries against streams

use crate::core::event::Event;
use crate::query::ast::*;
use tracing::{debug, info};

/// Query execution result
pub type QueryResult = Result<(), QueryExecutionError>;

/// Query execution error
#[derive(Debug, thiserror::Error)]
pub enum QueryExecutionError {
    #[error("Execution error: {0}")]
    Execution(String),
    #[error("Stream not found: {0}")]
    StreamNotFound(String),
}

/// Query executor
pub struct QueryExecutor {
    query: Query,
}

impl QueryExecutor {
    /// Create a new query executor
    pub fn new(query: Query) -> Self {
        Self { query }
    }

    /// Execute the query against a stream
    pub async fn execute<F>(&self, _event_handler: F) -> QueryResult
    where
        F: FnMut(Event) -> Result<(), String>,
    {
        info!("Executing query on stream: {}", self.query.from.stream);

        // For now, this is a placeholder
        // A full implementation would:
        // - Connect to the stream
        // - Apply WHERE filters
        // - Apply SELECT projections
        // - Apply aggregations
        // - Apply windowing
        // - Emit results

        debug!("Query execution started");
        Ok(())
    }

    /// Check if event matches WHERE clause
    pub fn matches_filter(&self, event: &Event) -> bool {
        if let Some(ref where_clause) = self.query.where_clause {
            match where_clause.condition.evaluate(event) {
                Value::Boolean(b) => b,
                _ => false,
            }
        } else {
            true
        }
    }

    /// Project event according to SELECT clause
    pub fn project(&self, event: &Event) -> Event {
        // TODO: Implement field projection based on SELECT fields
        // For now, return event as-is
        event.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::event::{Event, EventKey, EventValue};
    use serde_json::json;

    #[tokio::test]
    async fn test_query_executor_filter() {
        let query = Query::select("events").where_clause(Expression::binary_op(
            Expression::field("temperature"),
            BinaryOperator::Gt,
            Expression::literal(Value::Integer(20)),
        ));

        let executor = QueryExecutor::new(query);

        let event_data = json!({"temperature": 25});
        let event = Event::new(
            EventKey::from_str("sensor"),
            EventValue::Json(event_data),
            0,
        );

        assert!(executor.matches_filter(&event));

        let event_data = json!({"temperature": 15});
        let event = Event::new(
            EventKey::from_str("sensor"),
            EventValue::Json(event_data),
            0,
        );

        assert!(!executor.matches_filter(&event));
    }
}
