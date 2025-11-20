//! Trace instrumentation helpers

use crate::tracing::context::{TraceContext, TraceId, SpanId};
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{span, Level, Span};

/// Thread-local trace context storage
thread_local! {
    static CURRENT_CONTEXT: std::cell::RefCell<Option<TraceContext>> = std::cell::RefCell::new(None);
}

/// Get the current trace context
pub fn current_context() -> Option<TraceContext> {
    CURRENT_CONTEXT.with(|ctx| ctx.borrow().clone())
}

/// Set the current trace context
pub fn set_context(ctx: TraceContext) {
    CURRENT_CONTEXT.with(|cell| {
        *cell.borrow_mut() = Some(ctx);
    });
}

/// Clear the current trace context
pub fn clear_context() {
    CURRENT_CONTEXT.with(|cell| {
        *cell.borrow_mut() = None;
    });
}

/// Execute a function with a trace context
pub fn with_trace_context<F, R>(ctx: TraceContext, f: F) -> R
where
    F: FnOnce() -> R,
{
    let old_ctx = current_context();
    set_context(ctx.clone());
    
    // Create a tracing span from the context
    let parent_span_id_str = ctx.parent_span_id.as_ref().map(|id| id.to_string());
    let span = if let Some(ref parent_id) = parent_span_id_str {
        span!(
            Level::INFO,
            "operation",
            trace_id = %ctx.trace_id,
            span_id = %ctx.span_id,
            parent_span_id = %parent_id,
        )
    } else {
        span!(
            Level::INFO,
            "operation",
            trace_id = %ctx.trace_id,
            span_id = %ctx.span_id,
        )
    };
    
    let _guard = span.enter();
    let result = f();
    drop(_guard);
    
    if let Some(old) = old_ctx {
        set_context(old);
    } else {
        clear_context();
    }
    
    result
}

/// Trace instrumentation helper
pub struct TraceInstrumentation;

impl TraceInstrumentation {
    /// Start a new span for an operation
    pub fn start_span(name: &str, ctx: Option<&TraceContext>) -> (Span, TraceContext) {
        use tracing::span;
        
        let trace_ctx = if let Some(parent) = ctx {
            parent.child()
        } else {
            TraceContext::new()
        };
        
        let parent_span_id_str = trace_ctx.parent_span_id.as_ref().map(|id| id.to_string());
        let span = if let Some(ref parent_id) = parent_span_id_str {
            span!(Level::INFO, "operation",
                name = name,
                trace_id = %trace_ctx.trace_id,
                span_id = %trace_ctx.span_id,
                parent_span_id = %parent_id,
            )
        } else {
            span!(Level::INFO, "operation",
                name = name,
                trace_id = %trace_ctx.trace_id,
                span_id = %trace_ctx.span_id,
            )
        };
        
        (span, trace_ctx)
    }

    /// Add attributes to the current span
    pub fn add_attributes(span: &Span, attributes: &std::collections::HashMap<String, String>) {
        for (key, value) in attributes {
            span.record(key.as_str(), value.as_str());
        }
    }

    /// Record an error in the current span
    pub fn record_error(span: &Span, error: &dyn std::error::Error) {
        span.record("error", true);
        span.record("error.message", error.to_string());
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_context_storage() {
        let ctx = TraceContext::new();
        set_context(ctx.clone());
        let retrieved = current_context().unwrap();
        assert_eq!(retrieved.trace_id, ctx.trace_id);
        clear_context();
        assert!(current_context().is_none());
    }

    #[test]
    fn test_with_trace_context() {
        let ctx = TraceContext::new();
        let result = with_trace_context(ctx.clone(), || {
            let current = current_context().unwrap();
            assert_eq!(current.trace_id, ctx.trace_id);
            42
        });
        assert_eq!(result, 42);
        assert!(current_context().is_none());
    }
}

