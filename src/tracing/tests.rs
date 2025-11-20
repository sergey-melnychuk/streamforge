//! Tests for distributed tracing

#[cfg(test)]
mod tests {
    use crate::tracing::context::{TraceContext, TraceId, SpanId};
    use crate::tracing::instrumentation::{current_context, set_context, clear_context, with_trace_context};
    use bincode;

    #[test]
    fn test_trace_context_propagation() {
        // Create root context
        let root = TraceContext::new();
        let root_trace_id = root.trace_id;
        let root_span_id = root.span_id;

        // Set context
        set_context(root.clone());
        assert_eq!(current_context().unwrap().trace_id, root_trace_id);

        // Create child
        let child = root.child();
        assert_eq!(child.trace_id, root_trace_id);
        assert_eq!(child.parent_span_id, Some(root_span_id));

        // Clear context
        clear_context();
        assert!(current_context().is_none());
    }

    #[test]
    fn test_with_trace_context() {
        let ctx = TraceContext::new();
        let trace_id = ctx.trace_id;

        let result = with_trace_context(ctx, || {
            let current = current_context().unwrap();
            assert_eq!(current.trace_id, trace_id);
            42
        });

        assert_eq!(result, 42);
        assert!(current_context().is_none());
    }

    #[test]
    fn test_trace_context_serialization() {
        let mut ctx = TraceContext::new();
        ctx.set_attribute("key1".to_string(), "value1".to_string());
        ctx.set_attribute("key2".to_string(), "value2".to_string());

        // Serialize
        let serialized = bincode::serialize(&ctx).unwrap();
        
        // Deserialize
        let deserialized: TraceContext = bincode::deserialize(&serialized).unwrap();
        
        assert_eq!(deserialized.trace_id, ctx.trace_id);
        assert_eq!(deserialized.span_id, ctx.span_id);
        assert_eq!(deserialized.parent_span_id, ctx.parent_span_id);
        assert_eq!(deserialized.get_attribute("key1"), Some(&"value1".to_string()));
        assert_eq!(deserialized.get_attribute("key2"), Some(&"value2".to_string()));
    }

    #[test]
    fn test_trace_id_roundtrip() {
        let id = TraceId::new();
        let hex = id.to_hex();
        let parsed = TraceId::from_hex(&hex).unwrap();
        assert_eq!(parsed, id);
    }

    #[test]
    fn test_span_id_roundtrip() {
        let id = SpanId::new();
        let hex = id.to_hex();
        let parsed = SpanId::from_hex(&hex).unwrap();
        assert_eq!(parsed, id);
    }
}
