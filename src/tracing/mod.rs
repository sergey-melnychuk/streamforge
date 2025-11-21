//! Distributed tracing support for StreamForge
//!
//! Provides trace context propagation and OpenTelemetry integration
//! for end-to-end request tracing across distributed nodes.

pub mod context;
pub mod exporter;
pub mod instrumentation;

#[cfg(test)]
mod tests;

pub use context::{SpanId, TraceContext, TraceId};
pub use exporter::{init_tracing, TraceExporter, TraceExporterConfig};
pub use instrumentation::{with_trace_context, TraceInstrumentation};
