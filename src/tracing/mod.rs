//! Distributed tracing support for StreamForge
//!
//! Provides trace context propagation and OpenTelemetry integration
//! for end-to-end request tracing across distributed nodes.

pub mod context;
pub mod instrumentation;
pub mod exporter;

#[cfg(test)]
mod tests;

pub use context::{TraceContext, TraceId, SpanId};
pub use instrumentation::{TraceInstrumentation, with_trace_context};
pub use exporter::{TraceExporter, TraceExporterConfig, init_tracing};

