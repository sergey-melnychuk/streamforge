//! Trace exporter configuration and initialization

use serde::{Deserialize, Serialize};

/// Trace exporter configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TraceExporterConfig {
    /// Exporter type
    pub exporter_type: TraceExporterType,
    /// Service name
    pub service_name: String,
    /// Endpoint URL (for OTLP, Jaeger, etc.)
    pub endpoint: Option<String>,
    /// Sampling rate (0.0 to 1.0)
    pub sampling_rate: f64,
}

/// Trace exporter type
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum TraceExporterType {
    /// No exporter (traces only logged)
    None,
    /// Jaeger exporter
    Jaeger,
    /// OTLP exporter (OpenTelemetry Protocol)
    Otlp,
    /// Console exporter (for debugging)
    Console,
}

impl Default for TraceExporterConfig {
    fn default() -> Self {
        Self {
            exporter_type: TraceExporterType::None,
            service_name: "streamforge".to_string(),
            endpoint: None,
            sampling_rate: 1.0,
        }
    }
}

/// Trace exporter
pub struct TraceExporter {
    config: TraceExporterConfig,
}

impl TraceExporter {
    /// Create a new trace exporter
    pub fn new(config: TraceExporterConfig) -> Self {
        Self { config }
    }

    /// Initialize the tracing system
    pub fn init(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        match self.config.exporter_type {
            TraceExporterType::None => {
                // Just use basic tracing subscriber
                tracing_subscriber::fmt()
                    .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
                    .init();
            }
            TraceExporterType::Console => {
                // Console exporter for debugging
                tracing_subscriber::fmt()
                    .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
                    .pretty()
                    .init();
            }
            TraceExporterType::Jaeger => {
                // Jaeger exporter - requires feature flag and additional setup
                tracing_subscriber::fmt()
                    .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
                    .init();
                tracing::warn!("Jaeger exporter requested but not fully implemented (requires 'jaeger' feature)");
            }
            TraceExporterType::Otlp => {
                // OTLP exporter - requires feature flag and additional setup
                tracing_subscriber::fmt()
                    .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
                    .init();
                tracing::warn!("OTLP exporter requested but not fully implemented (requires 'otlp' feature)");
            }
        }
        
        Ok(())
    }
}

/// Initialize tracing with default configuration
pub fn init_tracing(service_name: &str) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let config = TraceExporterConfig {
        service_name: service_name.to_string(),
        ..Default::default()
    };
    let exporter = TraceExporter::new(config);
    exporter.init()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_trace_exporter_config_default() {
        let config = TraceExporterConfig::default();
        assert_eq!(config.exporter_type, TraceExporterType::None);
        assert_eq!(config.service_name, "streamforge");
        assert_eq!(config.sampling_rate, 1.0);
    }
}
