//! Configuration management for StreamForge
//!
//! Provides comprehensive configuration for all StreamForge components

pub mod builder;
pub mod cluster;
pub mod network;
pub mod state;

pub use builder::ConfigBuilder;

use serde::{Deserialize, Serialize};

/// Main configuration for StreamForge
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct Config {
    /// Stream processing configuration
    pub processing: ProcessingConfig,
    /// Cluster configuration
    pub cluster: cluster::ClusterConfig,
    /// State backend configuration
    pub state: state::StateConfig,
    /// Network configuration
    pub network: network::NetworkConfig,
    /// Metrics configuration
    pub metrics: MetricsConfig,
    /// Job definition (sources, operators, sinks)
    #[serde(default)]
    pub job: Option<JobDefinition>,
}

/// Stream processing configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProcessingConfig {
    /// Number of parallel workers
    pub parallelism: usize,
    /// Buffer size for operators
    pub buffer_size: usize,
    /// Enable backpressure
    pub backpressure_enabled: bool,
    /// Max events per batch
    pub batch_size: usize,
}

impl Default for ProcessingConfig {
    fn default() -> Self {
        Self {
            parallelism: num_cpus::get(),
            buffer_size: 1024,
            backpressure_enabled: true,
            batch_size: 1000,
        }
    }
}

/// Metrics configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetricsConfig {
    /// Enable metrics collection
    pub enabled: bool,
    /// Metrics export interval (seconds)
    pub export_interval: u64,
    /// Metrics export endpoint (optional)
    pub export_endpoint: Option<String>,
}

impl Default for MetricsConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            export_interval: 10,
            export_endpoint: None,
        }
    }
}

/// Job definition for stream processing pipeline
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JobDefinition {
    /// Source configuration
    pub source: SourceConfig,
    /// Sink configuration
    pub sink: SinkConfig,
    /// Optional operators to apply
    #[serde(default)]
    pub operators: Vec<OperatorConfig>,
    /// Optional SQL query (alternative to operators)
    #[serde(default)]
    pub sql: Option<String>,
}

/// Source configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum SourceConfig {
    /// File source
    #[serde(rename = "file")]
    File {
        /// File path
        path: String,
        /// Format: "json", "csv", "text"
        #[serde(default = "default_format")]
        format: String,
        /// Whether to read continuously (tail -f style)
        #[serde(default)]
        follow: bool,
    },
    /// HTTP source
    #[serde(rename = "http")]
    Http {
        /// URLs to poll
        urls: Vec<String>,
        /// Polling interval in seconds
        #[serde(default = "default_poll_interval")]
        poll_interval: u64,
        /// Request timeout in seconds
        #[serde(default = "default_timeout")]
        timeout: u64,
        /// JSON path to extract value (e.g., "$.price")
        #[serde(default)]
        value_path: Option<String>,
        /// JSON path to extract key (e.g., "$.symbol")
        #[serde(default)]
        key_path: Option<String>,
    },
}

/// Sink configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum SinkConfig {
    /// File sink
    #[serde(rename = "file")]
    File {
        /// File path
        path: String,
        /// Format: "json", "csv", "text"
        #[serde(default = "default_format")]
        format: String,
        /// Append to file (true) or overwrite (false)
        #[serde(default = "default_append")]
        append: bool,
    },
    /// Metrics sink (Prometheus format)
    #[serde(rename = "metrics")]
    Metrics {
        /// Metric name prefix
        #[serde(default = "default_metric_prefix")]
        prefix: String,
        /// HTTP endpoint to expose metrics (e.g., "127.0.0.1:9090")
        #[serde(default)]
        http_endpoint: Option<String>,
    },
}

/// Operator configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum OperatorConfig {
    /// Filter operator (placeholder - filter logic would need to be defined)
    #[serde(rename = "filter")]
    Filter {
        /// Filter expression (placeholder)
        expression: String,
    },
    /// Map operator (placeholder - map logic would need to be defined)
    #[serde(rename = "map")]
    Map {
        /// Map expression (placeholder)
        expression: String,
    },
}

fn default_format() -> String {
    "json".to_string()
}

fn default_append() -> bool {
    true
}

fn default_poll_interval() -> u64 {
    1
}

fn default_timeout() -> u64 {
    5
}

fn default_metric_prefix() -> String {
    "streamforge".to_string()
}

impl Config {
    /// Load configuration from file
    pub fn from_file<P: AsRef<std::path::Path>>(path: P) -> Result<Self, ConfigError> {
        let content = std::fs::read_to_string(path)?;
        let config: Config = toml::from_str(&content)
            .map_err(|e| ConfigError::Parse(format!("TOML parse error: {}", e)))?;
        Ok(config)
    }

    /// Save configuration to file
    pub fn to_file<P: AsRef<std::path::Path>>(&self, path: P) -> Result<(), ConfigError> {
        let content = toml::to_string_pretty(self)
            .map_err(|e| ConfigError::Serialize(format!("TOML serialize error: {}", e)))?;
        std::fs::write(path, content)?;
        Ok(())
    }

    /// Load from environment variables
    pub fn from_env() -> Self {
        let mut config = Config::default();

        if let Ok(parallelism) = std::env::var("STREAMFORGE_PARALLELISM") {
            if let Ok(p) = parallelism.parse() {
                config.processing.parallelism = p;
            }
        }

        if let Ok(buffer_size) = std::env::var("STREAMFORGE_BUFFER_SIZE") {
            if let Ok(b) = buffer_size.parse() {
                config.processing.buffer_size = b;
            }
        }

        config
    }
}

/// Configuration error
#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
    #[error("Parse error: {0}")]
    Parse(String),
    #[error("Serialize error: {0}")]
    Serialize(String),
}
