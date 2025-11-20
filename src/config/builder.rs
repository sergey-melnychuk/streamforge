//! Configuration builder for fluent configuration

use super::{Config, ProcessingConfig};

/// Builder for creating stream processing configurations
pub struct ConfigBuilder {
    config: Config,
}

impl ConfigBuilder {
    pub fn new() -> Self {
        Self {
            config: Config::default(),
        }
    }

    pub fn with_parallelism(mut self, parallelism: usize) -> Self {
        self.config.processing.parallelism = parallelism;
        self
    }

    pub fn with_buffer_size(mut self, buffer_size: usize) -> Self {
        self.config.processing.buffer_size = buffer_size;
        self
    }

    pub fn with_batch_size(mut self, batch_size: usize) -> Self {
        self.config.processing.batch_size = batch_size;
        self
    }

    pub fn with_backpressure(mut self, enabled: bool) -> Self {
        self.config.processing.backpressure_enabled = enabled;
        self
    }

    pub fn with_cluster_bind_address(mut self, address: String) -> Self {
        self.config.cluster.bind_address = address;
        self
    }

    pub fn with_seed_nodes(mut self, nodes: Vec<String>) -> Self {
        self.config.cluster.seed_nodes = nodes;
        self
    }

    pub fn with_state_backend(mut self, backend_type: super::state::StateBackendType) -> Self {
        self.config.state.backend_type = backend_type;
        self
    }

    pub fn with_state_dir(mut self, dir: std::path::PathBuf) -> Self {
        self.config.state.state_dir = dir;
        self
    }

    pub fn with_metrics_enabled(mut self, enabled: bool) -> Self {
        self.config.metrics.enabled = enabled;
        self
    }

    pub fn build(self) -> Config {
        self.config
    }
}

impl Default for ConfigBuilder {
    fn default() -> Self {
        Self::new()
    }
}
