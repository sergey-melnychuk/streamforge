//! Configuration builder for fluent configuration

use super::Config;

/// Builder for creating stream processing configurations
pub struct ConfigBuilder {
    parallelism: Option<usize>,
    buffer_size: Option<usize>,
}

impl ConfigBuilder {
    pub fn new() -> Self {
        Self {
            parallelism: None,
            buffer_size: None,
        }
    }

    pub fn with_parallelism(mut self, parallelism: usize) -> Self {
        self.parallelism = Some(parallelism);
        self
    }

    pub fn with_buffer_size(mut self, buffer_size: usize) -> Self {
        self.buffer_size = Some(buffer_size);
        self
    }

    pub fn build(self) -> Config {
        let default = Config::default();
        Config {
            parallelism: self.parallelism.unwrap_or(default.parallelism),
            buffer_size: self.buffer_size.unwrap_or(default.buffer_size),
        }
    }
}

impl Default for ConfigBuilder {
    fn default() -> Self {
        Self::new()
    }
}
