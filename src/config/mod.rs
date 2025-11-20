//! Configuration management for StreamForge

pub mod builder;

pub use builder::ConfigBuilder;

/// Configuration for stream processing
#[derive(Debug, Clone)]
pub struct Config {
    pub parallelism: usize,
    pub buffer_size: usize,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            parallelism: num_cpus::get(),
            buffer_size: 1024,
        }
    }
}
