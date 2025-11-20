//! Configuration management example
//!
//! Demonstrates how to use StreamForge configuration system
//!
//! Run with: cargo run --example config_example

use streamforge::config::{Config, ConfigBuilder};
use std::path::PathBuf;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("╔══════════════════════════════════════════════════════════════╗");
    println!("║     StreamForge Configuration Example                        ║");
    println!("╚══════════════════════════════════════════════════════════════╝\n");

    // ========================================================================
    // Method 1: Default configuration
    // ========================================================================
    println!("📋 Method 1: Default Configuration\n");
    let default_config = Config::default();
    println!("   Parallelism: {}", default_config.processing.parallelism);
    println!("   Buffer size: {}", default_config.processing.buffer_size);
    println!("   Backpressure: {}", default_config.processing.backpressure_enabled);
    println!();

    // ========================================================================
    // Method 2: Builder pattern
    // ========================================================================
    println!("🔧 Method 2: Builder Pattern\n");
    let custom_config = ConfigBuilder::new()
        .with_parallelism(8)
        .with_buffer_size(2048)
        .with_batch_size(5000)
        .with_backpressure(true)
        .with_cluster_bind_address("0.0.0.0:9000".to_string())
        .with_seed_nodes(vec![
            "127.0.0.1:9001".to_string(),
            "127.0.0.1:9002".to_string(),
        ])
        .with_state_backend(streamforge::config::state::StateBackendType::RocksDB)
        .with_state_dir(PathBuf::from("./data/state"))
        .with_metrics_enabled(true)
        .build();

    println!("   Parallelism: {}", custom_config.processing.parallelism);
    println!("   Buffer size: {}", custom_config.processing.buffer_size);
    println!("   Batch size: {}", custom_config.processing.batch_size);
    println!("   Cluster bind: {}", custom_config.cluster.bind_address);
    println!("   Seed nodes: {:?}", custom_config.cluster.seed_nodes);
    println!("   State backend: {:?}", custom_config.state.backend_type);
    println!();

    // ========================================================================
    // Method 3: From environment variables
    // ========================================================================
    println!("🌍 Method 3: Environment Variables\n");
    std::env::set_var("STREAMFORGE_PARALLELISM", "16");
    std::env::set_var("STREAMFORGE_BUFFER_SIZE", "4096");
    let env_config = Config::from_env();
    println!("   Parallelism (from env): {}", env_config.processing.parallelism);
    println!("   Buffer size (from env): {}", env_config.processing.buffer_size);
    println!();

    // ========================================================================
    // Method 4: From TOML file
    // ========================================================================
    println!("📄 Method 4: TOML Configuration File\n");
    
    // Create a sample config file
    let config_file = "streamforge.toml";
    custom_config.to_file(config_file)?;
    println!("   ✓ Created config file: {}", config_file);
    
    // Load from file
    let loaded_config = Config::from_file(config_file)?;
    println!("   ✓ Loaded config from file");
    println!("   Parallelism: {}", loaded_config.processing.parallelism);
    println!("   State backend: {:?}", loaded_config.state.backend_type);
    println!();

    // Cleanup
    std::fs::remove_file(config_file)?;
    println!("   ✓ Cleaned up config file");
    println!();

    println!("╔══════════════════════════════════════════════════════════════╗");
    println!("║     Configuration example complete!                           ║");
    println!("╚══════════════════════════════════════════════════════════════╝");

    Ok(())
}

