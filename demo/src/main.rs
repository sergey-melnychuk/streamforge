//! StreamForge MVP Demo
//!
//! This demo showcases all MVP features:
//! 1. File sources and sinks
//! 2. Stream processing with operators
//! 3. Distributed execution
//! 4. Stateful operators
//! 5. Cluster setup

mod demo_file_processing;
mod demo_distributed;
mod demo_stateful;
mod demo_production;
mod demo_production_distributed;

use std::env;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize tracing
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    let args: Vec<String> = env::args().collect();
    let demo = args.get(1).map(|s| s.as_str()).unwrap_or("all");

    match demo {
        "file" => {
            println!("=== File Source & Sink Demo ===\n");
            demo_file_processing::run().await?;
        }
        "distributed" => {
            println!("=== Distributed Execution Demo ===\n");
            demo_distributed::run().await?;
        }
        "stateful" => {
            println!("=== Stateful Operators Demo ===\n");
            demo_stateful::run().await?;
        }
        "production" => {
            println!("=== Production Workload Demo (Single Node) ===\n");
            demo_production::run().await?;
        }
        "production-distributed" | "prod-dist" => {
            println!("=== Distributed Production Workload Demo ===\n");
            demo_production_distributed::run().await?;
        }
        "all" | _ => {
            println!("╔══════════════════════════════════════════════════════════════╗");
            println!("║     StreamForge MVP Demo - All Features                     ║");
            println!("╚══════════════════════════════════════════════════════════════╝\n");

            println!("1. File Source & Sink Demo\n");
            demo_file_processing::run().await?;
            println!("\n");

            println!("2. Distributed Execution Demo\n");
            demo_distributed::run().await?;
            println!("\n");

            println!("3. Stateful Operators Demo\n");
            demo_stateful::run().await?;
            println!("\n");

            println!("4. Production Workload Demo (Single Node)\n");
            demo_production::run().await?;
            println!("\n");

            println!("5. Distributed Production Workload Demo\n");
            demo_production_distributed::run().await?;
            println!("\n");

            println!("╔══════════════════════════════════════════════════════════════╗");
            println!("║     All Demos Complete!                                      ║");
            println!("╚══════════════════════════════════════════════════════════════╝");
        }
    }

    Ok(())
}

