//! StreamForge CLI - Command-line interface for job management

use clap::{Parser, Subcommand};
use std::path::PathBuf;

#[path = "../cli/mod.rs"]
mod cli;

use cli::commands::*;

#[derive(Parser)]
#[command(name = "streamforge")]
#[command(about = "Ultra-fast, distributed stream processing engine", long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Submit a new stream processing job
    Submit {
        /// Path to job configuration file
        #[arg(short, long)]
        config: PathBuf,
        /// Job name (optional, defaults to config filename)
        #[arg(short, long)]
        name: Option<String>,
        /// Run in daemon mode (automatically restart on failure)
        #[arg(long)]
        daemon: bool,
        /// Maximum restart attempts in daemon mode (default: 3)
        #[arg(long, default_value = "3")]
        max_restarts: u32,
        /// Delay between restart attempts in seconds (default: 5)
        #[arg(long, default_value = "5")]
        restart_delay: u64,
        /// Cluster nodes to try submitting to (host:port pairs, e.g., "127.0.0.1:9001,127.0.0.1:9002")
        #[arg(long)]
        nodes: Option<String>,
    },
    /// List all running jobs
    List {
        /// Show detailed information
        #[arg(short, long)]
        verbose: bool,
        /// Cluster nodes to query (host:port pairs, e.g., "127.0.0.1:9001,127.0.0.1:9002")
        #[arg(long)]
        nodes: Option<String>,
    },
    /// Show job status
    Status {
        /// Job ID
        job_id: String,
        /// Cluster nodes to query (host:port pairs, e.g., "127.0.0.1:9001,127.0.0.1:9002")
        #[arg(long)]
        nodes: Option<String>,
    },
    /// Stop a running job
    Stop {
        /// Job ID
        job_id: String,
        /// Force stop (don't wait for graceful shutdown)
        #[arg(short, long)]
        force: bool,
        /// Cluster nodes to query (host:port pairs, e.g., "127.0.0.1:9001,127.0.0.1:9002")
        #[arg(long)]
        nodes: Option<String>,
    },
    /// Validate a job configuration file
    Validate {
        /// Path to job configuration file
        config: PathBuf,
    },
    /// Show cluster status
    Cluster {
        #[command(subcommand)]
        command: Option<ClusterCommands>,
    },
    /// Start a cluster node
    Node {
        /// Path to node configuration file
        #[arg(short, long)]
        config: PathBuf,
        /// Run in daemon mode (restart on failure)
        #[arg(long)]
        daemon: bool,
    },
    /// Check if cluster nodes are ready
    Ready {
        /// Cluster nodes to check (host:port pairs, e.g., "127.0.0.1:9001,127.0.0.1:9002")
        #[arg(short, long)]
        nodes: String,
    },
}

#[derive(Subcommand)]
enum ClusterCommands {
    /// Show cluster status
    Status,
    /// Show cluster nodes
    Nodes,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // Initialize tracing
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    let cli = Cli::parse();

    match cli.command {
        Commands::Submit {
            config,
            name,
            daemon,
            max_restarts,
            restart_delay,
            nodes,
        } => {
            submit_job(config, name, daemon, max_restarts, restart_delay, nodes).await?;
        }
        Commands::List { verbose, nodes } => {
            list_jobs(verbose, nodes).await?;
        }
        Commands::Status { job_id, nodes } => {
            show_job_status(job_id, nodes).await?;
        }
        Commands::Stop {
            job_id,
            force,
            nodes,
        } => {
            stop_job(job_id, force, nodes).await?;
        }
        Commands::Validate { config } => {
            validate_config(config).await?;
        }
        Commands::Cluster { command } => match command {
            Some(ClusterCommands::Status) | None => {
                cluster_status().await?;
            }
            Some(ClusterCommands::Nodes) => {
                cluster_nodes().await?;
            }
        },
        Commands::Node { config, daemon } => {
            start_node(config, daemon).await?;
        }
        Commands::Ready { nodes } => {
            check_nodes_ready(nodes).await?;
        }
    }

    Ok(())
}
