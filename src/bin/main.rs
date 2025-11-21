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
    },
    /// List all running jobs
    List {
        /// Show detailed information
        #[arg(short, long)]
        verbose: bool,
    },
    /// Show job status
    Status {
        /// Job ID
        job_id: String,
    },
    /// Stop a running job
    Stop {
        /// Job ID
        job_id: String,
        /// Force stop (don't wait for graceful shutdown)
        #[arg(short, long)]
        force: bool,
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
        Commands::Submit { config, name } => {
            submit_job(config, name).await?;
        }
        Commands::List { verbose } => {
            list_jobs(verbose).await?;
        }
        Commands::Status { job_id } => {
            show_status(&job_id).await?;
        }
        Commands::Stop { job_id, force } => {
            stop_job(&job_id, force).await?;
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
    }

    Ok(())
}
