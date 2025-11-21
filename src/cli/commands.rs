//! CLI command implementations

use crate::cli::executor::JobExecutor;
use crate::cli::job::JobManager;
use std::path::PathBuf;
use std::sync::Arc;
use streamforge::config::Config;
use tracing::{error, info};

/// Submit a new job
pub async fn submit_job(
    config_path: PathBuf,
    name: Option<String>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    info!("Submitting job from config: {:?}", config_path);

    // Load and validate config
    let config = load_and_validate_config(&config_path).await?;

    // Determine job name
    let job_name = name.unwrap_or_else(|| {
        config_path
            .file_stem()
            .and_then(|s| s.to_str())
            .unwrap_or("unnamed")
            .to_string()
    });

    // Create job manager
    let manager = Arc::new(JobManager::new().await?);

    // Submit job
    let job_id = manager.submit_job(job_name, config).await?;

    // Start job execution
    let executor = JobExecutor::new(Arc::clone(&manager));
    if let Err(e) = executor.start_job(&job_id).await {
        error!("Failed to start job: {}", e);
        return Err(e);
    }

    println!("✅ Job submitted and started successfully!");
    println!("   Job ID: {}", job_id);
    println!("   Use 'streamforge status {}' to check status", job_id);

    Ok(())
}

/// List all jobs
pub async fn list_jobs(verbose: bool) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let manager = JobManager::new().await?;
    let jobs = manager.list_jobs().await?;

    if jobs.is_empty() {
        println!("No jobs found.");
        return Ok(());
    }

    if verbose {
        println!(
            "{:<36} {:<20} {:<15} {:<20}",
            "JOB ID", "NAME", "STATUS", "SUBMITTED"
        );
        println!("{}", "-".repeat(91));
        for job in jobs {
            println!(
                "{:<36} {:<20} {:<15} {:<20}",
                job.id,
                job.name,
                format!("{:?}", job.status),
                format_time(job.submitted_at)
            );
        }
    } else {
        println!("{:<36} {:<20} {:<15}", "JOB ID", "NAME", "STATUS");
        println!("{}", "-".repeat(71));
        for job in jobs {
            println!(
                "{:<36} {:<20} {:<15}",
                job.id,
                job.name,
                format!("{:?}", job.status)
            );
        }
    }

    Ok(())
}

/// Show job status
pub async fn show_status(job_id: &str) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let manager = JobManager::new().await?;
    let job = manager.get_job(job_id).await?;

    match job {
        Some(job) => {
            println!("Job: {}", job.name);
            println!("  ID: {}", job.id);
            println!("  Status: {:?}", job.status);
            println!("  Submitted: {}", format_time(job.submitted_at));
            if let Some(started_at) = job.started_at {
                println!("  Started: {}", format_time(started_at));
            }
            if let Some(stopped_at) = job.stopped_at {
                println!("  Stopped: {}", format_time(stopped_at));
            }
        }
        None => {
            println!("Job not found: {}", job_id);
            return Err("Job not found".into());
        }
    }

    Ok(())
}

/// Stop a job
pub async fn stop_job(
    job_id: &str,
    force: bool,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    info!("Stopping job: {} (force: {})", job_id, force);

    let manager = Arc::new(JobManager::new().await?);
    let executor = JobExecutor::new(Arc::clone(&manager));

    // Stop execution
    executor.stop_job(job_id, force).await?;

    // Update job status
    manager.stop_job(job_id, force).await?;

    println!("✅ Job stopped: {}", job_id);

    Ok(())
}

/// Validate a configuration file
pub async fn validate_config(
    config_path: PathBuf,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    println!("Validating configuration: {:?}", config_path);

    match load_and_validate_config(&config_path).await {
        Ok(config) => {
            println!("✅ Configuration is valid!");
            println!(
                "   Processing parallelism: {}",
                config.processing.parallelism
            );
            println!("   Cluster bind address: {:?}", config.cluster.bind_address);
            println!("   State backend: {:?}", config.state.backend_type);
            Ok(())
        }
        Err(e) => {
            error!("Configuration validation failed: {}", e);
            Err(e)
        }
    }
}

/// Show cluster status
pub async fn cluster_status() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    println!("Cluster Status:");
    println!("  (Cluster status functionality coming soon)");
    Ok(())
}

/// Show cluster nodes
pub async fn cluster_nodes() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    println!("Cluster Nodes:");
    println!("  (Cluster nodes functionality coming soon)");
    Ok(())
}

/// Load and validate a configuration file
async fn load_and_validate_config(
    path: &PathBuf,
) -> Result<Config, Box<dyn std::error::Error + Send + Sync>> {
    // Read file
    let content = tokio::fs::read_to_string(path).await?;

    // Parse TOML
    let config: Config =
        toml::from_str(&content).map_err(|e| format!("Failed to parse config: {}", e))?;

    // Validate
    validate_config_values(&config)?;

    Ok(config)
}

/// Validate configuration values
fn validate_config_values(config: &Config) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // Validate parallelism
    if config.processing.parallelism == 0 {
        return Err("Processing parallelism must be > 0".into());
    }

    // Validate buffer size
    if config.processing.buffer_size == 0 {
        return Err("Buffer size must be > 0".into());
    }

    // Validate batch size
    if config.processing.batch_size == 0 {
        return Err("Batch size must be > 0".into());
    }

    Ok(())
}

/// Format timestamp for display
fn format_time(timestamp: u64) -> String {
    use std::time::SystemTime;
    let datetime = SystemTime::UNIX_EPOCH + std::time::Duration::from_secs(timestamp);
    format!("{:?}", datetime)
}
