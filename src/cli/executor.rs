//! Job execution engine

use crate::cli::job::JobManager;
use std::sync::Arc;
use streamforge::config::Config;
use tokio::sync::RwLock;
use tokio::task::JoinHandle;
use tracing::{error, info, warn};

/// Job executor that runs jobs
pub struct JobExecutor {
    manager: Arc<JobManager>,
    running_jobs: Arc<RwLock<std::collections::HashMap<String, JoinHandle<()>>>>,
}

impl JobExecutor {
    /// Create a new job executor
    pub fn new(manager: Arc<JobManager>) -> Self {
        Self {
            manager,
            running_jobs: Arc::new(RwLock::new(std::collections::HashMap::new())),
        }
    }

    /// Start executing a job
    pub async fn start_job(
        &self,
        job_id: &str,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        // Check if already running
        {
            let running = self.running_jobs.read().await;
            if running.contains_key(job_id) {
                return Err(format!("Job {} is already running", job_id).into());
            }
        }

        // Get job and mark as running
        let (job_name, config) = {
            let mut jobs = self.manager.jobs.write().await;
            let job = jobs
                .get_mut(job_id)
                .ok_or_else(|| format!("Job not found: {}", job_id))?;

            job.start();
            let job_clone = job.clone();
            let name = job.name.clone();
            let cfg = job.config.clone();
            drop(jobs);

            // Save updated status
            self.manager.save_job(&job_clone).await?;

            (name, cfg)
        };

        info!("Starting job: {} ({})", &job_name, job_id);

        // Spawn job execution task
        let manager = Arc::clone(&self.manager);
        let job_id_clone = job_id.to_string();

        let handle = tokio::spawn(async move {
            // Execute the job
            match Self::execute_job(&job_name, &config).await {
                Ok(_) => {
                    info!("Job {} completed successfully", job_id_clone);
                    let mut jobs = manager.jobs.write().await;
                    if let Some(job) = jobs.get_mut(&job_id_clone) {
                        job.complete();
                        let job = job.clone();
                        drop(jobs);
                        let _ = manager.save_job(&job).await;
                    }
                }
                Err(e) => {
                    error!("Job {} failed: {}", job_id_clone, e);
                    let mut jobs = manager.jobs.write().await;
                    if let Some(job) = jobs.get_mut(&job_id_clone) {
                        job.fail(e.to_string());
                        let job = job.clone();
                        drop(jobs);
                        let _ = manager.save_job(&job).await;
                    }
                }
            }
        });

        // Store handle
        {
            let mut running = self.running_jobs.write().await;
            running.insert(job_id.to_string(), handle);
        }

        Ok(())
    }

    /// Stop a running job
    pub async fn stop_job(
        &self,
        job_id: &str,
        force: bool,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        // Remove from running jobs
        let handle = {
            let mut running = self.running_jobs.write().await;
            running.remove(job_id)
        };

        if let Some(handle) = handle {
            if force {
                handle.abort();
            } else {
                // Graceful shutdown - for now just abort
                handle.abort();
            }
        }

        // Update job status
        {
            let mut jobs = self.manager.jobs.write().await;
            if let Some(job) = jobs.get_mut(job_id) {
                job.stop();
                let job = job.clone();
                drop(jobs);
                self.manager.save_job(&job).await?;
            }
        }

        Ok(())
    }

    /// Execute a job (placeholder implementation)
    async fn execute_job(
        name: &str,
        config: &Config,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        info!(
            "Executing job: {} with parallelism: {}",
            name, config.processing.parallelism
        );

        // TODO: Implement actual job execution
        // For now, this is a placeholder that simulates work
        // In a real implementation, this would:
        // 1. Initialize cluster/node based on config
        // 2. Create sources from job definition
        // 3. Build processing pipeline
        // 4. Execute pipeline
        // 5. Handle shutdown signals

        warn!("Job execution is a placeholder - actual pipeline execution not yet implemented");
        warn!(
            "Job configuration loaded: parallelism={}, backend={:?}",
            config.processing.parallelism, config.state.backend_type
        );

        // Simulate some work
        tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;

        // For now, just succeed
        Ok(())
    }

    /// Check if a job is running
    #[allow(dead_code)]
    pub async fn is_running(&self, job_id: &str) -> bool {
        let running = self.running_jobs.read().await;
        running.contains_key(job_id)
    }
}
