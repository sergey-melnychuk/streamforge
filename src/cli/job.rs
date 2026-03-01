//! Job management infrastructure

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use streamforge::config::Config;
use tokio::sync::RwLock;
use uuid::Uuid;

/// Job status
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum JobStatus {
    /// Job is queued for execution
    Queued,
    /// Job is running
    Running,
    /// Job completed successfully
    Completed,
    /// Job failed
    Failed,
    /// Job was stopped
    Stopped,
}

impl std::fmt::Display for JobStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            JobStatus::Queued => write!(f, "Queued"),
            JobStatus::Running => write!(f, "Running"),
            JobStatus::Completed => write!(f, "Completed"),
            JobStatus::Failed => write!(f, "Failed"),
            JobStatus::Stopped => write!(f, "Stopped"),
        }
    }
}

/// Job metadata
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Job {
    /// Unique job ID
    pub id: String,
    /// Job name
    pub name: String,
    /// Job status
    pub status: JobStatus,
    /// Job configuration
    pub config: Config,
    /// When the job was submitted
    pub submitted_at: u64,
    /// When the job started (if running/completed)
    pub started_at: Option<u64>,
    /// When the job stopped (if stopped/completed/failed)
    pub stopped_at: Option<u64>,
    /// Error message (if failed)
    pub error: Option<String>,
}

impl Job {
    /// Create a new job
    pub fn new(name: String, config: Config) -> Self {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();

        Self {
            id: Uuid::new_v4().to_string(),
            name,
            status: JobStatus::Queued,
            config,
            submitted_at: now,
            started_at: None,
            stopped_at: None,
            error: None,
        }
    }

    /// Mark job as started
    pub fn start(&mut self) {
        self.status = JobStatus::Running;
        self.started_at = Some(
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_secs(),
        );
    }

    /// Mark job as completed
    pub fn complete(&mut self) {
        self.status = JobStatus::Completed;
        self.stopped_at = Some(
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_secs(),
        );
    }

    /// Mark job as failed
    pub fn fail(&mut self, error: String) {
        self.status = JobStatus::Failed;
        self.stopped_at = Some(
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_secs(),
        );
        self.error = Some(error);
    }

    /// Mark job as stopped
    pub fn stop(&mut self) {
        self.status = JobStatus::Stopped;
        self.stopped_at = Some(
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_secs(),
        );
    }
}

/// Job manager for tracking and managing jobs
pub struct JobManager {
    /// Storage directory for job metadata
    pub(crate) storage_dir: PathBuf,
    /// In-memory job registry
    pub(crate) jobs: Arc<RwLock<HashMap<String, Job>>>,
}

impl JobManager {
    /// Create a new job manager
    pub async fn new() -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        // Use default storage directory (overridable via STREAMFORGE_DATA_DIR)
        let storage_dir = std::env::var("STREAMFORGE_DATA_DIR")
            .map(PathBuf::from)
            .unwrap_or_else(|_| {
                dirs::data_dir()
                    .unwrap_or_else(|| PathBuf::from("/tmp"))
                    .join("streamforge")
            })
            .join("jobs");

        // Create directory if it doesn't exist
        tokio::fs::create_dir_all(&storage_dir).await?;

        let manager = Self {
            storage_dir,
            jobs: Arc::new(RwLock::new(HashMap::new())),
        };

        // Load existing jobs from disk
        manager.load_jobs().await?;

        Ok(manager)
    }

    /// Submit a new job
    pub async fn submit_job(
        &self,
        name: String,
        config: Config,
    ) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
        // Check if a job with the same name is already running
        let jobs = self.jobs.read().await;
        for (existing_id, existing_job) in jobs.iter() {
            if existing_job.name == name {
                use crate::cli::job::JobStatus;
                if matches!(existing_job.status, JobStatus::Running) {
                    return Err(format!(
                        "Job with name '{}' is already running (ID: {})",
                        name, existing_id
                    )
                    .into());
                }
            }
        }
        drop(jobs);

        let job = Job::new(name, config);
        let job_id = job.id.clone();

        // Store job
        {
            let mut jobs = self.jobs.write().await;
            jobs.insert(job_id.clone(), job.clone());
        }

        // Persist to disk
        self.save_job(&job).await?;

        Ok(job_id)
    }

    /// Get a job by ID
    pub async fn get_job(
        &self,
        job_id: &str,
    ) -> Result<Option<Job>, Box<dyn std::error::Error + Send + Sync>> {
        let jobs = self.jobs.read().await;
        Ok(jobs.get(job_id).cloned())
    }

    /// List all jobs
    pub async fn list_jobs(&self) -> Result<Vec<Job>, Box<dyn std::error::Error + Send + Sync>> {
        let jobs = self.jobs.read().await;
        Ok(jobs.values().cloned().collect())
    }

    /// Stop a job
    #[allow(dead_code)]
    pub async fn stop_job(
        &self,
        job_id: &str,
        _force: bool,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let mut jobs = self.jobs.write().await;
        if let Some(job) = jobs.get_mut(job_id) {
            job.stop();
            let job = job.clone();
            drop(jobs);

            // Persist updated status
            self.save_job(&job).await?;
            Ok(())
        } else {
            Err(format!("Job not found: {}", job_id).into())
        }
    }

    /// Save a job to disk
    pub(crate) async fn save_job(
        &self,
        job: &Job,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let path = self.storage_dir.join(format!("{}.json", job.id));
        let content = serde_json::to_string_pretty(job)?;
        tokio::fs::write(&path, content).await?;
        Ok(())
    }

    /// Load jobs from disk
    async fn load_jobs(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let mut entries = tokio::fs::read_dir(&self.storage_dir).await?;
        let mut jobs = self.jobs.write().await;

        while let Some(entry) = entries.next_entry().await? {
            if entry.path().extension().and_then(|s| s.to_str()) == Some("json") {
                if let Ok(content) = tokio::fs::read_to_string(entry.path()).await {
                    if let Ok(job) = serde_json::from_str::<Job>(&content) {
                        jobs.insert(job.id.clone(), job);
                    }
                }
            }
        }

        Ok(())
    }
}
