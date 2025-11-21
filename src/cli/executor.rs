//! Job execution engine

use crate::cli::job::JobManager;
use crate::cli::offset_tracker::OffsetTracker;
use crate::cli::sink_tracker::SinkTracker;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;
use streamforge::config::Config;
use streamforge::execution::Stream;
use streamforge::sinks::file::FileSink;
use streamforge::sources::{file::FileSource, http::HttpSource};
use tokio::signal;
use tokio::sync::RwLock;
use tokio::task::JoinHandle;
use tokio::time::sleep;
use tracing::{error, info, warn};

/// Job executor that runs jobs
pub struct JobExecutor {
    manager: Arc<JobManager>,
    running_jobs: Arc<RwLock<std::collections::HashMap<String, JoinHandle<()>>>>,
    /// Whether to run in daemon mode (restart failed jobs)
    daemon_mode: bool,
    /// Maximum restart attempts for failed jobs
    max_restarts: u32,
    /// Delay between restart attempts
    restart_delay: Duration,
}

impl JobExecutor {
    /// Create a new job executor
    pub fn new(manager: Arc<JobManager>) -> Self {
        Self {
            manager,
            running_jobs: Arc::new(RwLock::new(std::collections::HashMap::new())),
            daemon_mode: false,
            max_restarts: 3,
            restart_delay: Duration::from_secs(5),
        }
    }

    /// Create a new job executor with daemon mode enabled
    pub fn new_daemon(manager: Arc<JobManager>, max_restarts: u32, restart_delay: Duration) -> Self {
        Self {
            manager,
            running_jobs: Arc::new(RwLock::new(std::collections::HashMap::new())),
            daemon_mode: true,
            max_restarts,
            restart_delay,
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
        let daemon_mode = self.daemon_mode;
        let max_restarts = self.max_restarts;
        let restart_delay = self.restart_delay;

        let handle = tokio::spawn(async move {
            let mut restart_count = 0;
            
            loop {
                // Execute the job
                match Self::execute_job(&job_name, &config, &job_id_clone).await {
                    Ok(_) => {
                        info!("Job {} completed successfully", job_id_clone);
                        let mut jobs = manager.jobs.write().await;
                        if let Some(job) = jobs.get_mut(&job_id_clone) {
                            job.complete();
                            let job = job.clone();
                            drop(jobs);
                            let _ = manager.save_job(&job).await;
                        }
                        break; // Job completed successfully, exit loop
                    }
                    Err(e) => {
                        error!("Job {} failed: {}", job_id_clone, e);
                        restart_count += 1;
                        
                        // Update job status
                        {
                            let mut jobs = manager.jobs.write().await;
                            if let Some(job) = jobs.get_mut(&job_id_clone) {
                                job.fail(format!("{} (restart {}/{})", e, restart_count, max_restarts));
                                let job = job.clone();
                                drop(jobs);
                                let _ = manager.save_job(&job).await;
                            }
                        }
                        
                        // Check if we should restart
                        if daemon_mode && restart_count <= max_restarts {
                            warn!(
                                "Job {} failed, restarting in {:?} (attempt {}/{})",
                                job_id_clone, restart_delay, restart_count, max_restarts
                            );
                            sleep(restart_delay).await;
                            
                            // Reset job status for restart
                            {
                                let mut jobs = manager.jobs.write().await;
                                if let Some(job) = jobs.get_mut(&job_id_clone) {
                                    job.start(); // Mark as running again
                                    let job = job.clone();
                                    drop(jobs);
                                    let _ = manager.save_job(&job).await;
                                }
                            }
                            
                            // Continue loop to restart
                            continue;
                        } else {
                            // Max restarts reached or not in daemon mode
                            if daemon_mode {
                                error!(
                                    "Job {} exceeded max restarts ({}), giving up",
                                    job_id_clone, max_restarts
                                );
                            }
                            break; // Exit loop
                        }
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

    /// Execute a job
    async fn execute_job(
        name: &str,
        config: &Config,
        job_id: &str,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        info!(
            "Executing job: {} (ID: {}) with parallelism: {}",
            name, job_id, config.processing.parallelism
        );

        // Check if job definition exists
        let job_def = config
            .job
            .as_ref()
            .ok_or_else(|| "Job definition not found in config".to_string())?;

        // Setup offset and sink tracking
        let storage_dir = dirs::data_dir()
            .unwrap_or_else(|| PathBuf::from("/tmp"))
            .join("streamforge")
            .join("jobs");
        let offset_tracker = OffsetTracker::new(job_id, &storage_dir).await?;
        let sink_tracker = SinkTracker::new(job_id, &storage_dir).await?;

        // Setup shutdown signal handler (cross-platform)
        let shutdown = Arc::new(AtomicBool::new(false));
        let shutdown_clone = Arc::clone(&shutdown);

        // Spawn signal handler task
        tokio::spawn(async move {
            #[cfg(unix)]
            {
                use signal::unix::{signal, SignalKind};
                let mut sigterm = signal(SignalKind::terminate())
                    .unwrap_or_else(|_| signal(SignalKind::interrupt()).unwrap());
                let mut sigint = signal(SignalKind::interrupt()).ok();

                tokio::select! {
                    _ = sigterm.recv() => {
                        info!("Received SIGTERM, shutting down gracefully...");
                        shutdown_clone.store(true, Ordering::Relaxed);
                    }
                    _ = async {
                        if let Some(ref mut sigint) = sigint {
                            sigint.recv().await
                        } else {
                            std::future::pending().await
                        }
                    } => {
                        info!("Received SIGINT, shutting down gracefully...");
                        shutdown_clone.store(true, Ordering::Relaxed);
                    }
                }
            }
            #[cfg(not(unix))]
            {
                // On Windows, use Ctrl+C handler
                signal::ctrl_c().await.ok();
                info!("Received Ctrl+C, shutting down gracefully...");
                shutdown_clone.store(true, Ordering::Relaxed);
            }
        });

        // Create source from config with offset tracking
        info!("Creating source from job definition...");
        let source_id = Self::get_source_id(&job_def.source);
        let last_offset = offset_tracker.get_offset(&source_id).await;
        if let Some(offset) = last_offset {
            info!("Resuming from offset: {}", offset);
        }
        let mut stream = Self::create_stream_from_source(&job_def.source, last_offset)?;

        // Apply SQL query if provided, otherwise use operators
        if let Some(ref sql_query) = job_def.sql {
            info!("Executing SQL query: {}", sql_query);
            let query = streamforge::query::SqlParser::parse(sql_query)
                .map_err(|e| format!("SQL parse error: {}", e))?;
            
            // Check if query has aggregations (requires batch processing)
            let has_aggregations = query.aggregations.is_some() || query.group_by.is_some();
            let executor = streamforge::query::QueryExecutor::new(query);
            
            if has_aggregations {
                // Collect events for batch processing
                let events: Vec<streamforge::core::Event> = stream.collect().await
                    .map_err(|e| format!("Failed to collect events: {}", e))?;
                
                // Execute with aggregations
                let results = executor.execute_with_aggregations(events).await
                    .map_err(|e| format!("Query execution error: {}", e))?;
                
                // Convert results back to stream
                stream = streamforge::execution::Stream::from_iter(results);
            } else {
                // Execute as streaming query
                stream = executor.execute_stream(stream).await
                    .map_err(|e| format!("Query execution error: {}", e))?;
            }
        } else {
            // Apply operators if any
            for (idx, op_config) in job_def.operators.iter().enumerate() {
                info!("Applying operator {}: {:?}", idx, op_config);
                // For now, operators are placeholders - in a full implementation,
                // we would parse the expressions and apply them
                match op_config {
                    streamforge::config::OperatorConfig::Filter { .. } => {
                        // Placeholder: would apply filter based on expression
                        warn!("Filter operator not yet implemented, skipping");
                    }
                    streamforge::config::OperatorConfig::Map { .. } => {
                        // Placeholder: would apply map based on expression
                        warn!("Map operator not yet implemented, skipping");
                    }
                }
            }
        }

        // Create sink from config and execute pipeline with tracking
        info!("Creating sink from job definition...");
        let sink_id = Self::get_sink_id(&job_def.sink);
        info!("Starting pipeline execution...");
        
        // Track events processed
        let mut events_processed = 0u64;
        let mut current_offset = last_offset.unwrap_or(0);
        
        // Execute pipeline with offset and write tracking
        let result = Self::execute_pipeline_with_tracking(
            stream,
            &job_def.sink,
            &offset_tracker,
            &sink_tracker,
            &source_id,
            &sink_id,
            &mut events_processed,
            &mut current_offset,
        ).await;

        if shutdown.load(Ordering::Relaxed) {
            info!("Job {} stopped due to shutdown signal", name);
            return Ok(());
        }

        match result {
            Ok(_) => {
                info!("Job {} completed successfully", name);
                Ok(())
            }
            Err(e) => {
                error!("Job {} failed: {}", name, e);
                Err(format!("Pipeline execution failed: {}", e).into())
            }
        }
    }

    /// Get source ID for tracking
    fn get_source_id(source_config: &streamforge::config::SourceConfig) -> String {
        match source_config {
            streamforge::config::SourceConfig::File { path, .. } => format!("file:{}", path),
            streamforge::config::SourceConfig::Http { urls, .. } => format!("http:{}", urls.join(",")),
        }
    }

    /// Get sink ID for tracking
    fn get_sink_id(sink_config: &streamforge::config::SinkConfig) -> String {
        match sink_config {
            streamforge::config::SinkConfig::File { path, .. } => format!("file:{}", path),
        }
    }

    /// Create a stream from source configuration with optional offset
    fn create_stream_from_source(
        source_config: &streamforge::config::SourceConfig,
        _start_offset: Option<u64>,
    ) -> Result<Stream, Box<dyn std::error::Error + Send + Sync>> {
        match source_config {
            streamforge::config::SourceConfig::File { path, format, follow } => {
                let config = streamforge::sources::file::FileSourceConfig {
                    path: path.clone(),
                    format: format.clone(),
                    follow: *follow,
                };
                let source = FileSource::new(config)?;
                Ok(Stream::from_source(source))
            }
            streamforge::config::SourceConfig::Http {
                urls,
                poll_interval,
                timeout,
                value_path,
                key_path,
            } => {
                let config = streamforge::sources::http::HttpSourceConfig {
                    urls: urls.clone(),
                    poll_interval: std::time::Duration::from_secs(*poll_interval),
                    timeout: std::time::Duration::from_secs(*timeout),
                    max_retries: 3,
                    retry_delay: std::time::Duration::from_millis(500),
                    value_path: value_path.clone(),
                    key_path: key_path.clone(),
                    headers: Vec::new(),
                };
                let source = HttpSource::new(config)?;
                Ok(Stream::from_source(source))
            }
        }
    }

    /// Execute pipeline with sink
    async fn execute_pipeline(
        stream: Stream,
        sink_config: &streamforge::config::SinkConfig,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        match sink_config {
            streamforge::config::SinkConfig::File { path, format, append } => {
                let config = streamforge::sinks::file::FileSinkConfig {
                    path: path.clone(),
                    format: format.clone(),
                    append: *append,
                };
                let sink = FileSink::new(config)?;
                stream.sink(sink).await.map_err(|e| format!("Pipeline execution failed: {}", e).into())
            }
        }
    }

    /// Execute pipeline with offset and write tracking
    #[allow(clippy::too_many_arguments)]
    async fn execute_pipeline_with_tracking(
        stream: Stream,
        sink_config: &streamforge::config::SinkConfig,
        offset_tracker: &OffsetTracker,
        sink_tracker: &SinkTracker,
        source_id: &str,
        sink_id: &str,
        events_processed: &mut u64,
        current_offset: &mut u64,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        // For now, use the simple pipeline execution
        // TODO: Implement proper event-by-event tracking
        // This requires extending Stream to support tracking callbacks
        
        let result = Self::execute_pipeline(stream, sink_config).await;
        
        // Update tracking after pipeline completes
        // In a full implementation, we'd track during processing
        if result.is_ok() {
            offset_tracker.update_offset(source_id, *current_offset).await?;
            let transaction_id = Some(format!("tx-{}", *events_processed));
            sink_tracker.update_write(sink_id, *events_processed, transaction_id, *events_processed).await?;
        }
        
        result
    }

    /// Check if a job is running
    #[allow(dead_code)]
    pub async fn is_running(&self, job_id: &str) -> bool {
        let running = self.running_jobs.read().await;
        running.contains_key(job_id)
    }
}
