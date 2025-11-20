//! Metrics HTTP server for exposing metrics

use crate::metrics::collector::MetricsCollector;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{info, warn};

/// Metrics server for exposing metrics via HTTP
pub struct MetricsServer {
    collectors: Arc<RwLock<HashMap<String, Arc<MetricsCollector>>>>,
    bind_address: SocketAddr,
}

impl MetricsServer {
    /// Create a new metrics server
    pub fn new(bind_address: SocketAddr) -> Self {
        Self {
            collectors: Arc::new(RwLock::new(HashMap::new())),
            bind_address,
        }
    }

    /// Register a metrics collector
    pub async fn register_collector(&self, name: String, collector: Arc<MetricsCollector>) {
        let mut collectors = self.collectors.write().await;
        collectors.insert(name, collector);
    }

    /// Start the metrics server
    pub async fn start(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let collectors = Arc::clone(&self.collectors);
        let addr = self.bind_address;

        let server = tokio::spawn(async move {
            use tokio::net::TcpListener;
            use tokio::io::{AsyncReadExt, AsyncWriteExt};

            let listener = match TcpListener::bind(addr).await {
                Ok(l) => l,
                Err(e) => {
                    warn!("Failed to bind metrics server: {}", e);
                    return;
                }
            };

            info!("Metrics server listening on {}", addr);

            loop {
                match listener.accept().await {
                    Ok((mut stream, peer_addr)) => {
                        let collectors = Arc::clone(&collectors);
                        tokio::spawn(async move {
                            let mut buffer = [0; 1024];
                            if let Ok(n) = stream.read(&mut buffer).await {
                                let request = String::from_utf8_lossy(&buffer[..n]);
                                
                                let response = if request.starts_with("GET /metrics") {
                                    Self::handle_metrics(&collectors).await
                                } else if request.starts_with("GET /health") {
                                    Self::handle_health().await
                                } else {
                                    "HTTP/1.1 404 Not Found\r\n\r\n".to_string()
                                };

                                if let Err(e) = stream.write_all(response.as_bytes()).await {
                                    warn!("Error writing response to {}: {}", peer_addr, e);
                                }
                            }
                        });
                    }
                    Err(e) => {
                        warn!("Error accepting connection: {}", e);
                    }
                }
            }
        });

        // Don't await - let it run in background
        Ok(())
    }

    async fn handle_metrics(collectors: &Arc<RwLock<HashMap<String, Arc<MetricsCollector>>>>) -> String {
        let collectors_guard = collectors.read().await;
        let mut metrics = String::new();

        for (_, collector) in collectors_guard.iter() {
            let snapshot = collector.snapshot();
            metrics.push_str(&snapshot.to_prometheus());
            metrics.push('\n');
        }

        format!(
            "HTTP/1.1 200 OK\r\n\
             Content-Type: text/plain; version=0.0.4\r\n\
             Content-Length: {}\r\n\r\n{}",
            metrics.len(),
            metrics
        )
    }

    async fn handle_health() -> String {
        let health = serde_json::json!({
            "status": "healthy",
            "timestamp": chrono::Utc::now().to_rfc3339(),
        });

        let body = serde_json::to_string_pretty(&health).unwrap();
        format!(
            "HTTP/1.1 200 OK\r\n\
             Content-Type: application/json\r\n\
             Content-Length: {}\r\n\r\n{}",
            body.len(),
            body
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[tokio::test]
    async fn test_metrics_server() {
        let server = MetricsServer::new("127.0.0.1:0".parse().unwrap());
        let collector = Arc::new(MetricsCollector::new("test"));
        collector.record_event(100);
        
        server.register_collector("test".to_string(), collector).await;
        
        // Server would need to be started to test fully
        // This is a basic structure test
    }
}

