//! Raft consensus protocol implementation
//!
//! Provides leader election and log replication for cluster coordination

use crate::distributed::node::NodeId;
use crate::network::rpc::RpcClient;
use crate::network::transport::Transport;
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{mpsc, RwLock};
use tokio::time::{sleep, Instant};
use tracing::{debug, info};

/// Raft node role
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum RaftRole {
    /// Follower: receives log entries from leader
    Follower,
    /// Candidate: seeking votes for leadership
    Candidate,
    /// Leader: handles client requests and replicates log
    Leader,
}

/// Raft log entry
#[derive(Debug, Clone)]
pub struct LogEntry {
    /// Term when entry was created
    pub term: u64,
    /// Entry index
    pub index: u64,
    /// Entry data
    pub data: Bytes,
}

// Serialization helper for LogEntry
#[derive(Serialize, Deserialize)]
struct LogEntrySer {
    term: u64,
    index: u64,
    data: Vec<u8>,
}

impl Serialize for LogEntry {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        LogEntrySer {
            term: self.term,
            index: self.index,
            data: self.data.to_vec(),
        }.serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for LogEntry {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let ser = LogEntrySer::deserialize(deserializer)?;
        Ok(LogEntry {
            term: ser.term,
            index: ser.index,
            data: Bytes::from(ser.data),
        })
    }
}

/// Raft node state
#[derive(Debug)]
pub struct RaftState {
    /// Current term (incremented on election)
    pub current_term: u64,
    /// Node that received vote in current term (or None)
    pub voted_for: Option<NodeId>,
    /// Log entries
    pub log: Vec<LogEntry>,
    /// Index of highest log entry known to be committed
    pub commit_index: u64,
    /// Index of highest log entry applied to state machine
    pub last_applied: u64,
    /// Role (Follower, Candidate, Leader)
    pub role: RaftRole,
    /// Leader ID (if known)
    pub leader_id: Option<NodeId>,
}

/// RequestVote RPC arguments
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RequestVoteArgs {
    /// Candidate's term
    pub term: u64,
    /// Candidate requesting vote
    pub candidate_id: NodeId,
    /// Index of candidate's last log entry
    pub last_log_index: u64,
    /// Term of candidate's last log entry
    pub last_log_term: u64,
}

/// RequestVote RPC result
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RequestVoteResult {
    /// Current term, for candidate to update itself
    pub term: u64,
    /// True means candidate received vote
    pub vote_granted: bool,
}

/// AppendEntries RPC arguments
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AppendEntriesArgs {
    /// Leader's term
    pub term: u64,
    /// Leader ID
    pub leader_id: NodeId,
    /// Index of log entry immediately preceding new ones
    pub prev_log_index: u64,
    /// Term of prev_log_index entry
    pub prev_log_term: u64,
    /// Log entries to store (empty for heartbeat)
    pub entries: Vec<LogEntry>,
    /// Leader's commit_index
    pub leader_commit: u64,
}

/// AppendEntries RPC result
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AppendEntriesResult {
    /// Current term, for leader to update itself
    pub term: u64,
    /// True if follower contained entry matching prev_log_index and prev_log_term
    pub success: bool,
}

/// Error type for Raft operations
#[derive(Debug, thiserror::Error)]
pub enum RaftError {
    #[error("Not the leader")]
    NotLeader,
    #[error("Term mismatch: current={}, received={}", .0, .1)]
    TermMismatch(u64, u64),
    #[error("Raft error: {0}")]
    Other(String),
}

/// Result type for Raft operations
pub type RaftResult<T> = Result<T, RaftError>;

/// Raft consensus implementation
pub struct Raft {
    /// Local node ID
    local_id: NodeId,
    /// Raft state
    state: Arc<RwLock<RaftState>>,
    /// Transport for network communication
    transport: Arc<Transport>,
    /// RPC client
    rpc_client: Arc<RpcClient>,
    /// Known cluster nodes (node_id -> address)
    nodes: Arc<RwLock<HashMap<NodeId, SocketAddr>>>,
    /// Configuration
    config: RaftConfig,
    /// Running flag
    running: Arc<std::sync::atomic::AtomicBool>,
    /// Event channel for leadership changes
    event_tx: mpsc::Sender<RaftEvent>,
    event_rx: Option<mpsc::Receiver<RaftEvent>>,
}

/// Raft events
#[derive(Debug, Clone)]
pub enum RaftEvent {
    /// Leader was elected
    LeaderElected { term: u64, leader_id: NodeId },
    /// Leadership was lost
    LeadershipLost { term: u64 },
    /// New term started
    NewTerm { term: u64 },
}

/// Raft configuration
#[derive(Debug, Clone)]
pub struct RaftConfig {
    /// Election timeout (randomized between min and max)
    pub election_timeout_min: Duration,
    pub election_timeout_max: Duration,
    /// Heartbeat interval (leader sends heartbeats)
    pub heartbeat_interval: Duration,
}

impl Default for RaftConfig {
    fn default() -> Self {
        Self {
            election_timeout_min: Duration::from_millis(150),
            election_timeout_max: Duration::from_millis(300),
            heartbeat_interval: Duration::from_millis(50),
        }
    }
}

impl Raft {
    /// Create a new Raft instance
    pub fn new(
        local_id: NodeId,
        transport: Arc<Transport>,
        config: RaftConfig,
    ) -> Self {
        let (event_tx, event_rx) = mpsc::channel(100);
        let rpc_client = Arc::new(RpcClient::new((*transport).clone()));

        let state = Arc::new(RwLock::new(RaftState {
            current_term: 0,
            voted_for: None,
            log: Vec::new(),
            commit_index: 0,
            last_applied: 0,
            role: RaftRole::Follower,
            leader_id: None,
        }));

        Self {
            local_id,
            state,
            transport,
            rpc_client,
            nodes: Arc::new(RwLock::new(HashMap::new())),
            config,
            running: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            event_tx,
            event_rx: Some(event_rx),
        }
    }

    /// Add a node to the cluster
    pub async fn add_node(&self, node_id: NodeId, address: SocketAddr) {
        let mut nodes = self.nodes.write().await;
        nodes.insert(node_id, address);
    }

    /// Get current state
    pub async fn get_state(&self) -> RaftState {
        self.state.read().await.clone()
    }

    /// Check if this node is the leader
    pub async fn is_leader(&self) -> bool {
        let state = self.state.read().await;
        state.role == RaftRole::Leader
    }

    /// Get the current leader ID
    pub async fn get_leader(&self) -> Option<NodeId> {
        let state = self.state.read().await;
        state.leader_id
    }

    /// Start the Raft node
    pub async fn start(&self) -> RaftResult<()> {
        self.running.store(true, std::sync::atomic::Ordering::Relaxed);
        info!("Raft node {} started", self.local_id);

        // Start as follower
        {
            let mut state = self.state.write().await;
            state.role = RaftRole::Follower;
        }

        // Start main loop
        let raft = self.clone();
        tokio::spawn(async move {
            raft.run().await;
        });

        Ok(())
    }

    /// Stop the Raft node
    pub async fn stop(&self) {
        self.running.store(false, std::sync::atomic::Ordering::Relaxed);
    }

    /// Main Raft loop
    async fn run(&self) {
        while self.running.load(std::sync::atomic::Ordering::Relaxed) {
            let role = {
                let state = self.state.read().await;
                state.role
            };

            match role {
                RaftRole::Follower => self.run_follower().await,
                RaftRole::Candidate => self.run_candidate().await,
                RaftRole::Leader => self.run_leader().await,
            }
        }
    }

    /// Run as follower
    async fn run_follower(&self) {
        let timeout = self.random_election_timeout();
        let deadline = Instant::now() + timeout;

        while Instant::now() < deadline {
            if !self.running.load(std::sync::atomic::Ordering::Relaxed) {
                return;
            }

            // Wait for heartbeat or timeout
            sleep(Duration::from_millis(10)).await;

            // Check if we received a heartbeat (simplified - in real impl, would check actual messages)
            let state = self.state.read().await;
            if state.leader_id.is_some() {
                // Reset timeout if we have a leader
                return;
            }
        }

        // Timeout: become candidate
        debug!("Election timeout, becoming candidate");
        let mut state = self.state.write().await;
        state.role = RaftRole::Candidate;
        state.current_term += 1;
        state.voted_for = Some(self.local_id);
    }

    /// Run as candidate
    async fn run_candidate(&self) {
        let term = {
            let state = self.state.read().await;
            state.current_term
        };

        info!("Starting election for term {}", term);

        // Request votes from all nodes
        let nodes = self.nodes.read().await.clone();
        let mut votes = 1; // Vote for self
        let votes_needed = (nodes.len() + 1) / 2 + 1; // Majority

        let last_log_index;
        let _last_log_term;
        {
            let state = self.state.read().await;
            last_log_index = if state.log.is_empty() { 0 } else { state.log.len() as u64 - 1 };
            _last_log_term = if state.log.is_empty() { 0 } else { state.log[last_log_index as usize].term };
        }

        // Request votes from all nodes
        let mut vote_tasks = Vec::new();
        for (node_id, address) in nodes.iter() {
            if *node_id == self.local_id {
                continue;
            }

            let rpc_client = Arc::clone(&self.rpc_client);
            let local_id = self.local_id;
            let node_id = *node_id;
            let address = *address;
            let term = term;
            let last_log_index = last_log_index;
            let last_log_term = {
                let state = self.state.read().await;
                if state.log.is_empty() { 0 } else { state.log[last_log_index as usize].term }
            };

            let task = tokio::spawn(async move {
                let args = RequestVoteArgs {
                    term,
                    candidate_id: local_id,
                    last_log_index,
                    last_log_term,
                };

                let payload = bincode::serialize(&args)
                    .map_err(|e| format!("Serialization error: {}", e))?;

                match rpc_client.call(address, "raft_request_vote", payload).await {
                    Ok(response) => {
                        let result: RequestVoteResult = bincode::deserialize(&response)
                            .map_err(|e| format!("Deserialization error: {}", e))?;
                        Ok((node_id, result))
                    }
                    Err(e) => Err(format!("RPC error: {}", e)),
                }
            });

            vote_tasks.push(task);
        }

        // Collect votes
        for task in vote_tasks {
            if let Ok(Ok((_node_id, result))) = task.await {
                if result.vote_granted && result.term == term {
                    votes += 1;
                } else if result.term > term {
                    // Higher term seen, step down
                    let mut state = self.state.write().await;
                    state.current_term = result.term;
                    state.role = RaftRole::Follower;
                    state.voted_for = None;
                    return;
                }
            }
        }

        // Check if we got majority
        if votes >= votes_needed {
            // Become leader
            info!("Elected as leader for term {}", term);
            let mut state = self.state.write().await;
            state.role = RaftRole::Leader;
            state.leader_id = Some(self.local_id);

            let _ = self.event_tx.send(RaftEvent::LeaderElected {
                term,
                leader_id: self.local_id,
            }).await;
        } else {
            // Didn't get majority, go back to follower
            let mut state = self.state.write().await;
            state.role = RaftRole::Follower;
        }
    }

    /// Run as leader
    async fn run_leader(&self) {
        // Send heartbeats to all followers
        let nodes = self.nodes.read().await.clone();
        let (term, commit_index, prev_log_index, prev_log_term) = {
            let state = self.state.read().await;
            let prev_log_index = if state.log.is_empty() { 0 } else { state.log.len() as u64 };
            let prev_log_term = if state.log.is_empty() { 0 } else { 
                state.log[prev_log_index as usize - 1].term 
            };
            (state.current_term, state.commit_index, prev_log_index, prev_log_term)
        };

        // Send AppendEntries (heartbeat) to all followers
        for (node_id, address) in nodes.iter() {
            if *node_id == self.local_id {
                continue;
            }

            let rpc_client = Arc::clone(&self.rpc_client);
            let address = *address;
            let leader_id = self.local_id;
            let term = term;
            let prev_log_index = prev_log_index;
            let prev_log_term = prev_log_term;
            let commit_index = commit_index;

            tokio::spawn(async move {
                let args = AppendEntriesArgs {
                    term,
                    leader_id,
                    prev_log_index,
                    prev_log_term,
                    entries: Vec::new(), // Empty for heartbeat
                    leader_commit: commit_index,
                };

                let payload = match bincode::serialize(&args) {
                    Ok(p) => p,
                    Err(_) => return,
                };

                if let Ok(response) = rpc_client.call(address, "raft_append_entries", payload).await {
                    if let Ok(result) = bincode::deserialize::<AppendEntriesResult>(&response) {
                        if result.term > term {
                            // Higher term seen, would need to step down
                            // This is handled in the main loop
                        }
                    }
                }
            });
        }

        sleep(self.config.heartbeat_interval).await;

        // Check if we're still leader (could have been demoted)
        let state = self.state.read().await;
        if state.role != RaftRole::Leader || state.current_term != term {
            return;
        }
    }

    /// Generate random election timeout
    fn random_election_timeout(&self) -> Duration {
        use rand::Rng;
        let mut rng = rand::thread_rng();
        let min_ms = self.config.election_timeout_min.as_millis() as u64;
        let max_ms = self.config.election_timeout_max.as_millis() as u64;
        let timeout_ms = rng.gen_range(min_ms..=max_ms);
        Duration::from_millis(timeout_ms)
    }

    /// Handle RequestVote RPC
    pub async fn handle_request_vote(&self, args: RequestVoteArgs) -> RequestVoteResult {
        let mut state = self.state.write().await;

        // Reply false if term < currentTerm
        if args.term < state.current_term {
            return RequestVoteResult {
                term: state.current_term,
                vote_granted: false,
            };
        }

        // If RPC request contains term > currentTerm, set currentTerm = term, convert to follower
        if args.term > state.current_term {
            state.current_term = args.term;
            state.voted_for = None;
            state.role = RaftRole::Follower;
        }

        // If votedFor is null or candidateId, and candidate's log is at least as up-to-date as receiver's log, grant vote
        let vote_granted = (state.voted_for.is_none() || state.voted_for == Some(args.candidate_id))
            && self.is_log_up_to_date(&state, args.last_log_index, args.last_log_term);

        if vote_granted {
            state.voted_for = Some(args.candidate_id);
        }

        RequestVoteResult {
            term: state.current_term,
            vote_granted,
        }
    }

    /// Handle AppendEntries RPC
    pub async fn handle_append_entries(&self, args: AppendEntriesArgs) -> AppendEntriesResult {
        let mut state = self.state.write().await;

        // Reply false if term < currentTerm
        if args.term < state.current_term {
            return AppendEntriesResult {
                term: state.current_term,
                success: false,
            };
        }

        // If RPC request contains term > currentTerm, set currentTerm = term, convert to follower
        if args.term > state.current_term {
            state.current_term = args.term;
            state.voted_for = None;
            state.role = RaftRole::Follower;
        }

        // Recognize new leader
        state.leader_id = Some(args.leader_id);
        state.role = RaftRole::Follower;

        // Check if log matches
        let success = if args.prev_log_index == 0 || 
            (args.prev_log_index <= state.log.len() as u64 && 
             state.log[args.prev_log_index as usize - 1].term == args.prev_log_term) {
            // Append new entries
            if !args.entries.is_empty() {
                // Truncate log if necessary
                if args.prev_log_index < state.log.len() as u64 {
                    state.log.truncate(args.prev_log_index as usize);
                }
                state.log.extend(args.entries);
            }

            // Update commit index
            if args.leader_commit > state.commit_index {
                state.commit_index = std::cmp::min(args.leader_commit, state.log.len() as u64);
            }

            true
        } else {
            false
        };

        AppendEntriesResult {
            term: state.current_term,
            success,
        }
    }

    /// Check if candidate's log is at least as up-to-date as receiver's log
    fn is_log_up_to_date(&self, state: &RaftState, last_log_index: u64, last_log_term: u64) -> bool {
        if state.log.is_empty() {
            return true;
        }

        let our_last_term = state.log.last().unwrap().term;
        let our_last_index = state.log.len() as u64 - 1;

        last_log_term > our_last_term || 
        (last_log_term == our_last_term && last_log_index >= our_last_index)
    }

    /// Get events channel
    pub fn events(&mut self) -> Option<mpsc::Receiver<RaftEvent>> {
        self.event_rx.take()
    }
}

impl Clone for Raft {
    fn clone(&self) -> Self {
        Self {
            local_id: self.local_id,
            state: Arc::clone(&self.state),
            transport: Arc::clone(&self.transport),
            rpc_client: Arc::clone(&self.rpc_client),
            nodes: Arc::clone(&self.nodes),
            config: self.config.clone(),
            running: Arc::clone(&self.running),
            event_tx: self.event_tx.clone(),
            event_rx: None,
        }
    }
}

impl Clone for RaftState {
    fn clone(&self) -> Self {
        Self {
            current_term: self.current_term,
            voted_for: self.voted_for,
            log: self.log.clone(),
            commit_index: self.commit_index,
            last_applied: self.last_applied,
            role: self.role,
            leader_id: self.leader_id,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_raft_creation() {
        let node_id = NodeId::new(1);
        let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
        let transport = Arc::new(Transport::bind(addr).await.unwrap());
        let config = RaftConfig::default();
        let raft = Raft::new(node_id, transport, config);

        let state = raft.get_state().await;
        assert_eq!(state.current_term, 0);
        assert_eq!(state.role, RaftRole::Follower);
    }

    #[tokio::test]
    async fn test_raft_request_vote() {
        let node_id = NodeId::new(1);
        let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
        let transport = Arc::new(Transport::bind(addr).await.unwrap());
        let config = RaftConfig::default();
        let raft = Raft::new(node_id, transport, config);

        let args = RequestVoteArgs {
            term: 1,
            candidate_id: NodeId::new(2),
            last_log_index: 0,
            last_log_term: 0,
        };

        let result = raft.handle_request_vote(args).await;
        assert!(result.vote_granted);
        assert_eq!(result.term, 1);
    }

    #[tokio::test]
    async fn test_raft_append_entries() {
        let node_id = NodeId::new(1);
        let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
        let transport = Arc::new(Transport::bind(addr).await.unwrap());
        let config = RaftConfig::default();
        let raft = Raft::new(node_id, transport, config);

        let args = AppendEntriesArgs {
            term: 1,
            leader_id: NodeId::new(2),
            prev_log_index: 0,
            prev_log_term: 0,
            entries: Vec::new(),
            leader_commit: 0,
        };

        let result = raft.handle_append_entries(args).await;
        assert!(result.success);
        assert_eq!(result.term, 1);

        // Check that we recognized the leader
        let state = raft.get_state().await;
        assert_eq!(state.leader_id, Some(NodeId::new(2)));
        assert_eq!(state.role, RaftRole::Follower);
    }

    #[tokio::test]
    async fn test_raft_term_update() {
        let node_id = NodeId::new(1);
        let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
        let transport = Arc::new(Transport::bind(addr).await.unwrap());
        let config = RaftConfig::default();
        let raft = Raft::new(node_id, transport, config);

        // Request vote with higher term
        let args = RequestVoteArgs {
            term: 5,
            candidate_id: NodeId::new(2),
            last_log_index: 0,
            last_log_term: 0,
        };

        let result = raft.handle_request_vote(args).await;
        assert!(result.vote_granted);
        assert_eq!(result.term, 5);

        // Check that term was updated
        let state = raft.get_state().await;
        assert_eq!(state.current_term, 5);
    }

    #[tokio::test]
    async fn test_raft_is_leader() {
        let node_id = NodeId::new(1);
        let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
        let transport = Arc::new(Transport::bind(addr).await.unwrap());
        let config = RaftConfig::default();
        let raft = Raft::new(node_id, transport, config);

        // Initially not leader
        assert!(!raft.is_leader().await);

        // Manually set as leader (for testing)
        {
            let mut state = raft.state.write().await;
            state.role = RaftRole::Leader;
            state.leader_id = Some(node_id);
        }

        assert!(raft.is_leader().await);
    }
}

