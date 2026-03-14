//! Metrics and observability for OmniPaxos.

use crate::{
    ballot_leader_election::Ballot,
    util::{ConfigurationId, NodeId},
};
use std::collections::HashMap;

/// The role of an OmniPaxos node.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum Role {
    /// This node is the leader.
    Leader,
    /// This node is a follower.
    Follower,
}

/// The phase of an OmniPaxos node.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum Phase {
    /// Leader election / log recovery.
    Prepare,
    /// Normal operation — replicating entries.
    Accept,
    /// Recovering from a crash, waiting for leader.
    Recover,
    /// Initial state before any leader is known.
    None,
}

/// The health state of the node.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum RunningState {
    /// Node is operating normally.
    Healthy,
    /// A storage error has occurred.
    StorageError,
    /// Node has been reconfigured (stopsign decided).
    Reconfigured,
}

/// Per-follower replication metrics, populated only when this node is the leader.
#[derive(Debug, Clone)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct FollowerMetrics {
    /// The follower's node ID.
    pub node_id: NodeId,
    /// The follower's last known accepted index.
    pub accepted_idx: usize,
    /// Whether the follower has promised to this leader.
    pub is_promised: bool,
    /// How far behind the leader's accepted_idx this follower is.
    pub replication_lag: usize,
    /// The tick at which the last heartbeat was received from this peer (if tracked).
    pub last_heartbeat_tick: Option<u64>,
}

/// A snapshot of the current metrics for an OmniPaxos node.
#[derive(Debug, Clone)]
pub struct Metrics {
    /// This node's ID.
    pub pid: NodeId,
    /// Current role.
    pub role: Role,
    /// Current phase.
    pub phase: Phase,
    /// The current leader's node ID (0 if unknown).
    pub current_leader: NodeId,
    /// The current promise ballot.
    pub promise: Ballot,
    /// The decided log index.
    pub decided_idx: usize,
    /// The accepted log index.
    pub accepted_idx: usize,
    /// The compacted log index.
    pub compacted_idx: usize,
    /// Total number of messages dropped due to buffer overflow.
    pub dropped_messages: u64,
    /// Number of entries currently in the batch buffer.
    pub batched_entries: usize,
    /// The configuration ID.
    pub configuration_id: ConfigurationId,
    /// Whether this node has been reconfigured (stopsign decided).
    pub is_reconfigured: bool,
    /// The health state of the node.
    pub running_state: RunningState,
    /// Monotonically increasing counter, bumped on every observable state change.
    /// Callers can poll `metrics_version` cheaply to detect changes.
    pub metrics_version: u64,
    /// Total number of `tick()` calls since startup.
    pub tick_count: u64,
    /// The tick at which a quorum last acknowledged the leader.
    /// `None` if this node is not leader or has never received a quorum ack.
    pub last_quorum_ack_tick: Option<u64>,
    /// Number of ticks since the last quorum ack. `None` if not leader.
    pub ticks_since_quorum_ack: Option<u64>,
    /// Per-follower replication metrics (populated only when leader).
    pub follower_metrics: HashMap<NodeId, FollowerMetrics>,
    /// Number of peers in the cluster (excluding self).
    pub num_peers: usize,
    /// Number of nodes needed for a write quorum.
    pub write_quorum_size: usize,
    /// Number of nodes needed for a read quorum.
    pub read_quorum_size: usize,
    /// The current heartbeat round number from BLE.
    pub heartbeat_round: u32,
}

/// Trait for pluggable metrics recording.
///
/// Implement this trait to integrate OmniPaxos with your monitoring system
/// (Prometheus, StatsD, tracing, etc.). All methods have default no-op
/// implementations, so you only need to override the events you care about.
pub trait MetricsRecorder: Send {
    /// Called when this node's role changes (e.g., follower → leader).
    fn on_role_change(&self, _pid: NodeId, _old: Role, _new: Role) {}
    /// Called when the decided index advances.
    fn on_decided(&self, _pid: NodeId, _old_idx: usize, _new_idx: usize) {}
    /// Called when the leader changes.
    fn on_leader_change(&self, _pid: NodeId, _old_leader: NodeId, _new_leader: NodeId) {}
    /// Called when the leader receives a quorum ack.
    fn on_quorum_ack(&self, _pid: NodeId, _decided_idx: usize) {}
    /// Called when a message is dropped due to buffer overflow.
    fn on_message_dropped(&self, _pid: NodeId, _total_dropped: u64) {}
    /// Called when the phase changes.
    fn on_phase_change(&self, _pid: NodeId, _old: Phase, _new: Phase) {}
}
