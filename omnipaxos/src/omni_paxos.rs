use crate::{
    ballot_leader_election::{Ballot, BallotLeaderElection},
    engine::{Engine, EngineAction, InitialEngineState, Phase},
    engine::state::EngineStateSnapshot,
    errors::{valid_config, ConfigError, OmniPaxosError, StorageError},
    messages::Message,
    runtime::Runtime,
    storage::{Entry, PersistedState, StopSign, Storage},
    util::{
        defaults::{BUFFER_SIZE, ELECTION_TIMEOUT, FLUSH_BATCH_TIMEOUT, RESEND_MESSAGE_TIMEOUT},
        ConfigurationId, FlexibleQuorum, LogEntry, LogicalClock, NodeId,
    },
};

/// Snapshot of engine state for rollback on storage failure.
struct EngineSnapshot<T: Entry> {
    state: EngineStateSnapshot<T>,
    outgoing_len: usize,
}
#[cfg(any(feature = "toml_config", feature = "serde"))]
use serde::Deserialize;
#[cfg(feature = "serde")]
use serde::Serialize;
#[cfg(feature = "toml_config")]
use std::fs;
use std::{
    error::Error,
    fmt::{Debug, Display},
    ops::RangeBounds,
};
#[cfg(feature = "toml_config")]
use toml;

/// Configuration for `OmniPaxos`.
/// # Fields
/// * `cluster_config`: The configuration settings that are cluster-wide.
/// * `server_config`: The configuration settings that are specific to this OmniPaxos server.
#[allow(missing_docs)]
#[derive(Clone, Debug, Default)]
#[cfg_attr(feature = "toml_config", derive(Deserialize), serde(default))]
pub struct OmniPaxosConfig {
    pub cluster_config: ClusterConfig,
    pub server_config: ServerConfig,
}

impl OmniPaxosConfig {
    /// Checks that all the fields of the cluster config are valid.
    pub fn validate(&self) -> Result<(), ConfigError> {
        self.cluster_config.validate()?;
        self.server_config.validate()?;
        valid_config!(
            self.cluster_config.nodes.contains(&self.server_config.pid),
            "Nodes must include own server pid"
        );
        Ok(())
    }

    /// Creates a new `OmniPaxosConfig` from a `toml` file.
    #[cfg(feature = "toml_config")]
    pub fn with_toml(file_path: &str) -> Result<Self, ConfigError> {
        let config_file = fs::read_to_string(file_path)?;
        let config: OmniPaxosConfig = toml::from_str(&config_file)?;
        config.validate()?;
        Ok(config)
    }

    /// Checks all configuration fields and returns the local OmniPaxos node if successful.
    pub async fn build<T, B>(self, storage: B) -> Result<OmniPaxos<T, B>, OmniPaxosError>
    where
        T: Entry,
        B: Storage<T>,
    {
        self.validate()?;
        // Load all persisted state in one call
        let state = storage
            .load_state()
            .await
            .map_err(StorageError::load_state)?;

        validate_persisted_state(&state)?;

        let mut accepted_idx = state.log_len + state.compacted_idx;
        if state.stopsign.is_some() {
            accepted_idx += 1;
        }

        let pid = self.server_config.pid;
        let peers: Vec<NodeId> = self
            .cluster_config
            .nodes
            .iter()
            .copied()
            .filter(|x| *x != pid)
            .collect();

        let initial_state = InitialEngineState {
            promise: state.promise,
            accepted_round: state.accepted_round,
            decided_idx: state.decided_idx,
            accepted_idx,
            compacted_idx: state.compacted_idx,
            stopsign: state.stopsign,
            flexible_quorum: self.cluster_config.flexible_quorum,
        };

        let engine = Engine::new(
            pid,
            peers,
            self.server_config.buffer_size,
            self.server_config.batch_size,
            initial_state,
        );

        // If we recovered a promise, set it in storage (matching old behavior)
        let mut runtime = Runtime::new(storage);
        if let Some(b) = state.promise {
            if b != Ballot::default() {
                runtime
                    .storage
                    .write_atomically(vec![crate::storage::StorageOp::SetPromise(b)])
                    .await
                    .map_err(|e| OmniPaxosError::Fatal(StorageError::write_batch(e)))?;
            }
        }

        Ok(OmniPaxos {
            engine,
            runtime,
            ble: BallotLeaderElection::with(self.clone().into(), state.promise),
            election_clock: LogicalClock::with(self.server_config.election_tick_timeout),
            resend_message_clock: LogicalClock::with(
                self.server_config.resend_message_tick_timeout,
            ),
            flush_batch_clock: LogicalClock::with(self.server_config.flush_batch_tick_timeout),
            runtime_config: RuntimeConfig::default(),
            tick_count: 0,
            metrics_version: 0,
            running_state: crate::metrics::RunningState::Healthy,
            last_quorum_ack_tick: None,
            lease_tick_timeout: self.server_config.lease_tick_timeout,
            metrics_recorder: None,
        })
    }
}

/// Validate invariants of persisted state loaded from storage at startup.
/// Catches corrupted or inconsistent storage before constructing the node.
fn validate_persisted_state<T: Entry>(state: &PersistedState<T>) -> Result<(), OmniPaxosError> {
    let max_accepted = state.log_len + state.compacted_idx
        + state.stopsign.is_some() as usize;

    if state.compacted_idx > state.decided_idx {
        return Err(OmniPaxosError::CorruptedState(format!(
            "compacted_idx ({}) > decided_idx ({})",
            state.compacted_idx, state.decided_idx
        )));
    }

    if state.decided_idx > max_accepted {
        return Err(OmniPaxosError::CorruptedState(format!(
            "decided_idx ({}) > accepted_idx ({})",
            state.decided_idx, max_accepted
        )));
    }

    if state.snapshot.is_some() && state.compacted_idx == 0 {
        return Err(OmniPaxosError::CorruptedState(
            "snapshot present but compacted_idx is 0".to_string(),
        ));
    }

    if let (Some(ref promise), Some(ref accepted_round)) = (&state.promise, &state.accepted_round)
    {
        if promise < accepted_round {
            return Err(OmniPaxosError::CorruptedState(format!(
                "promise ({:?}) < accepted_round ({:?})",
                promise, accepted_round
            )));
        }
    }

    Ok(())
}

/// Configuration for an `OmniPaxos` cluster.
/// # Fields
/// * `configuration_id`: The identifier for the cluster configuration that this OmniPaxos server is part of.
/// * `nodes`: The nodes in the cluster i.e. the `pid`s of the other servers in the configuration.
/// * `flexible_quorum` : Defines read and write quorum sizes. Can be used for different latency vs fault tolerance tradeoffs.
#[derive(Clone, Debug, PartialEq, Default)]
#[cfg_attr(any(feature = "serde", feature = "toml_config"), derive(Deserialize))]
#[cfg_attr(feature = "toml_config", serde(default))]
#[cfg_attr(feature = "serde", derive(Serialize))]
pub struct ClusterConfig {
    /// The identifier for the cluster configuration that this OmniPaxos server is part of. Must
    /// not be 0 and be greater than the previous configuration's id.
    pub configuration_id: ConfigurationId,
    /// The nodes in the cluster i.e. the `pid`s of the servers in the configuration.
    pub nodes: Vec<NodeId>,
    /// Defines read and write quorum sizes. Can be used for different latency vs fault tolerance tradeoffs.
    pub flexible_quorum: Option<FlexibleQuorum>,
}

impl ClusterConfig {
    /// Checks that all the fields of the cluster config are valid.
    pub fn validate(&self) -> Result<(), ConfigError> {
        let num_nodes = self.nodes.len();
        valid_config!(num_nodes >= 1, "Need at least 1 node");
        valid_config!(self.configuration_id != 0, "Configuration ID cannot be 0");
        if let Some(FlexibleQuorum {
            read_quorum_size,
            write_quorum_size,
        }) = self.flexible_quorum
        {
            valid_config!(
                read_quorum_size + write_quorum_size > num_nodes,
                "The quorums must overlap i.e., the sum of their sizes must exceed the # of nodes"
            );
            valid_config!(
                read_quorum_size >= 1 && read_quorum_size <= num_nodes,
                "Read quorum must be in range 1 to # of nodes in the cluster"
            );
            valid_config!(
                write_quorum_size >= 1 && write_quorum_size <= num_nodes,
                "Write quorum must be in range 1 to # of nodes in the cluster"
            );
            if num_nodes > 1 {
                valid_config!(
                    write_quorum_size >= 2,
                    "Write quorum must be at least 2 for multi-node clusters to ensure durability"
                );
            }
        }
        Ok(())
    }

    /// Checks all configuration fields and builds a local OmniPaxos node with settings for this
    /// node defined in `server_config` and using storage `with_storage`.
    pub async fn build_for_server<T, B>(
        self,
        server_config: ServerConfig,
        with_storage: B,
    ) -> Result<OmniPaxos<T, B>, OmniPaxosError>
    where
        T: Entry,
        B: Storage<T>,
    {
        let op_config = OmniPaxosConfig {
            cluster_config: self,
            server_config,
        };
        op_config.build(with_storage).await
    }
}

/// Configuration for a singular `OmniPaxos` instance in a cluster.
/// # Fields
/// * `pid`: The unique identifier of this node. Must not be 0.
/// * `election_tick_timeout`: The number of calls to `tick()` before leader election is updated. If this is set to 5 and `tick()` is called every 10ms, then the election timeout will be 50ms. Must not be 0.
/// * `resend_message_tick_timeout`: The number of calls to `tick()` before a message is considered dropped and thus resent. Must not be 0.
/// * `buffer_size`: The buffer size for outgoing messages.
/// * `batch_size`: The size of the buffer for log batching. The default is 1, which means no batching.
/// * `logger_file_path`: The path where the default logger logs events.
/// * `leader_priority` : Custom priority for this node to be elected as the leader.
#[derive(Clone, Debug)]
#[cfg_attr(feature = "toml_config", derive(Deserialize), serde(default))]
pub struct ServerConfig {
    /// The unique identifier of this node. Must not be 0.
    pub pid: NodeId,
    /// The number of calls to `tick()` before leader election is updated. If this is set to 5 and `tick()` is called every 10ms, then the election timeout will be 50ms.
    pub election_tick_timeout: u64,
    /// The number of calls to `tick()` before a message is considered dropped and thus resent. Must not be 0.
    pub resend_message_tick_timeout: u64,
    /// The buffer size for outgoing messages.
    pub buffer_size: usize,
    /// The size of the buffer for log batching. The default is 1, which means no batching.
    pub batch_size: usize,
    /// The number of calls to `tick()` before the batched log entries are flushed.
    pub flush_batch_tick_timeout: u64,
    /// Custom priority for this node to be elected as the leader.
    pub leader_priority: u32,
    /// Optional lease-based read timeout in ticks. When set, the leader can serve
    /// linearizable reads without a network round-trip if fewer than this many
    /// ticks have elapsed since the last quorum acknowledgment.
    ///
    /// **Safety:** Lease reads rely on bounded clock drift. The lease timeout
    /// should be significantly less than `election_tick_timeout` to maintain
    /// safety. If clock drift exceeds the margin, linearizability may be violated.
    /// Defaults to `None` (disabled — always uses ReadIndex protocol).
    pub lease_tick_timeout: Option<u64>,
}

impl ServerConfig {
    /// Checks that all the fields of the server config are valid.
    pub fn validate(&self) -> Result<(), ConfigError> {
        valid_config!(self.pid != 0, "Server pid cannot be 0");
        valid_config!(self.buffer_size != 0, "Buffer size must be greater than 0");
        valid_config!(self.batch_size != 0, "Batch size must be greater than 0");
        valid_config!(
            self.election_tick_timeout != 0,
            "Election tick timeout must be greater than 0"
        );
        valid_config!(
            self.resend_message_tick_timeout != 0,
            "Resend message tick timeout must be greater than 0"
        );
        if let Some(lease) = self.lease_tick_timeout {
            valid_config!(lease > 0, "Lease tick timeout must be greater than 0");
        }
        Ok(())
    }
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            pid: 0,
            election_tick_timeout: ELECTION_TIMEOUT,
            resend_message_tick_timeout: RESEND_MESSAGE_TIMEOUT,
            buffer_size: BUFFER_SIZE,
            batch_size: 1,
            flush_batch_tick_timeout: FLUSH_BATCH_TIMEOUT,
            leader_priority: 0,
            lease_tick_timeout: None,
        }
    }
}

/// The `OmniPaxos` struct represents an OmniPaxos server. Maintains the replicated log that can be read from and appended to.
/// It also handles incoming messages and produces outgoing messages that you need to fetch and send periodically using your own network implementation.
pub struct OmniPaxos<T, B>
where
    T: Entry,
    B: Storage<T>,
{
    engine: Engine<T>,
    runtime: Runtime<T, B>,
    ble: BallotLeaderElection,
    election_clock: LogicalClock,
    resend_message_clock: LogicalClock,
    flush_batch_clock: LogicalClock,
    runtime_config: RuntimeConfig,
    tick_count: u64,
    metrics_version: u64,
    running_state: crate::metrics::RunningState,
    last_quorum_ack_tick: Option<u64>,
    lease_tick_timeout: Option<u64>,
    metrics_recorder: Option<Box<dyn crate::metrics::MetricsRecorder>>,
}

impl<T, B> OmniPaxos<T, B>
where
    T: Entry,
    B: Storage<T>,
{
    /// Snapshot engine state (scalars + outgoing length) before an operation.
    fn snapshot_engine(&self) -> EngineSnapshot<T> {
        EngineSnapshot {
            state: self.engine.s.snapshot(),
            outgoing_len: self.engine.outgoing.len(),
        }
    }

    /// Restore engine state on failure.
    fn restore_engine(&mut self, snap: EngineSnapshot<T>) {
        self.engine.s.restore(snap.state);
        self.engine.outgoing.truncate(snap.outgoing_len);
        self.engine.commands.clear();
        // Reset backoff — the stall was caused by our storage failure, not
        // the followers' unresponsiveness.
        self.engine.s.leader_state.reset_backoff();
    }

    /// Helper: run engine, execute commands, handle action.
    /// Snapshots engine state before execution and restores on failure.
    async fn execute_engine_commands(&mut self) -> Result<(), StorageError> {
        let cmds = self.engine.take_commands();
        if cmds.is_empty() {
            return Ok(());
        }
        // Note: snapshot was already taken by the caller before engine mutation
        self.runtime.execute_commands(cmds).await
    }

    /// Helper: run action from engine that may need storage reads.
    async fn handle_engine_action(&mut self, action: EngineAction<T>) -> Result<(), StorageError> {
        // Execute any commands generated before the action
        let cmds = self.engine.take_commands();
        self.runtime.execute_commands(cmds).await?;
        // Then handle the action (which may generate more commands)
        self.runtime.handle_action(action, &mut self.engine).await
    }

    /// Snapshot 5 scalar values before a public entry point.
    fn snapshot_metrics(
        &self,
    ) -> (
        crate::engine::state::Role,
        crate::engine::state::Phase,
        usize,
        NodeId,
        u64,
    ) {
        let (role, phase) = self.engine.s.state.clone();
        let decided_idx = self.engine.get_decided_idx();
        let leader = self.engine.get_promise().pid;
        let dropped = self.engine.get_dropped_message_count();
        (role, phase, decided_idx, leader, dropped)
    }

    /// Compare old vs new state after a public entry point, increment
    /// metrics_version and call the recorder if installed.
    fn detect_and_report_changes(
        &mut self,
        old_role: crate::engine::state::Role,
        old_phase: crate::engine::state::Phase,
        old_decided: usize,
        old_leader: NodeId,
        old_dropped: u64,
    ) {
        use crate::engine::state::{Role as R, Phase as P};
        use crate::metrics::{Role, Phase};

        let (new_role, new_phase) = &self.engine.s.state;
        let new_decided = self.engine.get_decided_idx();
        let new_leader = self.engine.get_promise().pid;
        let new_dropped = self.engine.get_dropped_message_count();

        let mut changed = false;

        if *new_role != old_role {
            changed = true;
            if let Some(ref recorder) = self.metrics_recorder {
                let old_r = match old_role {
                    R::Leader => Role::Leader,
                    R::Follower => Role::Follower,
                };
                let new_r = match *new_role {
                    R::Leader => Role::Leader,
                    R::Follower => Role::Follower,
                };
                recorder.on_role_change(self.engine.s.pid, old_r, new_r);
            }
        }

        if *new_phase != old_phase {
            changed = true;
            if let Some(ref recorder) = self.metrics_recorder {
                let old_p = match old_phase {
                    P::Prepare => Phase::Prepare,
                    P::Accept => Phase::Accept,
                    P::Recover => Phase::Recover,
                    P::None => Phase::None,
                };
                let new_p = match *new_phase {
                    P::Prepare => Phase::Prepare,
                    P::Accept => Phase::Accept,
                    P::Recover => Phase::Recover,
                    P::None => Phase::None,
                };
                recorder.on_phase_change(self.engine.s.pid, old_p, new_p);
            }
        }

        if new_decided != old_decided {
            changed = true;
            // Update last_quorum_ack_tick when leader decides
            if *new_role == R::Leader {
                self.last_quorum_ack_tick = Some(self.tick_count);
            }
            if let Some(ref recorder) = self.metrics_recorder {
                recorder.on_decided(self.engine.s.pid, old_decided, new_decided);
            }
        }

        // Update last_quorum_ack_tick when a ReadIndex request reaches quorum
        if self.engine.s.read_quorum_reached {
            self.engine.s.read_quorum_reached = false;
            if *new_role == R::Leader {
                self.last_quorum_ack_tick = Some(self.tick_count);
            }
        }

        if new_leader != old_leader {
            changed = true;
            if let Some(ref recorder) = self.metrics_recorder {
                recorder.on_leader_change(self.engine.s.pid, old_leader, new_leader);
            }
        }

        if new_dropped != old_dropped {
            changed = true;
            if let Some(ref recorder) = self.metrics_recorder {
                recorder.on_message_dropped(self.engine.s.pid, new_dropped);
            }
        }

        if changed {
            self.metrics_version += 1;
        }
    }

    /// Initiates the trim process.
    /// # Arguments
    /// * `trim_index` - Deletes all entries up to [`trim_index`], if the [`trim_index`] is `None` then the minimum index accepted by **ALL** servers will be used as the [`trim_index`].
    pub async fn trim(&mut self, trim_index: Option<usize>) -> Result<(), CompactionErr> {
        use crate::engine::state::Role;
        match self.engine.s.state {
            (Role::Leader, _) => {
                let min_all_accepted_idx =
                    self.engine.s.leader_state.get_min_all_accepted_idx();
                let trimmed_idx = match trim_index {
                    Some(idx) if idx <= min_all_accepted_idx => idx,
                    None => min_all_accepted_idx,
                    _ => {
                        return Err(CompactionErr::NotAllDecided(min_all_accepted_idx));
                    }
                };
                let snap = self.snapshot_engine();
                self.engine.try_trim(trimmed_idx);
                let result = self.execute_engine_commands().await;
                if result.is_err() {
                    self.restore_engine(snap);
                }
                if result.is_ok() {
                    // Send compaction to peers
                    let pid = self.engine.s.pid;
                    let peers = self.engine.s.peers.clone();
                    for peer in peers {
                        use crate::messages::sequence_paxos::*;
                        let msg = PaxosMsg::Compaction(Compaction::Trim(trimmed_idx));
                        self.engine.try_push_message(Message::SequencePaxos(
                            PaxosMessage {
                                from: pid,
                                to: peer,
                                msg,
                            },
                        ));
                    }
                }
                result.map_err(CompactionErr::Storage)
            }
            _ => Err(CompactionErr::NotCurrentLeader(
                self.engine.get_promise().pid,
            )),
        }
    }

    /// Trim the log and create a snapshot. ** Note: only up to the `decided_idx` can be snapshotted **
    /// # Arguments
    /// `compact_idx` - Snapshots all entries < [`compact_idx`], if the [`compact_idx`] is None then the decided index will be used.
    /// `local_only` - If `true`, only this server snapshots the log. If `false` all servers performs the snapshot.
    pub async fn snapshot(
        &mut self,
        compact_idx: Option<usize>,
        local_only: bool,
    ) -> Result<(), CompactionErr> {
        self.runtime
            .try_snapshot(&mut self.engine, compact_idx, false)
            .await?;
        if !local_only {
            let pid = self.engine.s.pid;
            let peers = self.engine.s.peers.clone();
            for peer in peers {
                use crate::messages::sequence_paxos::*;
                let msg = PaxosMsg::Compaction(Compaction::Snapshot(compact_idx));
                self.engine
                    .try_push_message(Message::SequencePaxos(PaxosMessage {
                        from: pid,
                        to: peer,
                        msg,
                    }));
            }
        }
        Ok(())
    }

    /// Return the decided index. 0 means that no entry has been decided.
    pub fn get_decided_idx(&self) -> usize {
        self.engine.get_decided_idx()
    }

    /// Return trim index from storage.
    pub fn get_compacted_idx(&self) -> usize {
        self.engine.get_compacted_idx()
    }

    /// Returns the ID of the current leader and whether the node's `Phase` is `Phase::Accepted`.
    ///
    /// If the node's phase is `Phase::Accepted`, this implies that the returned leader is also
    /// in the accepted phase. However, a `Phase::Prepare` or a `false` response does not
    /// necessarily imply that the leader is not in the accepted phase; it only reflects the current
    /// phase of this node.
    pub fn get_current_leader(&self) -> Option<(NodeId, bool)> {
        let promised_pid = self.engine.get_promise().pid;
        if promised_pid == 0 {
            None
        } else {
            let is_accepted = self.engine.get_state().1 == Phase::Accept;
            Some((promised_pid, is_accepted))
        }
    }

    /// Returns the promised ballot of this node.
    pub fn get_promise(&self) -> Ballot {
        self.engine.get_promise()
    }

    /// Moves outgoing messages from this server into the buffer. The messages should then be sent via the network implementation.
    pub fn take_outgoing_messages(&mut self, buffer: &mut Vec<Message<T>>) {
        self.engine.take_outgoing_msgs(buffer);
        buffer.extend(self.ble.outgoing_mut().drain(..).map(|b| Message::BLE(b)));
    }

    /// Read entry at index `idx` in the log. Returns `None` if `idx` is out of bounds.
    pub async fn read(&self, idx: usize) -> Result<Option<LogEntry<T>>, OmniPaxosError> {
        match self
            .runtime
            .read(&self.engine, idx..idx + 1)
            .await?
        {
            Some(mut v) => Ok(v.pop()),
            None => Ok(None),
        }
    }

    /// Read entries in the range `r` in the log. Returns `None` if `r` is out of bounds.
    pub async fn read_entries<R>(&self, r: R) -> Result<Option<Vec<LogEntry<T>>>, OmniPaxosError>
    where
        R: RangeBounds<usize>,
    {
        Ok(self.runtime.read(&self.engine, r).await?)
    }

    /// Read all decided entries starting at `from_idx` (inclusive) in the log. Returns `None` if `from_idx` is out of bounds.
    pub async fn read_decided_suffix(
        &self,
        from_idx: usize,
    ) -> Result<Option<Vec<LogEntry<T>>>, OmniPaxosError> {
        Ok(self
            .runtime
            .read_decided_suffix(&self.engine, from_idx)
            .await?)
    }

    /// Handle an incoming message.
    /// Note: on storage failure, state may be partially updated (matching
    /// the old behavior where each storage call was independent).
    pub async fn handle_incoming(&mut self, m: Message<T>) -> Result<(), OmniPaxosError> {
        let (old_role, old_phase, old_decided, old_leader, old_dropped) =
            self.snapshot_metrics();
        match m {
            Message::SequencePaxos(p) => {
                let action = self.engine.handle(p);
                if let Err(e) = self.handle_engine_action(action).await {
                    self.running_state = crate::metrics::RunningState::StorageError;
                    self.metrics_version += 1;
                    return Err(e.into());
                }
            }
            Message::BLE(b) => self.ble.handle(b),
        }
        self.detect_and_report_changes(old_role, old_phase, old_decided, old_leader, old_dropped);
        Ok(())
    }

    /// Returns whether this Sequence Paxos has been reconfigured
    pub fn is_reconfigured(&self) -> Option<StopSign> {
        self.engine.is_reconfigured()
    }

    /// Append an entry to the replicated log.
    ///
    /// Returns `Ok(AppendResult)` describing how the entry was handled, or an error
    /// if the entry could not be proposed.
    pub async fn append(&mut self, entry: T) -> Result<AppendResult, ProposeErr<T>> {
        if !self.runtime_config.enable_proposals {
            return Err(ProposeErr::ProposalsDisabled(entry));
        }
        if self.engine.s.transfer_state.is_some() {
            return Err(ProposeErr::TransferInProgress(entry));
        }
        if self.engine.accepted_reconfiguration() {
            return Err(ProposeErr::PendingReconfigEntry(entry));
        }
        let (old_role, old_phase, old_decided, old_leader, old_dropped) =
            self.snapshot_metrics();
        let snap = self.snapshot_engine();
        match self.engine.propose_entry(entry) {
            Ok(result) => {
                if let Err(e) = self.execute_engine_commands().await {
                    self.restore_engine(snap);
                    self.running_state = crate::metrics::RunningState::StorageError;
                    self.metrics_version += 1;
                    return Err(ProposeErr::Storage(e));
                }
                self.detect_and_report_changes(
                    old_role, old_phase, old_decided, old_leader, old_dropped,
                );
                Ok(match result {
                    crate::engine::ProposeResult::Accepted => AppendResult::Accepted,
                    crate::engine::ProposeResult::Forwarded { leader } => {
                        AppendResult::Forwarded { leader }
                    }
                    crate::engine::ProposeResult::Buffered => AppendResult::Buffered,
                })
            }
            Err(entry) => Err(ProposeErr::NoLeader(entry)),
        }
    }

    /// Propose a cluster reconfiguration. Returns an error if the current configuration has already been stopped
    /// by a previous reconfiguration request or if the `new_configuration` is invalid.
    /// `new_configuration` defines the cluster-wide configuration settings for the **next** cluster.
    /// `metadata` is optional data to commit alongside the reconfiguration.
    pub async fn reconfigure(
        &mut self,
        new_configuration: ClusterConfig,
        metadata: Option<Vec<u8>>,
    ) -> Result<(), ProposeErr<T>> {
        if let Err(config_error) = new_configuration.validate() {
            return Err(ProposeErr::ConfigError(
                config_error,
                new_configuration,
                metadata,
            ));
        }
        if self.engine.accepted_reconfiguration() {
            return Err(ProposeErr::PendingReconfigConfig(
                new_configuration,
                metadata,
            ));
        }
        use crate::engine::state::Role;
        let ss = StopSign::with(new_configuration, metadata);
        match self.engine.s.state {
            (Role::Leader, Phase::Prepare) => self.engine.s.buffered_stopsign = Some(ss),
            (Role::Leader, Phase::Accept) => {
                let snap = self.snapshot_engine();
                self.engine.accept_stopsign_leader(ss);
                if let Err(e) = self.execute_engine_commands().await {
                    self.restore_engine(snap);
                    return Err(ProposeErr::Storage(e));
                }
            }
            _ => self.engine.forward_stopsign(ss),
        }
        Ok(())
    }

    /// Handles re-establishing a connection to a previously disconnected peer.
    /// This should only be called if the underlying network implementation indicates that a connection has been re-established.
    pub fn reconnected(&mut self, pid: NodeId) {
        self.engine.reconnected(pid)
    }

    /// Increments the internal logical clock. This drives the processes for leader changes, resending dropped messages, and flushing batched log entries.
    /// Each of these is triggered every `election_tick_timeout`, `resend_message_tick_timeout`, and `flush_batch_tick_timeout` number of calls to this function
    /// (See how to configure these timeouts in `ServerConfig`).
    pub async fn tick(&mut self) -> Result<(), OmniPaxosError> {
        self.tick_count += 1;
        let (old_role, old_phase, old_decided, old_leader, old_dropped) =
            self.snapshot_metrics();
        if self.election_clock.tick_and_check_timeout() {
            self.election_timeout().await?;
        }
        if self.resend_message_clock.tick_and_check_timeout() {
            self.engine.resend_message_timeout();
        }
        // Tick leader transfer timeout
        self.engine.tick_transfer();
        // Tick per-follower backoff counters
        self.engine.tick_backoff();
        if self.flush_batch_clock.tick_and_check_timeout() {
            let snap = self.snapshot_engine();
            let old_leader_accepted_idx = self
                .engine
                .s
                .leader_state
                .get_accepted_idx(self.engine.s.pid);
            self.engine.flush_batch_timeout();
            if let Err(e) = self.execute_engine_commands().await {
                self.restore_engine(snap);
                self.engine
                    .s
                    .leader_state
                    .set_accepted_idx(self.engine.s.pid, old_leader_accepted_idx);
                self.running_state = crate::metrics::RunningState::StorageError;
                self.metrics_version += 1;
                return Err(e.into());
            }
        }
        self.detect_and_report_changes(old_role, old_phase, old_decided, old_leader, old_dropped);
        Ok(())
    }

    /// Manually attempt to become the leader by incrementing this instance's Ballot. Calling this
    /// function may not result in gainig leadership if other instances are competing for
    /// leadership with higher Ballots.
    pub async fn try_become_leader(&mut self) -> Result<(), OmniPaxosError> {
        let mut my_ballot = self.ble.get_current_ballot();
        let promise = self.engine.get_promise();
        my_ballot.n = promise.n + 1;
        // Sync BLE state so it's aware of the new ballot
        self.ble.set_current_ballot(my_ballot);
        self.ble.set_leader(my_ballot);
        let snap = self.snapshot_engine();
        let action = self.engine.handle_leader(my_ballot);
        if let Err(e) = self.handle_engine_action(action).await {
            self.restore_engine(snap);
            return Err(e.into());
        }
        Ok(())
    }

    /// Returns the total number of outgoing messages that were dropped due to buffer overflow.
    pub fn get_dropped_message_count(&self) -> u64 {
        self.engine.get_dropped_message_count()
    }

    /// Returns a snapshot of the current metrics for this node.
    pub fn metrics(&self) -> crate::metrics::Metrics {
        use crate::engine::state::{Phase as P, Role as R};
        use crate::metrics::{
            FollowerMetrics, Metrics, Phase, Role, RunningState,
        };
        let (role, phase) = &self.engine.s.state;
        let role = match role {
            R::Leader => Role::Leader,
            R::Follower => Role::Follower,
        };
        let phase = match phase {
            P::Prepare => Phase::Prepare,
            P::Accept => Phase::Accept,
            P::Recover => Phase::Recover,
            P::None => Phase::None,
        };

        let leader_accepted_idx = self.engine.get_accepted_idx();
        let follower_metrics = if role == Role::Leader {
            let data = self.engine.s.leader_state.get_follower_accepted_indexes();
            data.into_iter()
                .filter(|(pid, _, _)| *pid != self.engine.s.pid)
                .map(|(pid, acc_idx, is_promised)| {
                    (
                        pid,
                        FollowerMetrics {
                            node_id: pid,
                            accepted_idx: acc_idx,
                            is_promised,
                            replication_lag: leader_accepted_idx.saturating_sub(acc_idx),
                            last_heartbeat_tick: None,
                            backed_off: self.engine.s.leader_state.is_backed_off(pid),
                        },
                    )
                })
                .collect()
        } else {
            std::collections::HashMap::new()
        };

        let running_state = if self.engine.is_reconfigured().is_some() {
            RunningState::Reconfigured
        } else {
            self.running_state
        };

        let ticks_since_quorum_ack = if role == Role::Leader {
            self.last_quorum_ack_tick
                .map(|t| self.tick_count.saturating_sub(t))
        } else {
            None
        };

        Metrics {
            pid: self.engine.s.pid,
            role,
            phase,
            current_leader: self.engine.get_promise().pid,
            promise: self.engine.get_promise(),
            decided_idx: self.engine.get_decided_idx(),
            accepted_idx: leader_accepted_idx,
            compacted_idx: self.engine.get_compacted_idx(),
            dropped_messages: self.engine.get_dropped_message_count(),
            batched_entries: self.engine.s.batched_entries.len(),
            configuration_id: self.engine.get_promise().config_id,
            is_reconfigured: self.engine.is_reconfigured().is_some(),
            running_state,
            metrics_version: self.metrics_version,
            tick_count: self.tick_count,
            last_quorum_ack_tick: self.last_quorum_ack_tick,
            ticks_since_quorum_ack,
            follower_metrics,
            num_peers: self.engine.s.peers.len(),
            write_quorum_size: self.engine.s.leader_state.quorum.write_quorum_size(),
            read_quorum_size: self.engine.s.leader_state.quorum.read_quorum_size(),
            heartbeat_round: self.ble.get_hb_round(),
        }
    }

    /// Set a custom metrics recorder for push-based observability.
    pub fn set_metrics_recorder(
        &mut self,
        recorder: Box<dyn crate::metrics::MetricsRecorder>,
    ) {
        self.metrics_recorder = Some(recorder);
    }

    /// Returns the current metrics version. This is a cheap way to detect
    /// if any observable state has changed since the last poll.
    pub fn metrics_version(&self) -> u64 {
        self.metrics_version
    }

    /// Check whether the current metrics satisfy a condition.
    /// Useful for polling loops and tests.
    ///
    /// # Example
    /// ```ignore
    /// while !node.check_condition(|m| m.decided_idx >= 5) {
    ///     node.tick().await?;
    /// }
    /// ```
    pub fn check_condition<F>(&self, f: F) -> bool
    where
        F: FnOnce(&crate::metrics::Metrics) -> bool,
    {
        f(&self.metrics())
    }

    /// Start a linearizable read by confirming leader authority.
    ///
    /// If a lease timeout is configured and the leader recently received a quorum
    /// acknowledgment, returns [`ReadGuard::Lease`] with an immediate barrier
    /// (no network round-trip). Otherwise falls back to the ReadIndex protocol
    /// and returns [`ReadGuard::Pending`] with a `read_id` to poll.
    ///
    /// For single-node clusters, the ReadIndex barrier is also immediately available.
    pub fn ensure_linearizable(&mut self) -> Result<ReadGuard, ReadError> {
        use crate::engine::state::{Role, Phase as EngPhase};
        // Lease fast path: skip network round-trip if within lease window
        if let Some(lease_timeout) = self.lease_tick_timeout {
            if self.engine.s.state == (Role::Leader, EngPhase::Accept) {
                if let Some(last_ack) = self.last_quorum_ack_tick {
                    let elapsed = self.tick_count.saturating_sub(last_ack);
                    if elapsed < lease_timeout {
                        return Ok(ReadGuard::Lease(ReadBarrier {
                            read_idx: self.engine.get_decided_idx(),
                        }));
                    }
                }
            }
        }
        // Slow path: ReadIndex protocol
        self.engine.start_read_index().map(ReadGuard::Pending)
    }

    /// Poll for a read barrier. Returns `Ok(ReadBarrier)` when a quorum has confirmed
    /// leader authority, `Err(ReadError::QuorumPending)` if still waiting, or
    /// `Err(ReadError::QuorumLost)` if the read-index was lost.
    ///
    /// Once you have a `ReadBarrier`, wait until `get_decided_idx() >= barrier.read_idx()`
    /// then reads from the log are linearizable.
    pub fn poll_read_barrier(&mut self, read_id: u64) -> Result<ReadBarrier, ReadError> {
        self.engine.poll_read_barrier(read_id)
    }

    /// Initiate a graceful leader transfer to the specified target node.
    /// The leader will stop accepting new proposals and, once the target is caught up,
    /// send a `TransferLeader` message causing the target to start an election.
    pub fn transfer_leader(&mut self, target: NodeId) -> Result<(), TransferError> {
        self.engine.transfer_leader(target)
    }

    /// Set the runtime configuration (election and proposal toggles).
    pub fn set_runtime_config(&mut self, config: RuntimeConfig) {
        self.runtime_config = config;
    }

    /// Toggle whether this node participates in leader elections.
    pub fn set_election_enabled(&mut self, enabled: bool) {
        self.runtime_config.enable_election = enabled;
    }

    /// Toggle whether this node accepts proposals.
    pub fn set_proposals_enabled(&mut self, enabled: bool) {
        self.runtime_config.enable_proposals = enabled;
    }

    /*** BLE calls ***/
    /// Update the custom priority used in the Ballot for this server. Note that changing the
    /// priority triggers a leader re-election.
    pub fn set_priority(&mut self, p: u32) {
        self.ble.set_priority(p)
    }

    /// If the heartbeat of a leader is not received when election_timeout() is called, the server might attempt to become the leader.
    /// It is also used for the election process, where the server checks if it can become the leader.
    /// For instance if `election_timeout()` is called every 100ms, then if the leader fails, the servers will detect it after 100ms and elect a new server after another 100ms if possible.
    async fn election_timeout(&mut self) -> Result<(), OmniPaxosError> {
        if let Some(new_leader) = self
            .ble
            .hb_timeout(self.engine.get_state(), self.engine.get_promise())
        {
            // Skip becoming leader if elections are disabled for this node
            if !self.runtime_config.enable_election && new_leader.pid == self.engine.s.pid {
                return Ok(());
            }
            let snap = self.snapshot_engine();
            let action = self.engine.handle_leader(new_leader);
            if let Err(e) = self.handle_engine_action(action).await {
                self.restore_engine(snap);
                return Err(e.into());
            }
        }
        Ok(())
    }
}

/// Runtime-switchable configuration flags.
/// These can be changed without restarting the node.
#[derive(Clone, Debug)]
pub struct RuntimeConfig {
    /// When false, node will not attempt to become leader. Still responds to heartbeats.
    pub enable_election: bool,
    /// When false, node rejects all proposals. In-flight entries still get decided.
    pub enable_proposals: bool,
}

impl Default for RuntimeConfig {
    fn default() -> Self {
        Self {
            enable_election: true,
            enable_proposals: true,
        }
    }
}

/// Result of [`ensure_linearizable()`](OmniPaxos::ensure_linearizable).
#[derive(Debug)]
pub enum ReadGuard {
    /// Leader lease is valid; the read barrier is immediately available.
    /// No network round-trip was needed.
    Lease(ReadBarrier),
    /// ReadIndex protocol has been initiated. Poll with
    /// [`poll_read_barrier()`](OmniPaxos::poll_read_barrier) using this ID.
    Pending(u64),
}

/// A read barrier indicating that reads at or below `read_idx` are linearizable.
#[derive(Debug, Clone)]
#[must_use = "call read_idx() to check if reads are safe"]
pub struct ReadBarrier {
    pub(crate) read_idx: usize,
}

impl ReadBarrier {
    /// The decided index at which reads are linearizable.
    /// Wait until `get_decided_idx() >= read_idx()` before reading.
    pub fn read_idx(&self) -> usize {
        self.read_idx
    }

    /// Returns true if the node's decided_idx has reached the barrier.
    pub fn is_ready(&self, current_decided_idx: usize) -> bool {
        current_decided_idx >= self.read_idx
    }
}

/// Error type for linearizable read operations.
#[derive(Debug)]
pub enum ReadError {
    /// This node is not the leader.
    NotLeader {
        /// The current leader, if known.
        leader: Option<NodeId>,
    },
    /// Read-index quorum was lost (leader may have lost leadership).
    QuorumLost,
    /// Quorum not yet reached; poll again.
    QuorumPending,
}

impl Display for ReadError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ReadError::NotLeader { leader } => write!(f, "not leader (leader: {:?})", leader),
            ReadError::QuorumLost => write!(f, "read-index quorum lost"),
            ReadError::QuorumPending => write!(f, "read-index quorum pending"),
        }
    }
}

impl Error for ReadError {}

/// Error type for leader transfer operations.
#[derive(Debug)]
pub enum TransferError {
    /// Not the current leader.
    NotLeader {
        /// The current leader, if known.
        leader: Option<NodeId>,
    },
    /// Target is not a known peer.
    UnknownTarget(NodeId),
    /// A transfer is already in progress.
    TransferInProgress {
        /// The target of the current transfer.
        target: NodeId,
    },
    /// Node has been reconfigured.
    Reconfigured,
}

impl Display for TransferError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TransferError::NotLeader { leader } => {
                write!(f, "not leader (leader: {:?})", leader)
            }
            TransferError::UnknownTarget(pid) => write!(f, "unknown target: {}", pid),
            TransferError::TransferInProgress { target } => {
                write!(f, "transfer in progress to {}", target)
            }
            TransferError::Reconfigured => write!(f, "node is reconfigured"),
        }
    }
}

impl Error for TransferError {}

/// Describes how an appended entry was handled.
#[derive(Debug)]
pub enum AppendResult {
    /// Leader accepted the entry directly.
    Accepted,
    /// Entry forwarded to the known leader.
    Forwarded {
        /// The leader the entry was forwarded to.
        leader: NodeId,
    },
    /// Entry buffered (leader in Prepare phase).
    Buffered,
}

/// An error indicating a failed proposal due to the current cluster configuration being already stopped
/// or due to an invalid proposed configuration. Returns the failed proposal.
#[derive(Debug)]
pub enum ProposeErr<T>
where
    T: Entry,
{
    /// Couldn't propose entry because a reconfiguration is pending. Returns the failed, proposed entry.
    PendingReconfigEntry(T),
    /// Couldn't propose reconfiguration because a reconfiguration is already pending. Returns the failed, proposed `ClusterConfig` and the metadata.
    /// cluster config and metadata.
    PendingReconfigConfig(ClusterConfig, Option<Vec<u8>>),
    /// Couldn't propose reconfiguration because of an invalid cluster config. Contains the config
    /// error and the failed, proposed cluster config and metadata.
    ConfigError(ConfigError, ClusterConfig, Option<Vec<u8>>),
    /// Storage backend error during proposal. The entry may not have been persisted.
    Storage(StorageError),
    /// No leader is known. Entry returned to caller.
    NoLeader(T),
    /// Proposals are disabled via RuntimeConfig. Entry returned to caller.
    ProposalsDisabled(T),
    /// A leader transfer is in progress. Entry returned to caller.
    TransferInProgress(T),
}

impl<T: Entry> Display for ProposeErr<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            ProposeErr::PendingReconfigEntry(_) => {
                write!(f, "proposal rejected: reconfiguration is pending")
            }
            ProposeErr::PendingReconfigConfig(_, _) => {
                write!(
                    f,
                    "reconfiguration rejected: a reconfiguration is already pending"
                )
            }
            ProposeErr::ConfigError(e, _, _) => {
                write!(f, "reconfiguration rejected: {}", e)
            }
            ProposeErr::Storage(e) => {
                write!(f, "storage error during proposal: {}", e)
            }
            ProposeErr::NoLeader(_) => {
                write!(f, "proposal rejected: no leader known")
            }
            ProposeErr::ProposalsDisabled(_) => {
                write!(f, "proposal rejected: proposals are disabled")
            }
            ProposeErr::TransferInProgress(_) => {
                write!(f, "proposal rejected: leader transfer in progress")
            }
        }
    }
}

impl<T: Entry> std::error::Error for ProposeErr<T> {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            ProposeErr::ConfigError(e, _, _) => Some(e),
            ProposeErr::Storage(e) => Some(e),
            _ => None,
        }
    }
}

impl<T: Entry> ProposeErr<T> {
    /// Returns `true` if this is a fatal storage error (node should stop).
    pub fn is_fatal(&self) -> bool {
        matches!(self, ProposeErr::Storage(_))
    }

    /// Extract the rejected entry, if one was returned.
    pub fn into_entry(self) -> Option<T> {
        match self {
            ProposeErr::PendingReconfigEntry(e)
            | ProposeErr::NoLeader(e)
            | ProposeErr::ProposalsDisabled(e)
            | ProposeErr::TransferInProgress(e) => Some(e),
            _ => None,
        }
    }
}

/// An error returning the proposal that was failed due to that the current configuration is stopped.
#[derive(Debug)]
pub enum CompactionErr {
    /// Snapshot was called with an index that is not decided yet. Returns the currently decided index.
    UndecidedIndex(usize),
    /// Snapshot was called with an index which is already trimmed. Returns the currently compacted index.
    TrimmedIndex(usize),
    /// Trim was called with an index that is not decided by all servers yet. Returns the index decided by ALL servers currently.
    NotAllDecided(usize),
    /// Trim was called at a follower node. Trim must be called by the leader, which is the returned NodeId.
    NotCurrentLeader(NodeId),
    /// A storage backend error occurred during the compaction operation.
    Storage(StorageError),
    /// Snapshot deferred: storage backend indicated it is not ready. Retry later.
    Deferred,
}

impl CompactionErr {
    /// Returns `true` if this is a fatal storage error (node should stop).
    pub fn is_fatal(&self) -> bool {
        matches!(self, CompactionErr::Storage(_))
    }
}

impl Error for CompactionErr {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            CompactionErr::Storage(e) => Some(e),
            _ => None,
        }
    }
}

impl Display for CompactionErr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CompactionErr::Storage(e) => write!(f, "storage error during compaction: {}", e),
            other => Debug::fmt(other, f),
        }
    }
}
