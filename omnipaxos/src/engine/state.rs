use crate::{
    ballot_leader_election::Ballot,
    messages::sequence_paxos::Promise,
    storage::{Entry, StopSign},
    util::{LeaderState, NodeId, SequenceNumber},
};

/// State that can be snapshotted for rollback on storage failure.
/// Note: current_seq_num is intentionally NOT included because it must
/// stay advanced even on failure (the leader has already moved on).
pub(crate) struct EngineStateSnapshot<T: Entry> {
    pub decided_idx: u64,
    pub accepted_idx: u64,
    pub compacted_idx: u64,
    pub promise: Ballot,
    pub accepted_round: Ballot,
    pub stopsign: Option<StopSign>,
    pub batched_entries: Vec<T>,
    pub latest_accepted_meta: Option<(Ballot, usize)>,
    pub cached_promise_message: Option<crate::messages::sequence_paxos::Promise<T>>,
    pub state: (Role, Phase),
}

/// All in-memory state for the Paxos algorithm.
/// Absorbs fields from SequencePaxos + StateCache.
pub(crate) struct EngineState<T: Entry> {
    // --- Identity ---
    pub pid: NodeId,
    pub peers: Vec<NodeId>,

    // --- Role/Phase ---
    pub state: (Role, Phase),

    // --- Buffered proposals ---
    pub buffered_proposals: Vec<T>,
    pub buffered_stopsign: Option<StopSign>,

    // --- Message buffer ---
    pub buffer_size: usize,

    // --- Leader state ---
    pub leader_state: LeaderState<T>,

    // --- Follower accept tracking ---
    pub latest_accepted_meta: Option<(Ballot, usize)>,
    pub current_seq_num: SequenceNumber,
    pub cached_promise_message: Option<Promise<T>>,

    // --- Metrics ---
    pub dropped_messages: u64,

    // --- Leader transfer ---
    pub transfer_state: Option<TransferState>,

    // --- Linearizable reads ---
    pub pending_read_indexes: Vec<PendingReadIndex>,
    pub next_read_id: u64,
    /// Set to `true` when a read-index request reaches quorum.
    /// Checked and reset by `OmniPaxos` to update `last_quorum_ack_tick`.
    pub read_quorum_reached: bool,

    // --- Cached storage state (from StateCache) ---
    pub batch_size: usize,
    pub batched_entries: Vec<T>,
    pub promise: Ballot,
    pub accepted_round: Ballot,
    pub decided_idx: u64,
    pub accepted_idx: u64,
    pub compacted_idx: u64,
    pub stopsign: Option<StopSign>,
}

/// State for a pending linearizable read-index request.
#[derive(Clone, Debug)]
pub(crate) struct PendingReadIndex {
    /// Unique ID for this request.
    pub read_id: u64,
    /// The decided_idx at the time the read was requested.
    pub read_idx: u64,
    /// Number of confirmations received from followers.
    pub confirmations: usize,
    /// The quorum size needed.
    pub quorum_size: usize,
    /// Whether the quorum has been reached.
    pub quorum_reached: bool,
}

/// State for an in-progress leader transfer.
#[derive(Clone, Debug)]
pub(crate) struct TransferState {
    /// Target node for the transfer.
    pub target: NodeId,
    /// Ticks remaining before aborting the transfer.
    pub ticks_remaining: u64,
}

impl<T: Entry> EngineState<T> {
    // --- Batching helpers (from StateCache) ---

    /// Append an entry; returns entries to flush if batch is full.
    pub fn append_entry_to_batch(&mut self, entry: T) -> Option<Vec<T>> {
        self.batched_entries.push(entry);
        self.take_entries_if_batch_is_full()
    }

    /// Append entries; returns entries to flush if batch is full.
    pub fn append_entries_to_batch(&mut self, entries: Vec<T>) -> Option<Vec<T>> {
        self.batched_entries.extend(entries);
        self.take_entries_if_batch_is_full()
    }

    fn take_entries_if_batch_is_full(&mut self) -> Option<Vec<T>> {
        if self.batched_entries.len() >= self.batch_size {
            Some(self.take_batched_entries())
        } else {
            None
        }
    }

    pub fn take_batched_entries(&mut self) -> Vec<T> {
        std::mem::take(&mut self.batched_entries)
    }

    /// Returns whether a stopsign is decided.
    pub fn stopsign_is_decided(&self) -> bool {
        self.stopsign.is_some() && self.decided_idx == self.accepted_idx
    }

    /// Returns decided_idx without counting the stopsign.
    pub fn get_decided_idx_without_stopsign(&self) -> u64 {
        if self.stopsign_is_decided() {
            self.decided_idx - 1
        } else {
            self.decided_idx
        }
    }

    /// Snapshot mutable state for potential rollback.
    pub fn snapshot(&self) -> EngineStateSnapshot<T> {
        EngineStateSnapshot {
            decided_idx: self.decided_idx,
            accepted_idx: self.accepted_idx,
            compacted_idx: self.compacted_idx,
            promise: self.promise,
            accepted_round: self.accepted_round,
            stopsign: self.stopsign.clone(),
            batched_entries: self.batched_entries.clone(),
            latest_accepted_meta: self.latest_accepted_meta,
            cached_promise_message: self.cached_promise_message.clone(),
            state: self.state.clone(),
        }
    }

    /// Restore state from snapshot (on storage failure).
    /// Note: current_seq_num is NOT restored because the leader has already
    /// advanced its seq_num counter - restoring it would cause DroppedPreceding.
    pub fn restore(&mut self, snap: EngineStateSnapshot<T>) {
        self.decided_idx = snap.decided_idx;
        self.accepted_idx = snap.accepted_idx;
        self.compacted_idx = snap.compacted_idx;
        self.promise = snap.promise;
        self.accepted_round = snap.accepted_round;
        self.stopsign = snap.stopsign;
        self.batched_entries = snap.batched_entries;
        self.latest_accepted_meta = snap.latest_accepted_meta;
        self.cached_promise_message = snap.cached_promise_message;
        self.state = snap.state;
    }
}

#[derive(Clone, PartialEq, Debug)]
pub(crate) enum Phase {
    Prepare,
    Accept,
    Recover,
    None,
}

#[derive(Clone, PartialEq, Debug)]
pub(crate) enum Role {
    Follower,
    Leader,
}
