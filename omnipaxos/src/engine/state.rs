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
    pub durable_log_idx: u64,
    pub durable_accepted_round: Ballot,
    pub pending_commit: Option<PendingCommit>,
    pub follower_ack: Option<FollowerAck>,
}

/// The state `Engine::on_durability_changed` mutates, for rollback when the
/// writes it emits fail.
pub(crate) struct DurabilitySnapshot {
    pub decided_idx: u64,
    pub durable_log_idx: u64,
    pub durable_accepted_round: Ballot,
    pub pending_commit: Option<PendingCommit>,
    pub follower_ack: Option<FollowerAck>,
    pub latest_accepted_meta: Option<(Ballot, usize)>,
    pub leader_self_accepted_idx: u64,
}

/// An accept of `round` whose log rewrite is not yet committed in storage. It
/// commits with `SetAcceptedRound(round)` once the real log below `head` is
/// durable.
#[derive(Clone, Debug)]
pub(crate) struct PendingCommit {
    pub round: Ballot,
    /// The real-log prefix the sync left as it was: its `sync_idx`, or less
    /// when a snapshot replaced entries below `sync_idx`. Until the commit,
    /// a crash recovers only this prefix of the synced log.
    pub unchanged_prefix: u64,
    /// The real-log head the sync produced.
    pub head: u64,
    /// The `SetStopsign` value to write with the commit, if the sync or a
    /// later AcceptStopSign changed the StopSign.
    pub stopsign: Option<Option<StopSign>>,
}

/// What this follower has acknowledged to the leader of `ballot`.
#[derive(Clone, Copy, Debug)]
pub(crate) struct FollowerAck {
    pub ballot: Ballot,
    /// The last accepted index sent in an `Accepted`, `None` before the first.
    pub acked: Option<u64>,
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

    // --- Log durability ---
    /// `false` when `Ok` from `write_atomically` means every op is durable;
    /// `true` when the host reports log durability via `on_log_durable`.
    pub async_log_durability: bool,
    /// The real-log prefix `[0, durable_log_idx)` is durable in storage.
    pub durable_log_idx: u64,
    /// The accepted round as last committed to storage.
    pub durable_accepted_round: Ballot,
    /// An AcceptSync whose `SetAcceptedRound` waits for its log to be durable.
    pub pending_commit: Option<PendingCommit>,
    /// The follower's acknowledgements to its current leader.
    pub follower_ack: Option<FollowerAck>,
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

    /// The end of the real (non-StopSign) log: `accepted_idx` without the
    /// slot an accepted StopSign occupies.
    pub fn real_log_head(&self) -> u64 {
        self.accepted_idx - u64::from(self.stopsign.is_some())
    }

    /// The real-log prefix that is durable and not covered by an uncommitted
    /// log rewrite.
    pub fn committed_real_prefix(&self) -> u64 {
        let prefix = self.durable_log_idx.min(self.real_log_head());
        match &self.pending_commit {
            Some(pending) => prefix.min(pending.unchanged_prefix),
            None => prefix,
        }
    }

    /// The committed accepted index: the committed real prefix plus the
    /// StopSign slot once the StopSign and the whole real log before it are
    /// committed.
    pub fn committed_accepted_idx(&self) -> u64 {
        let prefix = self.committed_real_prefix();
        if self.stopsign.is_some()
            && self.pending_commit.is_none()
            && prefix == self.real_log_head()
        {
            prefix + 1
        } else {
            prefix
        }
    }

    /// The accepted index this node may vote for in `ballot`: its committed
    /// accepted index, if its committed accepted round is `ballot`.
    pub fn vote_frontier(&self, ballot: Ballot) -> Option<u64> {
        (self.durable_accepted_round == ballot).then(|| self.committed_accepted_idx())
    }

    /// Snapshot the state that durability settlement mutates.
    pub fn durability_snapshot(&self) -> DurabilitySnapshot {
        DurabilitySnapshot {
            decided_idx: self.decided_idx,
            durable_log_idx: self.durable_log_idx,
            durable_accepted_round: self.durable_accepted_round,
            pending_commit: self.pending_commit.clone(),
            follower_ack: self.follower_ack,
            latest_accepted_meta: self.latest_accepted_meta,
            leader_self_accepted_idx: self.leader_state.get_accepted_idx(self.pid),
        }
    }

    /// Restore state from a durability snapshot (on storage failure).
    pub fn restore_durability(&mut self, snap: DurabilitySnapshot) {
        self.decided_idx = snap.decided_idx;
        self.durable_log_idx = snap.durable_log_idx;
        self.durable_accepted_round = snap.durable_accepted_round;
        self.pending_commit = snap.pending_commit;
        self.follower_ack = snap.follower_ack;
        self.latest_accepted_meta = snap.latest_accepted_meta;
        self.leader_state
            .set_accepted_idx(self.pid, snap.leader_self_accepted_idx);
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
            durable_log_idx: self.durable_log_idx,
            durable_accepted_round: self.durable_accepted_round,
            pending_commit: self.pending_commit.clone(),
            follower_ack: self.follower_ack,
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
        self.durable_log_idx = snap.durable_log_idx;
        self.durable_accepted_round = snap.durable_accepted_round;
        self.pending_commit = snap.pending_commit;
        self.follower_ack = snap.follower_ack;
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
