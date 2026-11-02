//! The pure, synchronous Paxos engine.
//!
//! The Engine owns all in-memory state and emits [`Command`]s for the Runtime
//! to execute against storage. It also pushes outgoing [`Message`]s to its
//! internal buffer.

pub(crate) mod commands;
pub(crate) mod state;

pub(crate) mod leader;
pub(crate) mod follower;

pub(crate) use state::{Phase, Role};

use crate::{
    ballot_leader_election::Ballot,
    messages::{
        sequence_paxos::*,
        Message,
    },
    storage::{Entry, StopSign},
    util::{LeaderState, NodeId, Quorum, SequenceNumber},
};
use commands::Command;
use state::EngineState;

/// The pure, synchronous Paxos engine.
///
/// Takes events (messages, ticks), updates in-memory state, emits [`Command`]s
/// for storage mutations, and pushes outgoing [`Message`]s to its buffer.
pub(crate) struct Engine<T: Entry> {
    pub(crate) s: EngineState<T>,
    pub(crate) outgoing: Vec<Message<T>>,
    pub(crate) commands: Vec<Command<T>>,
}

impl<T: Entry> Engine<T> {
    /// Create a new Engine with the given initial state.
    pub(crate) fn new(
        pid: NodeId,
        peers: Vec<NodeId>,
        buffer_size: usize,
        batch_size: usize,
        initial_state: InitialEngineState,
    ) -> Self {
        let num_nodes = peers.len() + 1;
        let quorum = Quorum::with(initial_state.flexible_quorum, num_nodes);
        let mut outgoing = Vec::with_capacity(buffer_size);

        let (state, leader_ballot) = match initial_state.promise {
            Some(b) if b != Ballot::default() => {
                let state = if peers.is_empty() {
                    (Role::Follower, Phase::None)
                } else {
                    for peer_pid in &peers {
                        let prepreq = PrepareReq { n: b };
                        outgoing.push(Message::SequencePaxos(PaxosMessage {
                            from: pid,
                            to: *peer_pid,
                            msg: PaxosMsg::PrepareReq(prepreq),
                        }));
                    }
                    (Role::Follower, Phase::Recover)
                };
                (state, b)
            }
            _ => ((Role::Follower, Phase::None), Ballot::default()),
        };

        let leader_state = LeaderState::<T>::with(leader_ballot, &peers, pid, quorum);

        Engine {
            s: EngineState {
                pid,
                peers,
                state,
                buffered_proposals: vec![],
                buffered_stopsign: None,
                buffer_size,
                leader_state,
                latest_accepted_meta: None,
                current_seq_num: SequenceNumber::default(),
                cached_promise_message: None,
                dropped_messages: 0,
                transfer_state: None,
                pending_read_indexes: Vec::new(),
                next_read_id: 1,
                read_quorum_reached: false,
                batch_size,
                batched_entries: Vec::with_capacity(batch_size),
                promise: initial_state.promise.unwrap_or_default(),
                accepted_round: initial_state.accepted_round.unwrap_or_default(),
                decided_idx: initial_state.decided_idx,
                accepted_idx: initial_state.accepted_idx,
                compacted_idx: initial_state.compacted_idx,
                stopsign: initial_state.stopsign,
            },
            outgoing,
            commands: Vec::new(),
        }
    }

    // --- Message buffer ---

    /// Try to push a message to the outgoing buffer.
    pub(crate) fn try_push_message(&mut self, msg: Message<T>) -> bool {
        if self.outgoing.len() >= self.s.buffer_size {
            self.s.dropped_messages += 1;
            tracing::warn!(
                pid = self.s.pid,
                buffer_size = self.s.buffer_size,
                total_dropped = self.s.dropped_messages,
                "message_dropped"
            );
            false
        } else {
            self.outgoing.push(msg);
            true
        }
    }

    /// Moves outgoing messages into the provided buffer.
    pub(crate) fn take_outgoing_msgs(&mut self, buffer: &mut Vec<Message<T>>) {
        if buffer.is_empty() {
            std::mem::swap(buffer, &mut self.outgoing);
        } else {
            buffer.append(&mut self.outgoing);
        }
        self.s.leader_state.reset_latest_accept_meta();
        self.s.latest_accepted_meta = None;
    }

    /// Drain and return all pending commands.
    pub(crate) fn take_commands(&mut self) -> Vec<Command<T>> {
        std::mem::take(&mut self.commands)
    }

    // --- Query methods ---

    pub(crate) fn get_state(&self) -> &(Role, Phase) {
        &self.s.state
    }

    pub(crate) fn get_promise(&self) -> Ballot {
        self.s.promise
    }

    pub(crate) fn get_decided_idx(&self) -> u64 {
        self.s.decided_idx
    }

    pub(crate) fn get_compacted_idx(&self) -> u64 {
        self.s.compacted_idx
    }

    pub(crate) fn get_accepted_idx(&self) -> u64 {
        self.s.accepted_idx
    }

    pub(crate) fn get_stopsign(&self) -> Option<StopSign> {
        self.s.stopsign.clone()
    }

    pub(crate) fn stopsign_is_decided(&self) -> bool {
        self.s.stopsign_is_decided()
    }

    pub(crate) fn get_dropped_message_count(&self) -> u64 {
        self.s.dropped_messages
    }

    fn get_current_leader(&self) -> NodeId {
        self.s.promise.pid
    }

    fn is_known_peer(&self, pid: NodeId) -> bool {
        pid == self.s.pid || self.s.peers.contains(&pid)
    }

    /// Returns whether this Sequence Paxos has been reconfigured.
    pub(crate) fn is_reconfigured(&self) -> Option<StopSign> {
        match &self.s.stopsign {
            Some(ss) if self.s.stopsign_is_decided() => Some(ss.clone()),
            _ => None,
        }
    }

    pub(crate) fn accepted_reconfiguration(&self) -> bool {
        self.s.stopsign.is_some()
    }

    // --- Proposal handling ---

    /// Propose an entry. Returns how the entry was handled.
    /// Returns `Err(entry)` if no leader is known and this node is not leader.
    pub(crate) fn propose_entry(&mut self, entry: T) -> Result<ProposeResult, T> {
        match self.s.state {
            (Role::Leader, Phase::Prepare) => {
                self.s.buffered_proposals.push(entry);
                Ok(ProposeResult::Buffered)
            }
            (Role::Leader, Phase::Accept) => {
                self.accept_entry_leader(entry);
                Ok(ProposeResult::Accepted)
            }
            _ => {
                let leader = self.get_current_leader();
                if leader > 0 && self.s.pid != leader {
                    self.forward_proposals(vec![entry]);
                    Ok(ProposeResult::Forwarded { leader })
                } else if leader == 0 {
                    Err(entry)
                } else {
                    // We are the leader but in wrong phase (Recover/None) — buffer
                    self.s.buffered_proposals.push(entry);
                    Ok(ProposeResult::Buffered)
                }
            }
        }
    }

    pub(crate) fn forward_proposals(&mut self, mut entries: Vec<T>) {
        let leader = self.get_current_leader();
        if leader > 0 && self.s.pid != leader {
            let pf = PaxosMsg::ProposalForward(entries);
            let msg = Message::SequencePaxos(PaxosMessage {
                from: self.s.pid,
                to: leader,
                msg: pf,
            });
            self.try_push_message(msg);
        } else {
            self.s.buffered_proposals.append(&mut entries);
        }
    }

    pub(crate) fn forward_stopsign(&mut self, ss: StopSign) {
        let leader = self.get_current_leader();
        if leader > 0 && self.s.pid != leader {
            let fs = PaxosMsg::ForwardStopSign(ss);
            let msg = Message::SequencePaxos(PaxosMessage {
                from: self.s.pid,
                to: leader,
                msg: fs,
            });
            self.try_push_message(msg);
        } else if self.s.buffered_stopsign.is_none() {
            self.s.buffered_stopsign = Some(ss);
        }
    }

    /// Handle reconnection to a peer.
    pub(crate) fn reconnected(&mut self, pid: NodeId) {
        if pid == self.s.pid {
            return;
        }
        let leader = self.get_current_leader();
        if pid == leader || leader == 0 {
            if pid == leader {
                self.s.state = (Role::Follower, Phase::Recover);
            }
            let prepreq = PrepareReq {
                n: self.get_promise(),
            };
            self.try_push_message(Message::SequencePaxos(PaxosMessage {
                from: self.s.pid,
                to: pid,
                msg: PaxosMsg::PrepareReq(prepreq),
            }));
        }
    }

    /// Handle an incoming Paxos message. Returns an EngineAction if storage
    /// reads are needed before completing.
    pub(crate) fn handle(&mut self, m: PaxosMessage<T>) -> EngineAction<T> {
        if !self.is_known_peer(m.from) {
            return EngineAction::Done;
        }
        match m.msg {
            PaxosMsg::PrepareReq(prepreq) => {
                self.handle_preparereq(prepreq, m.from);
                EngineAction::Done
            }
            PaxosMsg::Prepare(prep) => self.handle_prepare(prep, m.from),
            PaxosMsg::Promise(prom) => match &self.s.state {
                (Role::Leader, Phase::Prepare) => self.handle_promise_prepare(prom, m.from),
                (Role::Leader, Phase::Accept) => self.handle_promise_accept(prom, m.from),
                _ => EngineAction::Done,
            },
            PaxosMsg::AcceptSync(acc_sync) => {
                self.handle_acceptsync(acc_sync, m.from)
            }
            PaxosMsg::AcceptDecide(acc) => {
                self.handle_acceptdecide(acc);
                EngineAction::Done
            }
            PaxosMsg::NotAccepted(not_acc) => {
                self.handle_notaccepted(not_acc, m.from);
                EngineAction::Done
            }
            PaxosMsg::Accepted(accepted) => {
                self.handle_accepted(accepted, m.from);
                EngineAction::Done
            }
            PaxosMsg::Decide(d) => {
                self.handle_decide(d);
                EngineAction::Done
            }
            PaxosMsg::ProposalForward(proposals) => {
                self.handle_forwarded_proposal(proposals);
                EngineAction::Done
            }
            PaxosMsg::Compaction(c) => self.handle_compaction(c),
            PaxosMsg::AcceptStopSign(acc_ss) => {
                self.handle_accept_stopsign(acc_ss);
                EngineAction::Done
            }
            PaxosMsg::ForwardStopSign(f_ss) => {
                self.handle_forwarded_stopsign(f_ss);
                EngineAction::Done
            }
            PaxosMsg::TransferLeader(tl) => {
                self.handle_transfer_leader(tl, m.from);
                EngineAction::Done
            }
            PaxosMsg::ReadIndexReq(req) => {
                self.handle_read_index_req(req, m.from);
                EngineAction::Done
            }
            PaxosMsg::ReadIndexResp(resp) => {
                self.handle_read_index_resp(resp, m.from);
                EngineAction::Done
            }
        }
    }

    fn handle_compaction(&mut self, c: Compaction) -> EngineAction<T> {
        match c {
            Compaction::Trim(idx) => {
                self.try_trim(idx);
                EngineAction::Done
            }
            Compaction::Snapshot(idx) => {
                // Validate: can only snapshot up to decided_idx
                let decided_idx = self.s.decided_idx;
                let snapshot_idx = idx.map(|i| i.min(decided_idx));
                let compact_idx = snapshot_idx.unwrap_or(decided_idx);
                if compact_idx > self.s.compacted_idx {
                    EngineAction::NeedSnapshot {
                        compact_idx,
                        continuation: SnapshotContinuation::CompleteSnapshot {
                            local_only: true,
                            snapshot_idx,
                        },
                    }
                } else {
                    EngineAction::Done
                }
            }
        }
    }

    /// Validate and emit trim command.
    pub(crate) fn try_trim(&mut self, idx: u64) {
        let decided_idx = self.s.decided_idx;
        let log_decided_idx = self.s.get_decided_idx_without_stopsign();
        let new_compacted_idx = if idx < decided_idx {
            idx
        } else if idx == decided_idx {
            log_decided_idx
        } else {
            return; // Can't trim beyond decided
        };
        if new_compacted_idx > self.s.compacted_idx {
            tracing::info!(
                pid = self.s.pid,
                kind = "trim",
                old_compacted = self.s.compacted_idx,
                new_compacted = new_compacted_idx,
                "compaction"
            );
            self.commands.push(Command::WriteAtomic(vec![
                crate::storage::StorageOp::Trim(new_compacted_idx),
                crate::storage::StorageOp::SetCompactedIdx(new_compacted_idx),
            ]));
            self.s.compacted_idx = new_compacted_idx;
        }
    }

    // --- Timeout handlers ---

    pub(crate) fn resend_message_timeout(&mut self) {
        match self.s.state.0 {
            Role::Leader => {
                // Reset backoff so resend path can re-sync all followers
                self.s.leader_state.reset_backoff();
                self.resend_messages_leader();
            }
            Role::Follower => self.resend_messages_follower(),
        }
    }

    /// Tick per-follower backoff counters. Called once per tick when leader.
    pub(crate) fn tick_backoff(&mut self) {
        if self.s.state.0 == Role::Leader {
            self.s.leader_state.tick_backoff();
        }
    }

    /// Flush batch timeout. Returns EngineAction in case storage reads are needed.
    pub(crate) fn flush_batch_timeout(&mut self) {
        match self.s.state {
            (Role::Leader, Phase::Accept) => self.flush_batch_leader(),
            (Role::Follower, Phase::Accept) => self.flush_batch_follower(),
            _ => {}
        }
    }
}

/// Result of a successful proposal.
#[derive(Debug)]
pub(crate) enum ProposeResult {
    /// Leader accepted the entry in the Accept phase.
    Accepted,
    /// Entry forwarded to the known leader.
    Forwarded {
        /// The leader the entry was forwarded to.
        leader: NodeId,
    },
    /// Entry buffered (leader in Prepare phase or self recovering).
    Buffered,
}

/// Initial state loaded from storage during recovery.
pub(crate) struct InitialEngineState {
    pub promise: Option<Ballot>,
    pub accepted_round: Option<Ballot>,
    pub decided_idx: u64,
    pub accepted_idx: u64,
    pub compacted_idx: u64,
    pub stopsign: Option<StopSign>,
    pub flexible_quorum: Option<crate::util::FlexibleQuorum>,
}

/// Return type for Engine methods that may need async storage reads.
/// Fields are read by the Runtime, not the Engine, so some appear "unused" locally.
#[allow(dead_code)]
pub(crate) enum EngineAction<T: Entry> {
    /// All work done synchronously.
    Done,
    /// Need to build a LogSync from storage before completing.
    NeedLogSync {
        /// The index from which to build the sync.
        from_idx: u64,
        /// The decided index of the other node.
        other_decided_idx: u64,
        /// What to do with the LogSync once built.
        continuation: LogSyncContinuation,
    },
    /// Need multiple LogSyncs (for handle_majority_promises).
    NeedMultipleLogSyncs(Vec<LogSyncRequest<T>>),
    /// Need to build/merge a snapshot before completing.
    NeedSnapshot {
        compact_idx: u64,
        continuation: SnapshotContinuation<T>,
    },
}

/// What to do after building a LogSync.
pub(crate) enum LogSyncContinuation {
    /// Complete a Prepare response (follower → leader).
    CompletePrepare {
        prep_n: Ballot,
        from: NodeId,
        na: Ballot,
        accepted_idx: u64,
    },
    /// Complete an AcceptSync send (leader → follower).
    CompleteAcceptSync { to: NodeId },
}

/// Request for a LogSync build during majority promises handling.
pub(crate) struct LogSyncRequest<T: Entry> {
    pub to: NodeId,
    pub from_idx: u64,
    pub other_decided_idx: u64,
    pub _phantom: std::marker::PhantomData<T>,
}

/// What to do after building a snapshot.
/// Fields are consumed by the Runtime, not the Engine.
pub(crate) enum SnapshotContinuation<T: Entry> {
    /// Complete a trim+snapshot compaction.
    CompleteSnapshot {
        #[allow(dead_code)]
        local_only: bool,
        snapshot_idx: Option<u64>,
    },
    /// Complete a sync_log delta merge: merge `delta` into the existing snapshot.
    CompleteSyncLogDelta {
        delta: T::Snapshot,
        #[allow(dead_code)]
        compact_idx: u64,
    },
}
