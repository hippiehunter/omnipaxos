use super::{
    commands::Command,
    state::{PendingCommit, Phase, Role},
    Engine, EngineAction, LogSyncContinuation, LogSyncRequest,
};
use crate::{
    ballot_leader_election::Ballot,
    messages::{sequence_paxos::*, Message},
    storage::{Entry, StopSign, StorageOp},
    util::{AcceptedMetaData, LeaderState, NodeId, PromiseMetaData},
    InlineOrRef, ReadBarrier, ReadError, TransferError,
};
use std::sync::Arc;

impl<T: Entry> Engine<T> {
    /*** Leader ***/

    /// Handle a new leader election result.
    pub(crate) fn handle_leader(&mut self, n: Ballot) -> EngineAction<T> {
        if n <= self.s.leader_state.n_leader || n <= self.s.promise {
            return EngineAction::Done;
        }
        if self.s.pid == n.pid {
            tracing::info!(
                pid = self.s.pid,
                old_role = "follower",
                new_role = "leader",
                old_phase = ?self.s.state.1,
                new_phase = "prepare",
                ballot_n = n.n,
                ballot_pid = n.pid,
                "state_change"
            );
            self.s.leader_state =
                LeaderState::with(n, &self.s.peers, self.s.pid, self.s.leader_state.quorum);
            self.s.follower_ack = None;

            // Emit command: flush batch + set promise atomically
            let num_new = self.s.batched_entries.len();
            let mut ops = Vec::with_capacity(2);
            if num_new > 0 {
                ops.push(StorageOp::AppendEntries(
                    self.s.batched_entries.clone(),
                    false,
                ));
            }
            ops.push(StorageOp::SetPromise(n));
            self.commands.push(Command::WriteAtomic(ops));
            // Update shadow state
            if num_new > 0 {
                self.s.batched_entries.clear();
            }
            self.s.accepted_idx += num_new as u64;
            self.s.promise = n;

            /* insert my promise */
            let na = self.s.accepted_round;
            let decided_idx = self.s.decided_idx;
            let accepted_idx = self.s.accepted_idx;
            let my_promise = Promise {
                n,
                n_accepted: na,
                decided_idx,
                accepted_idx,
                log_sync: None,
            };
            let received_majority = self
                .s
                .leader_state
                .set_promise(my_promise, self.s.pid, true);
            self.s.state = (Role::Leader, Phase::Prepare);

            if received_majority {
                // Single-node: self-promise alone forms a quorum.
                return self.handle_majority_promises();
            } else {
                let prep = Prepare {
                    n,
                    decided_idx,
                    n_accepted: na,
                    accepted_idx,
                };
                for i in 0..self.s.peers.len() {
                    let pid = self.s.peers[i];
                    self.try_push_message(Message::SequencePaxos(PaxosMessage {
                        from: self.s.pid,
                        to: pid,
                        msg: PaxosMsg::Prepare(prep),
                    }));
                }
            }
        } else {
            self.become_follower();
        }
        EngineAction::Done
    }

    pub(crate) fn become_follower(&mut self) {
        if self.s.state.0 == Role::Leader {
            tracing::info!(
                pid = self.s.pid,
                old_role = "leader",
                new_role = "follower",
                "state_change"
            );
        }
        self.s.state.0 = Role::Follower;
    }

    pub(crate) fn handle_preparereq(&mut self, prepreq: PrepareReq, from: NodeId) {
        if self.s.state.0 == Role::Leader && prepreq.n <= self.s.leader_state.n_leader {
            self.s.leader_state.reset_promise(from);
            self.s.leader_state.set_latest_accept_meta(from, None);
            self.send_prepare(from);
        }
    }

    pub(crate) fn handle_forwarded_proposal(&mut self, mut entries: Vec<T>) {
        if !self.accepted_reconfiguration() {
            match self.s.state {
                (Role::Leader, Phase::Prepare) => self.s.buffered_proposals.append(&mut entries),
                (Role::Leader, Phase::Accept) => self.accept_entries_leader(entries),
                _ => self.forward_proposals(entries),
            }
        }
    }

    pub(crate) fn handle_forwarded_stopsign(&mut self, ss: StopSign) {
        if self.accepted_reconfiguration() {
            return;
        }
        if ss.next_config.configuration_id <= self.s.promise.config_id {
            tracing::warn!(
                pid = self.s.pid,
                new_config_id = ss.next_config.configuration_id,
                current_config_id = self.s.promise.config_id,
                "rejecting forwarded stopsign with non-monotonic config_id"
            );
            return;
        }
        match self.s.state {
            (Role::Leader, Phase::Prepare) => self.s.buffered_stopsign = Some(ss),
            (Role::Leader, Phase::Accept) => self.accept_stopsign_leader(ss),
            _ => self.forward_stopsign(ss),
        }
    }

    pub(crate) fn send_prepare(&mut self, to: NodeId) {
        let prep = Prepare {
            n: self.s.leader_state.n_leader,
            decided_idx: self.s.decided_idx,
            n_accepted: self.s.accepted_round,
            accepted_idx: self.s.accepted_idx,
        };
        self.try_push_message(Message::SequencePaxos(PaxosMessage {
            from: self.s.pid,
            to,
            msg: PaxosMsg::Prepare(prep),
        }));
    }

    pub(crate) fn accept_entry_leader(&mut self, entry: T) {
        if let Some(flushed_entries) = self.s.append_entry_to_batch(entry) {
            self.append_and_replicate_leader(flushed_entries);
        }
    }

    pub(crate) fn accept_entries_leader(&mut self, entries: Vec<T>) {
        if let Some(flushed_entries) = self.s.append_entries_to_batch(entries) {
            self.append_and_replicate_leader(flushed_entries);
        }
    }

    /// Append flushed entries to the leader's log and pipeline them to the
    /// followers. The leader's own vote for them is cast by
    /// `on_durability_changed` once they are durable.
    fn append_and_replicate_leader(&mut self, entries: Vec<T>) {
        let shared = Arc::new(entries.clone());
        self.commands.push(Command::AppendEntries(entries));
        self.s.accepted_idx += shared.len() as u64;
        self.send_acceptdecide(AcceptedMetaData { entries: shared });
    }

    pub(crate) fn accept_stopsign_leader(&mut self, ss: StopSign) {
        let has_batched = !self.s.batched_entries.is_empty();
        let num_new = self.s.batched_entries.len();
        // Build atomic transaction: flush entries + set stopsign
        let mut ops = Vec::with_capacity(2);
        if has_batched {
            ops.push(StorageOp::AppendEntries(
                self.s.batched_entries.clone(),
                false,
            ));
        }
        self.stage_stopsign(&mut ops, Some(ss.clone()));
        if !ops.is_empty() {
            self.commands.push(Command::WriteAtomic(ops));
        }

        // Update shadow state
        let accepted_entries_metadata = if has_batched {
            let entries = self.s.take_batched_entries();
            self.s.accepted_idx += num_new as u64;
            Some(AcceptedMetaData {
                entries: Arc::new(entries),
            })
        } else {
            None
        };
        self.s.stopsign = Some(ss.clone());
        self.s.accepted_idx += 1;

        if let Some(metadata) = accepted_entries_metadata {
            self.send_acceptdecide(metadata);
        }
        let followers: Vec<NodeId> = self.s.leader_state.get_promised_followers().to_vec();
        for pid in followers {
            self.send_accept_stopsign(pid, ss.clone(), false);
        }
    }

    /// Build AcceptSync message. This needs storage reads (create_log_sync),
    /// so returns EngineAction::NeedLogSync when it would need to read.
    /// For now, in the refactored version the Runtime handles the full send_accsync flow.
    pub(crate) fn prepare_accsync_params(&self, to: NodeId) -> (u64, u64) {
        let current_n = self.s.leader_state.n_leader;
        let PromiseMetaData {
            n_accepted: prev_round_max_promise_n,
            accepted_idx: prev_round_max_accepted_idx,
            ..
        } = self.s.leader_state.get_max_promise_meta();
        let PromiseMetaData {
            n_accepted: followers_promise_n,
            accepted_idx: followers_accepted_idx,
            pid,
            ..
        } = self.s.leader_state.get_promise_meta(to);
        let followers_decided_idx = self.s.leader_state.get_decided_idx(*pid).unwrap_or(0);
        let followers_valid_entries_idx = if *followers_promise_n == current_n {
            *followers_accepted_idx
        } else if *followers_promise_n == *prev_round_max_promise_n {
            *prev_round_max_accepted_idx.min(followers_accepted_idx)
        } else {
            followers_decided_idx
        };
        // Promise accepted_idx values count an accepted StopSign's virtual
        // slot, but the sync point is a real-log index: clamp to the real
        // log frontier or the follower re-counts the StopSign on top of an
        // already-inclusive index, deciding the StopSign one slot higher
        // than the original quorum did (divergent config_base_lsn).
        let followers_valid_entries_idx = followers_valid_entries_idx.min(self.s.real_log_head());
        (followers_valid_entries_idx, followers_decided_idx)
    }

    /// Complete sending AcceptSync after Runtime has built the LogSync.
    pub(crate) fn complete_accsync(&mut self, to: NodeId, log_sync: crate::util::LogSync<T>) {
        let current_n = self.s.leader_state.n_leader;
        let promise_meta = self.s.leader_state.get_promise_meta(to);
        let promised = (promise_meta.n_accepted, promise_meta.accepted_idx);
        self.s.leader_state.increment_seq_num_session(to);
        let acc_sync = AcceptSync {
            n: current_n,
            seq_num: self.s.leader_state.next_seq_num(to),
            decided_idx: self.s.decided_idx,
            log_sync: InlineOrRef::Inline(log_sync),
            promised,
        };
        let msg = Message::SequencePaxos(PaxosMessage {
            from: self.s.pid,
            to,
            msg: PaxosMsg::AcceptSync(acc_sync),
        });
        self.try_push_message(msg);
    }

    fn send_acceptdecide(&mut self, accepted: AcceptedMetaData<T>) {
        let decided_idx = self.s.decided_idx;
        let followers: Vec<NodeId> = self.s.leader_state.get_promised_followers().to_vec();
        for pid in followers {
            let latest_accdec = self.get_latest_accdec_message(pid);
            match latest_accdec {
                Some(accdec) => {
                    match &mut accdec.entries {
                        InlineOrRef::Inline(arc_entries) => {
                            Arc::make_mut(arc_entries).extend(accepted.entries.iter().cloned());
                        }
                        InlineOrRef::Ref(_) => {
                            // Outgoing messages from the leader are always Inline.
                            // Ref is only used on the wire after transport staging.
                        }
                    }
                    accdec.decided_idx = decided_idx;
                }
                None => {
                    let outgoing_idx = self.outgoing.len();
                    let acc = AcceptDecide {
                        n: self.s.leader_state.n_leader,
                        seq_num: self.s.leader_state.next_seq_num(pid),
                        decided_idx,
                        entries: InlineOrRef::Inline(Arc::clone(&accepted.entries)),
                    };
                    let pushed = self.try_push_message(Message::SequencePaxos(PaxosMessage {
                        from: self.s.pid,
                        to: pid,
                        msg: PaxosMsg::AcceptDecide(acc),
                    }));
                    if pushed {
                        self.s
                            .leader_state
                            .set_latest_accept_meta(pid, Some(outgoing_idx));
                    }
                }
            }
        }
    }

    fn send_accept_stopsign(&mut self, to: NodeId, ss: StopSign, resend: bool) {
        let seq_num = match resend {
            true => self.s.leader_state.get_seq_num(to),
            false => self.s.leader_state.next_seq_num(to),
        };
        let acc_ss = PaxosMsg::AcceptStopSign(AcceptStopSign {
            seq_num,
            n: self.s.leader_state.n_leader,
            ss,
        });
        self.try_push_message(Message::SequencePaxos(PaxosMessage {
            from: self.s.pid,
            to,
            msg: acc_ss,
        }));
    }

    pub(crate) fn send_decide(&mut self, to: NodeId, decided_idx: u64, resend: bool) {
        let seq_num = match resend {
            true => self.s.leader_state.get_seq_num(to),
            false => self.s.leader_state.next_seq_num(to),
        };
        let d = Decide {
            n: self.s.leader_state.n_leader,
            seq_num,
            decided_idx,
        };
        self.try_push_message(Message::SequencePaxos(PaxosMessage {
            from: self.s.pid,
            to,
            msg: PaxosMsg::Decide(d),
        }));
    }

    /// Handle majority promises. May need storage reads for sync.
    pub(crate) fn handle_majority_promises(&mut self) -> EngineAction<T> {
        let max_promise_sync = self.s.leader_state.take_max_promise_sync();
        let decided_idx = self.s.leader_state.get_max_decided_idx();

        self.sync_log_engine(self.s.leader_state.n_leader, decided_idx, max_promise_sync);

        if !self.accepted_reconfiguration() {
            if !self.s.buffered_proposals.is_empty() {
                let entries = std::mem::take(&mut self.s.buffered_proposals);
                let num = entries.len() as u64;
                self.commands.push(Command::AppendEntries(entries));
                self.s.accepted_idx += num;
            }
            if let Some(ss) = self.s.buffered_stopsign.take() {
                self.accept_stopsign_after_sync(ss);
            }
        }

        // The leader's own vote is cast by on_durability_changed once the
        // synced log is durable under this round.
        self.s.state = (Role::Leader, Phase::Accept);

        // Now need to send AcceptSync to all followers - this requires storage reads
        let followers: Vec<NodeId> = self.s.leader_state.get_promised_followers().to_vec();
        if followers.is_empty() {
            return EngineAction::Done;
        }
        let mut requests = Vec::with_capacity(followers.len());
        for pid in followers {
            let (from_idx, other_decided_idx) = self.prepare_accsync_params(pid);
            requests.push(LogSyncRequest {
                to: pid,
                from_idx,
                other_decided_idx,
                _phantom: std::marker::PhantomData,
            });
        }
        EngineAction::NeedMultipleLogSyncs(requests)
    }

    /// Helper to accept stopsign after sync (during handle_majority_promises).
    fn accept_stopsign_after_sync(&mut self, ss: StopSign) {
        let mut ops = Vec::with_capacity(1);
        self.stage_stopsign(&mut ops, Some(ss.clone()));
        if !ops.is_empty() {
            self.commands.push(Command::WriteAtomic(ops));
        }
        self.s.stopsign = Some(ss);
        self.s.accepted_idx += 1;
    }

    /// Sync log: compute ops and emit the WriteAtomic command that applies
    /// `log_sync`. A delta snapshot is merged into the stored snapshot together
    /// with this node's own decided entries below the delta, in the same atomic
    /// write that trims them, so the trimmed prefix stays readable from the
    /// snapshot (and a later AcceptSync this node sends can ship it).
    ///
    /// With storage whose log writes are durable on return, the batch sets the
    /// accepted round directly. Otherwise it opens the accept with
    /// `BeginSync` and leaves `SetAcceptedRound` (and any StopSign change) to
    /// the pending commit that `on_durability_changed` writes once the synced
    /// log is durable: a crash before then recovers the previous round's log.
    pub(crate) fn sync_log_engine(
        &mut self,
        accepted_round: Ballot,
        decided_idx: u64,
        log_sync: Option<crate::util::LogSync<T>>,
    ) {
        use crate::storage::SnapshotType;

        let async_durability = self.s.async_log_durability;
        let old_compacted_idx = self.s.compacted_idx;
        let old_decided_idx = self.s.get_decided_idx_without_stopsign();
        let mut new_compacted_idx = self.s.compacted_idx;
        let mut new_stopsign = self.s.stopsign.clone();
        let mut new_accepted_idx = self.s.accepted_idx;
        // (delta, position of its SetSnapshot in the batch, delta start)
        let mut delta_snapshot: Option<(T::Snapshot, usize, u64)> = None;
        let mut stopsign_change: Option<Option<StopSign>> = None;
        let old_real_log_head = self.s.real_log_head();
        let sync_idx = log_sync
            .as_ref()
            .map_or(old_real_log_head, |sync| sync.sync_idx);
        // A snapshot replaces the log below sync_idx as well.
        let mut unchanged_prefix = match log_sync.as_ref().map(|sync| &sync.decided_snapshot) {
            Some(Some(_)) => sync_idx.min(old_real_log_head),
            _ => sync_idx,
        };
        // A superseded uncommitted sync commits together with this one: its
        // rewrite stays uncommitted, and its StopSign change, not in storage
        // yet, is written with this sync unless this sync changes it again.
        let mut superseded_stopsign = None;
        if let Some(superseded) = self.s.pending_commit.take() {
            unchanged_prefix = unchanged_prefix.min(superseded.unchanged_prefix);
            superseded_stopsign = superseded.stopsign;
        }

        let mut sync_txn: Vec<StorageOp<T>> = Vec::with_capacity(6);
        if async_durability {
            sync_txn.push(StorageOp::BeginSync {
                round: accepted_round,
                sync_idx,
            });
        } else {
            sync_txn.push(StorageOp::SetAcceptedRound(accepted_round));
        }
        sync_txn.push(StorageOp::SetDecidedIndex(decided_idx));

        if let Some(sync) = log_sync {
            match sync.decided_snapshot {
                Some(SnapshotType::Complete(c)) => {
                    new_compacted_idx = sync.sync_idx;
                    sync_txn.push(StorageOp::Trim(sync.sync_idx));
                    sync_txn.push(StorageOp::SetCompactedIdx(sync.sync_idx));
                    sync_txn.push(StorageOp::SetSnapshot(Some(c)));
                }
                Some(SnapshotType::Delta(d)) => {
                    // The sender built the delta from our decided index:
                    // our own decided entries below it are folded in.
                    new_compacted_idx = sync.sync_idx;
                    sync_txn.push(StorageOp::Trim(sync.sync_idx));
                    sync_txn.push(StorageOp::SetCompactedIdx(sync.sync_idx));
                    delta_snapshot = Some((d, sync_txn.len(), old_decided_idx.min(sync.sync_idx)));
                }
                None => (),
            }
            new_accepted_idx = sync.sync_idx + sync.suffix.len() as u64;
            sync_txn.push(StorageOp::AppendOnPrefix(sync.sync_idx, sync.suffix, false));
            // The rewritten suffix is durable only once the host reports it.
            self.s.durable_log_idx = self.s.durable_log_idx.min(sync.sync_idx);
            match sync.stopsign {
                Some(ss) => {
                    new_stopsign = Some(ss.clone());
                    new_accepted_idx += 1;
                    stopsign_change = Some(Some(ss));
                }
                None if new_stopsign.is_some() => {
                    new_stopsign = None;
                    stopsign_change = Some(None);
                }
                None => (),
            }
        }
        let stopsign_change = stopsign_change.or(superseded_stopsign);

        if async_durability {
            self.s.pending_commit = Some(PendingCommit {
                round: accepted_round,
                unchanged_prefix,
                head: new_accepted_idx - u64::from(new_stopsign.is_some()),
                stopsign: stopsign_change,
            });
        } else {
            if let Some(ss) = stopsign_change {
                sync_txn.push(StorageOp::SetStopsign(ss));
            }
            self.s.durable_accepted_round = accepted_round;
        }

        match delta_snapshot {
            Some((delta, snapshot_at, own_to)) => {
                self.commands.push(Command::WriteAtomicWithDelta {
                    ops: sync_txn,
                    snapshot_at,
                    own_from: old_compacted_idx,
                    own_to: own_to.max(old_compacted_idx),
                    delta,
                })
            }
            None => self.commands.push(Command::WriteAtomic(sync_txn)),
        }
        self.s.accepted_round = accepted_round;
        self.s.decided_idx = decided_idx;
        self.s.compacted_idx = new_compacted_idx;
        self.s.stopsign = new_stopsign;
        self.s.accepted_idx = new_accepted_idx;
    }

    pub(crate) fn handle_promise_prepare(
        &mut self,
        prom: Promise<T>,
        from: NodeId,
    ) -> EngineAction<T> {
        if prom.n == self.s.leader_state.n_leader {
            let received_majority = self.s.leader_state.set_promise(prom, from, true);
            if received_majority {
                return self.handle_majority_promises();
            }
        }
        EngineAction::Done
    }

    pub(crate) fn handle_promise_accept(
        &mut self,
        prom: Promise<T>,
        from: NodeId,
    ) -> EngineAction<T> {
        if prom.n == self.s.leader_state.n_leader {
            self.s.leader_state.set_promise(prom, from, false);
            // Need to send AcceptSync which requires storage reads
            let (from_idx, other_decided_idx) = self.prepare_accsync_params(from);
            return EngineAction::NeedLogSync {
                from_idx,
                other_decided_idx,
                continuation: LogSyncContinuation::CompleteAcceptSync { to: from },
            };
        }
        EngineAction::Done
    }

    pub(crate) fn handle_accepted(&mut self, accepted: Accepted, from: NodeId) {
        if accepted.n == self.s.leader_state.n_leader
            && self.s.state == (Role::Leader, Phase::Accept)
        {
            tracing::debug!(
                pid = self.s.pid,
                from,
                accepted_idx = accepted.accepted_idx,
                "accepted_received"
            );
            // A follower's accepted index is durable in its storage under
            // this round, so it counts toward a write quorum.
            self.s
                .leader_state
                .set_accepted_idx(from, accepted.accepted_idx);
            self.s.leader_state.record_follower_ack(from);
            // Check if a pending transfer target has caught up
            self.check_transfer_on_accepted(from);
            self.try_decide_chosen();
        }
    }

    fn get_latest_accdec_message(&mut self, to: NodeId) -> Option<&mut AcceptDecide<T>> {
        if let Some((bal, outgoing_idx)) = self.s.leader_state.get_latest_accept_meta(to) {
            if bal == self.s.leader_state.n_leader {
                if let Some(Message::SequencePaxos(PaxosMessage {
                    msg: PaxosMsg::AcceptDecide(accdec),
                    ..
                })) = self.outgoing.get_mut(outgoing_idx)
                {
                    return Some(accdec);
                }
            }
        }
        None
    }

    pub(crate) fn handle_notaccepted(&mut self, not_acc: NotAccepted, from: NodeId) {
        if self.s.state.0 == Role::Leader && self.s.leader_state.n_leader < not_acc.n {
            tracing::debug!(
                pid = self.s.pid,
                from,
                higher_ballot_n = not_acc.n.n,
                "not_accepted"
            );
            self.s.leader_state.lost_promise(from);
        }
    }

    pub(crate) fn resend_messages_leader(&mut self) {
        match self.s.state.1 {
            Phase::Prepare => {
                let preparable_peers = self.s.leader_state.get_preparable_peers(&self.s.peers);
                for peer in preparable_peers {
                    self.send_prepare(peer);
                }
            }
            Phase::Accept => {
                if let Some(ss) = self.s.stopsign.clone() {
                    let decided_idx = self.s.decided_idx;
                    let followers: Vec<NodeId> =
                        self.s.leader_state.get_promised_followers().to_vec();
                    for follower in followers {
                        if self.s.stopsign_is_decided() {
                            self.send_decide(follower, decided_idx, true);
                        } else if self.s.leader_state.get_accepted_idx(follower)
                            != self.s.accepted_idx
                        {
                            self.send_accept_stopsign(follower, ss.clone(), true);
                        }
                    }
                }
                let preparable_peers = self.s.leader_state.get_preparable_peers(&self.s.peers);
                for peer in preparable_peers {
                    self.send_prepare(peer);
                }
            }
            Phase::Recover => (),
            Phase::None => (),
        }
    }

    /// Decide up to the highest index a write quorum of promised nodes has
    /// durably accepted, and tell the followers.
    pub(crate) fn try_decide_chosen(&mut self) {
        let chosen = self.s.leader_state.quorum_accepted_idx();
        if chosen <= self.s.decided_idx {
            return;
        }
        let old_idx = self.s.decided_idx;
        tracing::info!(
            pid = self.s.pid,
            old_idx,
            new_idx = chosen,
            count = chosen - old_idx,
            "entries_decided"
        );
        self.commands
            .push(Command::WriteAtomic(vec![StorageOp::SetDecidedIndex(
                chosen,
            )]));
        self.s.decided_idx = chosen;
        let followers: Vec<NodeId> = self.s.leader_state.get_promised_followers().to_vec();
        for pid in followers {
            match self.get_latest_accdec_message(pid) {
                Some(accdec) => accdec.decided_idx = chosen,
                None => self.send_decide(pid, chosen, false),
            }
        }
    }

    /// Handle a ReadIndexResp from a follower confirming leader authority.
    pub(crate) fn handle_read_index_resp(
        &mut self,
        resp: crate::messages::sequence_paxos::ReadIndexResp,
        _from: NodeId,
    ) {
        if resp.n != self.s.leader_state.n_leader || self.s.state.0 != Role::Leader {
            return;
        }
        if resp.ok {
            for pending in &mut self.s.pending_read_indexes {
                if pending.read_id == resp.read_id && !pending.quorum_reached {
                    pending.confirmations += 1;
                    if pending.confirmations >= pending.quorum_size {
                        pending.quorum_reached = true;
                        self.s.read_quorum_reached = true;
                    }
                }
            }
        }
    }

    /// Start a read-index request. Returns (read_id, read_idx) for single-node
    /// clusters (immediate), or (read_id, 0) for multi-node (pending quorum).
    pub(crate) fn start_read_index(&mut self) -> Result<u64, ReadError> {
        if self.s.state != (Role::Leader, Phase::Accept) {
            return Err(ReadError::NotLeader {
                leader: if self.s.promise.pid > 0 {
                    Some(self.s.promise.pid)
                } else {
                    None
                },
            });
        }
        if self.accepted_reconfiguration() {
            return Err(ReadError::Reconfigured);
        }
        let read_id = self.s.next_read_id;
        self.s.next_read_id += 1;
        let read_idx = self.s.decided_idx;

        if self.s.peers.is_empty() {
            // Single-node: immediate quorum
            self.s
                .pending_read_indexes
                .push(crate::engine::state::PendingReadIndex {
                    read_id,
                    read_idx,
                    confirmations: 1,
                    quorum_size: 1,
                    quorum_reached: true,
                });
        } else {
            let quorum_size = self.s.leader_state.quorum.prepare_quorum_size();
            // Count self
            self.s
                .pending_read_indexes
                .push(crate::engine::state::PendingReadIndex {
                    read_id,
                    read_idx,
                    confirmations: 1, // self
                    quorum_size,
                    quorum_reached: 1 >= quorum_size,
                });
            // Send ReadIndexReq to all peers
            let n = self.s.leader_state.n_leader;
            let peers = self.s.peers.clone();
            for pid in peers {
                let req = crate::messages::sequence_paxos::ReadIndexReq { n, read_id };
                self.try_push_message(Message::SequencePaxos(PaxosMessage {
                    from: self.s.pid,
                    to: pid,
                    msg: PaxosMsg::ReadIndexReq(req),
                }));
            }
        }
        Ok(read_id)
    }

    /// Poll a pending read-index. Returns the read barrier if quorum reached.
    pub(crate) fn poll_read_barrier(&mut self, read_id: u64) -> Result<ReadBarrier, ReadError> {
        let idx = self
            .s
            .pending_read_indexes
            .iter()
            .position(|p| p.read_id == read_id);
        match idx {
            Some(i) if self.s.pending_read_indexes[i].quorum_reached => {
                let pending = self.s.pending_read_indexes.remove(i);
                Ok(ReadBarrier {
                    read_idx: pending.read_idx,
                })
            }
            Some(_) => Err(ReadError::QuorumPending),
            None => Err(ReadError::QuorumLost),
        }
    }

    /// Initiate a leader transfer to the target node.
    /// Returns Err if preconditions aren't met.
    pub(crate) fn transfer_leader(&mut self, target: NodeId) -> Result<(), TransferError> {
        if self.s.state.0 != Role::Leader {
            return Err(TransferError::NotLeader {
                leader: if self.s.promise.pid > 0 {
                    Some(self.s.promise.pid)
                } else {
                    None
                },
            });
        }
        if self.accepted_reconfiguration() {
            return Err(TransferError::Reconfigured);
        }
        if let Some(ref ts) = self.s.transfer_state {
            return Err(TransferError::TransferInProgress { target: ts.target });
        }
        if !self.s.peers.contains(&target) {
            return Err(TransferError::UnknownTarget(target));
        }

        // Check if target is caught up
        let target_accepted = self.s.leader_state.get_accepted_idx(target);
        let my_accepted = self.s.accepted_idx;
        if target_accepted >= my_accepted {
            // Target is caught up, send TransferLeader immediately
            self.send_transfer_leader(target);
        } else {
            // Set transfer state to wait for target to catch up
            self.s.transfer_state = Some(crate::engine::state::TransferState {
                target,
                ticks_remaining: 50, // timeout in ticks
            });
        }
        Ok(())
    }

    /// Send the TransferLeader message to the target.
    fn send_transfer_leader(&mut self, target: NodeId) {
        let tl = crate::messages::sequence_paxos::TransferLeader {
            n: self.s.leader_state.n_leader,
            suggested_n: self.s.leader_state.n_leader.n + 1,
        };
        self.try_push_message(Message::SequencePaxos(PaxosMessage {
            from: self.s.pid,
            to: target,
            msg: PaxosMsg::TransferLeader(tl),
        }));
        // Clear transfer state
        self.s.transfer_state = None;
        tracing::info!(pid = self.s.pid, target, "leader_transfer_sent");
    }

    /// Check transfer state in handle_accepted - if target catches up, send transfer.
    pub(crate) fn check_transfer_on_accepted(&mut self, from: NodeId) {
        if let Some(ref ts) = self.s.transfer_state {
            if from == ts.target {
                let target_accepted = self.s.leader_state.get_accepted_idx(ts.target);
                if target_accepted >= self.s.accepted_idx {
                    let target = ts.target;
                    self.send_transfer_leader(target);
                }
            }
        }
    }

    /// Tick the transfer timeout. Returns true if transfer was aborted.
    pub(crate) fn tick_transfer(&mut self) -> bool {
        if let Some(ref mut ts) = self.s.transfer_state {
            if ts.ticks_remaining == 0 {
                self.s.transfer_state = None;
                return true;
            }
            ts.ticks_remaining -= 1;
        }
        false
    }

    pub(crate) fn flush_batch_leader(&mut self) {
        if self.s.batched_entries.is_empty() {
            return;
        }
        let entries = self.s.take_batched_entries();
        self.append_and_replicate_leader(entries);
    }
}
