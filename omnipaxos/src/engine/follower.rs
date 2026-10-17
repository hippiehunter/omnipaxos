use super::{
    commands::Command,
    state::{FollowerAck, Phase, Role},
    Engine, EngineAction, LogSyncContinuation,
};
use crate::{
    ballot_leader_election::Ballot,
    messages::{sequence_paxos::*, Message},
    storage::{Entry, StorageOp},
    util::{MessageStatus, NodeId, SequenceNumber},
    InlineOrRef,
};
use std::sync::Arc;

impl<T: Entry> Engine<T> {
    /*** Follower ***/

    pub(crate) fn handle_prepare(&mut self, prep: Prepare, from: NodeId) -> EngineAction<T> {
        let old_promise = self.s.promise;
        // An equal ballot is the current leader asking again: its resend
        // after a lost Promise, or a resync of a follower it saw stall. The
        // promise is unchanged, so answering is safe, and ignoring it leaves
        // the leader without this follower's promise for good.
        if old_promise <= prep.n {
            tracing::debug!(
                pid = self.s.pid,
                from,
                ballot_n = prep.n.n,
                ballot_pid = prep.n.pid,
                "prepare_received"
            );
            // Emit: flush batch + set promise atomically
            let num_new = self.s.batched_entries.len();
            let mut ops = Vec::with_capacity(2);
            if num_new > 0 {
                ops.push(StorageOp::AppendEntries(
                    self.s.batched_entries.clone(),
                    false,
                ));
            }
            ops.push(StorageOp::SetPromise(prep.n));
            self.commands.push(Command::WriteAtomic(ops));
            // Update shadow state
            if num_new > 0 {
                self.s.batched_entries.clear();
            }
            self.s.accepted_idx += num_new as u64;
            self.s.promise = prep.n;

            self.s.state = (Role::Follower, Phase::Prepare);
            self.s.current_seq_num = SequenceNumber::default();
            self.s.follower_ack = None;

            // The Promise reports the appended (in-memory) log, durable or
            // not: hiding a non-durable accepted entry could hide the only
            // witness of a chosen value.
            let na = self.s.accepted_round;
            let accepted_idx = self.s.accepted_idx;

            if na > prep.n_accepted {
                // I'm more up to date: need storage reads to build LogSync
                return EngineAction::NeedLogSync {
                    from_idx: prep.decided_idx,
                    other_decided_idx: prep.decided_idx,
                    continuation: LogSyncContinuation::CompletePrepare {
                        prep_n: prep.n,
                        from,
                        na,
                        accepted_idx,
                    },
                };
            } else if na == prep.n_accepted && accepted_idx > prep.accepted_idx {
                // Same round but I have more entries
                return EngineAction::NeedLogSync {
                    from_idx: prep.accepted_idx,
                    other_decided_idx: prep.decided_idx,
                    continuation: LogSyncContinuation::CompletePrepare {
                        prep_n: prep.n,
                        from,
                        na,
                        accepted_idx,
                    },
                };
            } else {
                // I'm equally or less up to date - no log sync needed
                let promise = Promise {
                    n: prep.n,
                    n_accepted: na,
                    decided_idx: self.s.decided_idx,
                    accepted_idx,
                    log_sync: None,
                };
                self.s.cached_promise_message = Some(promise.clone());
                self.try_push_message(Message::SequencePaxos(PaxosMessage {
                    from: self.s.pid,
                    to: from,
                    msg: PaxosMsg::Promise(promise),
                }));
            }
        }
        EngineAction::Done
    }

    /// Complete a prepare response after Runtime has built the LogSync.
    pub(crate) fn complete_prepare_with_sync(
        &mut self,
        prep_n: Ballot,
        from: NodeId,
        na: Ballot,
        accepted_idx: u64,
        log_sync: crate::util::LogSync<T>,
    ) {
        let promise = Promise {
            n: prep_n,
            n_accepted: na,
            decided_idx: self.s.decided_idx,
            accepted_idx,
            log_sync: Some(InlineOrRef::Inline(log_sync)),
        };
        self.s.cached_promise_message = Some(promise.clone());
        self.try_push_message(Message::SequencePaxos(PaxosMessage {
            from: self.s.pid,
            to: from,
            msg: PaxosMsg::Promise(promise),
        }));
    }

    pub(crate) fn handle_acceptsync(
        &mut self,
        accsync: AcceptSync<T>,
        from: NodeId,
    ) -> super::EngineAction<T> {
        if !self.check_valid_ballot(accsync.n) {
            return super::EngineAction::Done;
        }
        if self.s.state == (Role::Follower, Phase::Accept) {
            self.handle_acceptsync_in_accept(accsync, from);
            return super::EngineAction::Done;
        }
        if self.s.state == (Role::Follower, Phase::Prepare) {
            let log_sync = accsync.log_sync.into_inner();
            // The leader built this sync from a Promise of ours. If our
            // accepted state no longer matches it (e.g. we restarted and
            // recovered less of the log than we promised with), applying it
            // could leave a gap or keep a stale suffix: ask to be prepared
            // again instead.
            let stale = accsync.promised != (self.s.accepted_round, self.s.accepted_idx)
                || (log_sync.decided_snapshot.is_none() && log_sync.sync_idx > self.s.accepted_idx);
            if stale {
                tracing::warn!(
                    pid = self.s.pid,
                    from,
                    ballot_n = accsync.n.n,
                    promised_round_n = accsync.promised.0.n,
                    promised_accepted_idx = accsync.promised.1,
                    accepted_round_n = self.s.accepted_round.n,
                    accepted_idx = self.s.accepted_idx,
                    sync_idx = log_sync.sync_idx,
                    "accept_sync_does_not_match_promise"
                );
                self.reconnected(from);
                return super::EngineAction::Done;
            }
            tracing::debug!(
                pid = self.s.pid,
                from,
                ballot_n = accsync.n.n,
                sync_idx = log_sync.sync_idx,
                decided_idx = accsync.decided_idx,
                "accept_sync_received"
            );
            self.s.cached_promise_message = None;

            self.sync_log_engine(accsync.n, accsync.decided_idx, Some(log_sync));

            if self.s.stopsign.is_none() {
                self.forward_buffered_proposals();
            }

            self.s.state = (Role::Follower, Phase::Accept);
            self.s.current_seq_num = accsync.seq_num;
            // The Accepted for this round is sent by on_durability_changed
            // once the synced log and the round are durable.
            self.s.follower_ack = Some(FollowerAck {
                ballot: accsync.n,
                acked: None,
            });
        }
        super::EngineAction::Done
    }

    /// An AcceptSync for our current ballot while we already follow it: the
    /// leader answered a duplicate Promise (a second Prepare, a resent cached
    /// Promise, a duplicated message) after an earlier sync of ours, and its
    /// later messages carry this sync's newer session. If the sync leaves the
    /// log exactly as we hold it, adopt its session; ignoring it would make
    /// the next AcceptDecide look like a gap and restart the prepare. A sync
    /// that does not match our log means we missed messages: ask to be
    /// prepared again. Older or equal sessions are stale copies.
    ///
    /// The `promised` fence does not apply: it was built from the duplicate
    /// Promise, which predates our current state. Within one round every
    /// acceptor's log is a prefix of the leader's, so a sync of this round
    /// ending where our log ends carries exactly our log.
    fn handle_acceptsync_in_accept(&mut self, accsync: AcceptSync<T>, from: NodeId) {
        if accsync.seq_num.session <= self.s.current_seq_num.session {
            return;
        }
        let log_sync = accsync.log_sync.into_inner();
        let sync_head = log_sync.sync_idx + log_sync.suffix.len() as u64;
        let own_head = self.s.real_log_head() + self.s.batched_entries.len() as u64;
        if self.s.accepted_round == accsync.n
            && log_sync.stopsign == self.s.stopsign
            && sync_head == own_head
        {
            tracing::debug!(
                pid = self.s.pid,
                from,
                session = accsync.seq_num.session,
                "accept_sync_session_adopted"
            );
            self.s.current_seq_num = accsync.seq_num;
            self.update_decided_idx_and_flush(accsync.decided_idx);
        } else {
            tracing::warn!(
                pid = self.s.pid,
                from,
                session = accsync.seq_num.session,
                sync_head,
                own_head,
                "accept_sync_does_not_match_log"
            );
            self.reconnected(from);
        }
    }

    fn forward_buffered_proposals(&mut self) {
        let proposals = std::mem::take(&mut self.s.buffered_proposals);
        if !proposals.is_empty() {
            self.forward_proposals(proposals);
        }
    }

    pub(crate) fn handle_acceptdecide(&mut self, acc_dec: AcceptDecide<T>) {
        if self.check_valid_ballot(acc_dec.n)
            && self.s.state == (Role::Follower, Phase::Accept)
            && self.handle_sequence_num(acc_dec.seq_num, acc_dec.n.pid) == MessageStatus::Expected
        {
            let arc_entries = acc_dec.entries.into_inner();
            tracing::debug!(
                pid = self.s.pid,
                from = acc_dec.n.pid,
                num_entries = arc_entries.len(),
                decided_idx = acc_dec.decided_idx,
                "accept_decide_received"
            );
            let entries = Arc::try_unwrap(arc_entries).unwrap_or_else(|arc| (*arc).clone());
            // Batch the entries. The Accepted is sent by on_durability_changed
            // once they are durable.
            if let Some(flushed_entries) = self.s.append_entries_to_batch(entries) {
                let num = flushed_entries.len() as u64;
                self.commands.push(Command::AppendEntries(flushed_entries));
                self.s.accepted_idx += num;
            }
            self.update_decided_idx_and_flush(acc_dec.decided_idx);
        }
    }

    pub(crate) fn handle_accept_stopsign(&mut self, acc_ss: AcceptStopSign) {
        if self.check_valid_ballot(acc_ss.n)
            && self.s.state == (Role::Follower, Phase::Accept)
            && self.handle_sequence_num(acc_ss.seq_num, acc_ss.n.pid) == MessageStatus::Expected
        {
            // Atomically flush entries and set stopsign
            let num_new = self.s.batched_entries.len();
            let mut ops = Vec::with_capacity(2);
            if num_new > 0 {
                ops.push(StorageOp::AppendEntries(
                    self.s.batched_entries.clone(),
                    false,
                ));
            }
            self.stage_stopsign(&mut ops, Some(acc_ss.ss.clone()));
            if !ops.is_empty() {
                self.commands.push(Command::WriteAtomic(ops));
            }
            // Update shadow state
            if num_new > 0 {
                self.s.batched_entries.clear();
            }
            self.s.accepted_idx += num_new as u64;
            if self.s.stopsign.is_none() {
                self.s.accepted_idx += 1;
            }
            self.s.stopsign = Some(acc_ss.ss);
        }
    }

    pub(crate) fn handle_decide(&mut self, dec: Decide) {
        if self.check_valid_ballot(dec.n)
            && self.s.state.1 == Phase::Accept
            && self.handle_sequence_num(dec.seq_num, dec.n.pid) == MessageStatus::Expected
        {
            tracing::debug!(
                pid = self.s.pid,
                decided_idx = dec.decided_idx,
                "decide_received"
            );
            self.update_decided_idx_and_flush(dec.decided_idx);
        }
    }

    /// Update decided index, flushing the batch if the decided index reaches
    /// past the appended log.
    fn update_decided_idx_and_flush(&mut self, new_decided_idx: u64) {
        if new_decided_idx <= self.s.decided_idx {
            return;
        }
        if new_decided_idx > self.s.accepted_idx {
            // Need to flush batch to maintain decided <= accepted
            let num_new = self.s.batched_entries.len() as u64;
            let new_accepted_idx = self.s.accepted_idx + num_new;
            let clamped_decided_idx = new_decided_idx.min(new_accepted_idx);
            let mut ops = Vec::with_capacity(2);
            if num_new > 0 {
                ops.push(StorageOp::AppendEntries(
                    self.s.batched_entries.clone(),
                    false,
                ));
            }
            ops.push(StorageOp::SetDecidedIndex(clamped_decided_idx));
            self.commands.push(Command::WriteAtomic(ops));
            if num_new > 0 {
                self.s.batched_entries.clear();
            }
            self.s.accepted_idx = new_accepted_idx;
            self.s.decided_idx = clamped_decided_idx;
        } else {
            // Simple set decided idx
            self.commands
                .push(Command::WriteAtomic(vec![StorageOp::SetDecidedIndex(
                    new_decided_idx,
                )]));
            self.s.decided_idx = new_decided_idx;
        }
    }

    pub(crate) fn reply_accepted(&mut self, n: Ballot, accepted_idx: u64) {
        let latest_accepted = self.get_latest_accepted_message(n);
        match latest_accepted {
            Some(acc) => acc.accepted_idx = accepted_idx,
            None => {
                let accepted = Accepted { n, accepted_idx };
                let cached_idx = self.outgoing.len();
                let pushed = self.try_push_message(Message::SequencePaxos(PaxosMessage {
                    from: self.s.pid,
                    to: n.pid,
                    msg: PaxosMsg::Accepted(accepted),
                }));
                if pushed {
                    self.s.latest_accepted_meta = Some((n, cached_idx));
                }
            }
        }
    }

    fn get_latest_accepted_message(&mut self, n: Ballot) -> Option<&mut Accepted> {
        if let Some((ballot, outgoing_idx)) = &self.s.latest_accepted_meta {
            if *ballot == n {
                if let Some(Message::SequencePaxos(PaxosMessage {
                    msg: PaxosMsg::Accepted(a),
                    ..
                })) = self.outgoing.get_mut(*outgoing_idx)
                {
                    return Some(a);
                }
            }
        }
        None
    }

    pub(crate) fn check_valid_ballot(&mut self, message_ballot: Ballot) -> bool {
        let my_promise = self.s.promise;
        match my_promise.cmp(&message_ballot) {
            std::cmp::Ordering::Equal => true,
            std::cmp::Ordering::Greater => {
                let not_acc = NotAccepted { n: my_promise };
                self.try_push_message(Message::SequencePaxos(PaxosMessage {
                    from: self.s.pid,
                    to: message_ballot.pid,
                    msg: PaxosMsg::NotAccepted(not_acc),
                }));
                false
            }
            std::cmp::Ordering::Less => {
                self.reconnected(message_ballot.pid);
                false
            }
        }
    }

    fn handle_sequence_num(
        &mut self,
        seq_num: crate::util::SequenceNumber,
        _from: NodeId,
    ) -> MessageStatus {
        let msg_status = self.s.current_seq_num.check_msg_status(seq_num);
        match msg_status {
            MessageStatus::Expected => self.s.current_seq_num = seq_num,
            MessageStatus::DroppedPreceding => {
                self.reconnected(_from);
            }
            MessageStatus::Outdated => (),
        };
        msg_status
    }

    pub(crate) fn resend_messages_follower(&mut self) {
        match self.s.state.1 {
            Phase::Prepare => match &self.s.cached_promise_message {
                Some(promise) => {
                    self.try_push_message(Message::SequencePaxos(PaxosMessage {
                        from: self.s.pid,
                        to: promise.n.pid,
                        msg: PaxosMsg::Promise(promise.clone()),
                    }));
                }
                None => {
                    self.s.state = (Role::Follower, Phase::Recover);
                    self.send_preparereq_to_all_peers();
                }
            },
            Phase::Recover => {
                self.send_preparereq_to_all_peers();
            }
            Phase::Accept => {
                // The final Accepted of a burst has no other retry: resend
                // the last durable acknowledgement.
                if let Some(FollowerAck {
                    ballot,
                    acked: Some(acked),
                }) = self.s.follower_ack
                {
                    if ballot == self.s.promise {
                        self.reply_accepted(ballot, acked);
                    }
                }
            }
            Phase::None => (),
        }
    }

    fn send_preparereq_to_all_peers(&mut self) {
        let prepreq = PrepareReq {
            n: self.get_promise(),
        };
        for i in 0..self.s.peers.len() {
            let peer = self.s.peers[i];
            self.try_push_message(Message::SequencePaxos(PaxosMessage {
                from: self.s.pid,
                to: peer,
                msg: PaxosMsg::PrepareReq(prepreq),
            }));
        }
    }

    /// Handle a ReadIndexReq: leader is checking if we still follow it.
    pub(crate) fn handle_read_index_req(
        &mut self,
        req: crate::messages::sequence_paxos::ReadIndexReq,
        from: NodeId,
    ) {
        let ok = self.s.promise == req.n;
        let resp = crate::messages::sequence_paxos::ReadIndexResp {
            n: req.n,
            read_id: req.read_id,
            ok,
        };
        self.try_push_message(Message::SequencePaxos(
            crate::messages::sequence_paxos::PaxosMessage {
                from: self.s.pid,
                to: from,
                msg: crate::messages::sequence_paxos::PaxosMsg::ReadIndexResp(resp),
            },
        ));
    }

    /// Handle a TransferLeader message: the current leader is asking us to take over.
    pub(crate) fn handle_transfer_leader(
        &mut self,
        tl: crate::messages::sequence_paxos::TransferLeader,
        _from: NodeId,
    ) {
        // Only accept if the ballot is from our current leader
        if tl.n >= self.s.promise {
            // The election itself is started by the OmniPaxos layer, which
            // coordinates with BLE: a zero-tick transfer state targeting
            // self is the handoff marker its tick() consumes. The takeover
            // ballot is derived from the current promise there, which the
            // leader's `suggested_n` (promise.n + 1) equals.
            self.s.transfer_state = Some(crate::engine::state::TransferState {
                target: self.s.pid,
                ticks_remaining: 0,
            });
        }
    }

    pub(crate) fn flush_batch_follower(&mut self) {
        if self.s.batched_entries.is_empty() {
            return;
        }
        let entries = self.s.take_batched_entries();
        let num = entries.len() as u64;
        self.commands.push(Command::AppendEntries(entries));
        self.s.accepted_idx += num;
    }
}
