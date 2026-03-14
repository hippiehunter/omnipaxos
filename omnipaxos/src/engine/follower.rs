use super::{
    commands::Command,
    state::{Phase, Role},
    Engine, EngineAction, LogSyncContinuation,
};
use crate::{
    ballot_leader_election::Ballot,
    messages::{sequence_paxos::*, Message},
    storage::{Entry, StorageOp},
    util::{MessageStatus, NodeId, SequenceNumber},
};
use std::sync::Arc;

impl<T: Entry> Engine<T> {
    /*** Follower ***/

    pub(crate) fn handle_prepare(&mut self, prep: Prepare, from: NodeId) -> EngineAction<T> {
        let old_promise = self.s.promise;
        if old_promise < prep.n || (old_promise == prep.n && self.s.state.1 == Phase::Recover) {
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
                ops.push(StorageOp::AppendEntries(self.s.batched_entries.clone()));
            }
            ops.push(StorageOp::SetPromise(prep.n));
            self.commands.push(Command::WriteAtomic(ops));
            // Update shadow state
            if num_new > 0 {
                self.s.batched_entries.clear();
            }
            self.s.accepted_idx += num_new;
            self.s.promise = prep.n;

            self.s.state = (Role::Follower, Phase::Prepare);
            self.s.current_seq_num = SequenceNumber::default();

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
        accepted_idx: usize,
        log_sync: crate::util::LogSync<T>,
    ) {
        let promise = Promise {
            n: prep_n,
            n_accepted: na,
            decided_idx: self.s.decided_idx,
            accepted_idx,
            log_sync: Some(log_sync),
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
        if self.check_valid_ballot(accsync.n) && self.s.state == (Role::Follower, Phase::Prepare) {
            tracing::debug!(
                pid = self.s.pid,
                from,
                ballot_n = accsync.n.n,
                sync_idx = accsync.log_sync.sync_idx,
                decided_idx = accsync.decided_idx,
                "accept_sync_received"
            );
            self.s.cached_promise_message = None;

            // sync_log: emit WriteAtomic
            let delta_merge =
                self.sync_log_engine(accsync.n, accsync.decided_idx, Some(accsync.log_sync));

            if self.s.stopsign.is_none() {
                self.forward_buffered_proposals();
            }

            let accepted = Accepted {
                n: accsync.n,
                accepted_idx: self.s.accepted_idx,
            };
            self.s.state = (Role::Follower, Phase::Accept);
            self.s.current_seq_num = accsync.seq_num;
            let cached_idx = self.outgoing.len();
            let pushed = self.try_push_message(Message::SequencePaxos(PaxosMessage {
                from: self.s.pid,
                to: from,
                msg: PaxosMsg::Accepted(accepted),
            }));
            if pushed {
                self.s.latest_accepted_meta = Some((accsync.n, cached_idx));
            }

            if let Some((delta, compact_idx)) = delta_merge {
                return super::EngineAction::NeedSnapshot {
                    compact_idx,
                    continuation: super::SnapshotContinuation::CompleteSyncLogDelta {
                        delta,
                        compact_idx,
                    },
                };
            }
        }
        super::EngineAction::Done
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
            tracing::debug!(
                pid = self.s.pid,
                from = acc_dec.n.pid,
                num_entries = acc_dec.entries.len(),
                decided_idx = acc_dec.decided_idx,
                "accept_decide_received"
            );
            let entries = Arc::try_unwrap(acc_dec.entries).unwrap_or_else(|arc| (*arc).clone());
            // Batch the entries
            let append_res = self.s.append_entries_to_batch(entries);
            let mut new_accepted_idx: Option<usize> = None;
            if let Some(flushed_entries) = append_res {
                let num = flushed_entries.len();
                self.commands
                    .push(Command::AppendEntries(flushed_entries));
                self.s.accepted_idx += num;
                new_accepted_idx = Some(self.s.accepted_idx);
            }

            // Update decided idx
            let flushed_after_decide =
                self.update_decided_idx_and_flush(acc_dec.decided_idx);
            if flushed_after_decide.is_some() {
                new_accepted_idx = flushed_after_decide;
            }
            if let Some(idx) = new_accepted_idx {
                self.reply_accepted(acc_dec.n, idx);
            }
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
                ops.push(StorageOp::AppendEntries(self.s.batched_entries.clone()));
            }
            ops.push(StorageOp::SetStopsign(Some(acc_ss.ss.clone())));
            self.commands.push(Command::WriteAtomic(ops));
            // Update shadow state
            if num_new > 0 {
                self.s.batched_entries.clear();
            }
            self.s.accepted_idx += num_new;
            if !acc_ss.ss.clone().next_config.nodes.is_empty() && self.s.stopsign.is_none() {
                self.s.accepted_idx += 1;
            } else if self.s.stopsign.is_some() {
                // stopsign already exists, don't increment
            } else {
                self.s.accepted_idx += 1;
            }
            self.s.stopsign = Some(acc_ss.ss);
            let new_accepted_idx = self.s.accepted_idx;
            self.reply_accepted(acc_ss.n, new_accepted_idx);
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
            let new_accepted_idx = self.update_decided_idx_and_flush(dec.decided_idx);
            if let Some(idx) = new_accepted_idx {
                self.reply_accepted(dec.n, idx);
            }
        }
    }

    /// Update decided index, flushing batch if needed.
    /// Returns Some(new_accepted_idx) if entries were flushed.
    fn update_decided_idx_and_flush(&mut self, new_decided_idx: usize) -> Option<usize> {
        if new_decided_idx <= self.s.decided_idx {
            return None;
        }
        if new_decided_idx > self.s.accepted_idx {
            // Need to flush batch to maintain decided <= accepted
            let num_new = self.s.batched_entries.len();
            let new_accepted_idx = self.s.accepted_idx + num_new;
            let clamped_decided_idx = new_decided_idx.min(new_accepted_idx);
            let mut ops = Vec::with_capacity(2);
            if num_new > 0 {
                ops.push(StorageOp::AppendEntries(self.s.batched_entries.clone()));
            }
            ops.push(StorageOp::SetDecidedIndex(clamped_decided_idx));
            self.commands.push(Command::WriteAtomic(ops));
            if num_new > 0 {
                self.s.batched_entries.clear();
            }
            self.s.accepted_idx = new_accepted_idx;
            self.s.decided_idx = clamped_decided_idx;
            Some(new_accepted_idx)
        } else {
            // Simple set decided idx
            self.commands.push(Command::WriteAtomic(vec![
                StorageOp::SetDecidedIndex(new_decided_idx),
            ]));
            self.s.decided_idx = new_decided_idx;
            None
        }
    }

    pub(crate) fn reply_accepted(&mut self, n: Ballot, accepted_idx: usize) {
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

    fn handle_sequence_num(&mut self, seq_num: crate::util::SequenceNumber, _from: NodeId) -> MessageStatus {
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
            Phase::Prepare => {
                match &self.s.cached_promise_message {
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
                }
            }
            Phase::Recover => {
                self.send_preparereq_to_all_peers();
            }
            Phase::Accept => (),
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
            // Start an election with a ballot higher than the leader's
            let _suggested_n = if tl.suggested_n > tl.n.n {
                tl.suggested_n
            } else {
                tl.n.n + 1
            };
            // We don't directly call handle_leader here - that's done by the
            // OmniPaxos layer which coordinates with BLE.
            // Instead, we store the suggested ballot so the OmniPaxos layer can use it.
            self.s.transfer_state = Some(crate::engine::state::TransferState {
                target: self.s.pid,
                ticks_remaining: 0, // signal to OmniPaxos to initiate election
            });
        }
    }

    pub(crate) fn flush_batch_follower(&mut self) {
        let accepted_idx = self.s.accepted_idx;
        if self.s.batched_entries.is_empty() {
            return;
        }
        let entries = self.s.take_batched_entries();
        let num = entries.len();
        self.commands.push(Command::AppendEntries(entries));
        self.s.accepted_idx += num;
        let new_accepted_idx = self.s.accepted_idx;
        if new_accepted_idx > accepted_idx {
            self.reply_accepted(self.get_promise(), new_accepted_idx);
        }
    }
}
