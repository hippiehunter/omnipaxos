use super::super::ballot_leader_election::Ballot;

use super::*;

use crate::storage::StorageResult;
use crate::util::MessageStatus;
use std::sync::Arc;

impl<T, B> SequencePaxos<T, B>
where
    T: Entry,
    B: Storage<T>,
{
    /*** Follower ***/
    pub(crate) async fn handle_prepare(
        &mut self,
        prep: Prepare,
        from: NodeId,
    ) -> StorageResult<()> {
        let old_promise = self.internal_storage.get_promise();
        if old_promise < prep.n || (old_promise == prep.n && self.state.1 == Phase::Recover) {
            // Atomically flush pending writes and set promise (fix B1)
            let _ = self
                .internal_storage
                .flush_batch_and_set_promise(prep.n)
                .await?;
            self.state = (Role::Follower, Phase::Prepare);
            self.current_seq_num = SequenceNumber::default();
            let na = self.internal_storage.get_accepted_round();
            let accepted_idx = self.internal_storage.get_accepted_idx();
            let log_sync = if na > prep.n_accepted {
                // I'm more up to date: send leader what he is missing after his decided index.
                Some(
                    self.create_log_sync(prep.decided_idx, prep.decided_idx)
                        .await?,
                )
            } else if na == prep.n_accepted && accepted_idx > prep.accepted_idx {
                // I'm more up to date and in same round: send leader what he is missing after his
                // accepted index.
                Some(
                    self.create_log_sync(prep.accepted_idx, prep.decided_idx)
                        .await?,
                )
            } else {
                // I'm equally or less up to date
                None
            };
            let promise = Promise {
                n: prep.n,
                n_accepted: na,
                decided_idx: self.internal_storage.get_decided_idx(),
                accepted_idx,
                log_sync,
            };
            self.cached_promise_message = Some(promise.clone());
            self.try_push_message(Message::SequencePaxos(PaxosMessage {
                from: self.pid,
                to: from,
                msg: PaxosMsg::Promise(promise),
            }));
        }
        Ok(())
    }

    pub(crate) async fn handle_acceptsync(
        &mut self,
        accsync: AcceptSync<T>,
        from: NodeId,
    ) -> StorageResult<()> {
        if self.check_valid_ballot(accsync.n) && self.state == (Role::Follower, Phase::Prepare) {
            self.cached_promise_message = None;
            let new_accepted_idx = self
                .internal_storage
                .sync_log(accsync.n, accsync.decided_idx, Some(accsync.log_sync))
                .await?;
            if self.internal_storage.get_stopsign().is_none() {
                self.forward_buffered_proposals();
            }
            let accepted = Accepted {
                n: accsync.n,
                accepted_idx: new_accepted_idx,
            };
            self.state = (Role::Follower, Phase::Accept);
            self.current_seq_num = accsync.seq_num;
            let cached_idx = self.outgoing.len();
            let pushed = self.try_push_message(Message::SequencePaxos(PaxosMessage {
                from: self.pid,
                to: from,
                msg: PaxosMsg::Accepted(accepted),
            }));
            if pushed {
                self.latest_accepted_meta = Some((accsync.n, cached_idx));
            }
        }
        Ok(())
    }

    fn forward_buffered_proposals(&mut self) {
        let proposals = std::mem::take(&mut self.buffered_proposals);
        if !proposals.is_empty() {
            self.forward_proposals(proposals);
        }
    }

    pub(crate) async fn handle_acceptdecide(
        &mut self,
        acc_dec: AcceptDecide<T>,
    ) -> StorageResult<()> {
        if self.check_valid_ballot(acc_dec.n)
            && self.state == (Role::Follower, Phase::Accept)
            && self.handle_sequence_num(acc_dec.seq_num, acc_dec.n.pid) == MessageStatus::Expected
        {
            let entries = Arc::try_unwrap(acc_dec.entries).unwrap_or_else(|arc| (*arc).clone());
            let mut new_accepted_idx = self
                .internal_storage
                .append_entries_and_get_accepted_idx(entries)
                .await?;
            let flushed_after_decide = self
                .update_decided_idx_and_get_accepted_idx(acc_dec.decided_idx)
                .await?;
            if flushed_after_decide.is_some() {
                new_accepted_idx = flushed_after_decide;
            }
            if let Some(idx) = new_accepted_idx {
                self.reply_accepted(acc_dec.n, idx);
            }
        }
        Ok(())
    }

    pub(crate) async fn handle_accept_stopsign(
        &mut self,
        acc_ss: AcceptStopSign,
    ) -> StorageResult<()> {
        if self.check_valid_ballot(acc_ss.n)
            && self.state == (Role::Follower, Phase::Accept)
            && self.handle_sequence_num(acc_ss.seq_num, acc_ss.n.pid) == MessageStatus::Expected
        {
            // Atomically flush entries and set stopsign (fix B4)
            let new_accepted_idx = self
                .internal_storage
                .flush_batch_and_set_stopsign(Some(acc_ss.ss))
                .await?;
            self.reply_accepted(acc_ss.n, new_accepted_idx);
        }
        Ok(())
    }

    pub(crate) async fn handle_decide(&mut self, dec: Decide) -> StorageResult<()> {
        if self.check_valid_ballot(dec.n)
            && self.state.1 == Phase::Accept
            && self.handle_sequence_num(dec.seq_num, dec.n.pid) == MessageStatus::Expected
        {
            let new_accepted_idx = self
                .update_decided_idx_and_get_accepted_idx(dec.decided_idx)
                .await?;
            if let Some(idx) = new_accepted_idx {
                self.reply_accepted(dec.n, idx);
            }
        }
        Ok(())
    }

    /// To maintain decided index <= accepted index, batched entries may be flushed.
    /// Returns `Some(new_accepted_idx)` if entries are flushed, otherwise `None`.
    async fn update_decided_idx_and_get_accepted_idx(
        &mut self,
        new_decided_idx: usize,
    ) -> StorageResult<Option<usize>> {
        if new_decided_idx <= self.internal_storage.get_decided_idx() {
            return Ok(None);
        }
        if new_decided_idx > self.internal_storage.get_accepted_idx() {
            // Atomically flush batch and set decided index (fix B3)
            // Clamping to new accepted_idx is handled inside the method
            let new_accepted_idx = self
                .internal_storage
                .flush_batch_and_set_decided_idx(new_decided_idx)
                .await?;
            Ok(Some(new_accepted_idx))
        } else {
            self.internal_storage
                .set_decided_idx(new_decided_idx)
                .await?;
            Ok(None)
        }
    }

    fn reply_accepted(&mut self, n: Ballot, accepted_idx: usize) {
        let latest_accepted = self.get_latest_accepted_message(n);
        match latest_accepted {
            Some(acc) => acc.accepted_idx = accepted_idx,
            None => {
                let accepted = Accepted { n, accepted_idx };
                let cached_idx = self.outgoing.len();
                let pushed = self.try_push_message(Message::SequencePaxos(PaxosMessage {
                    from: self.pid,
                    to: n.pid,
                    msg: PaxosMsg::Accepted(accepted),
                }));
                if pushed {
                    self.latest_accepted_meta = Some((n, cached_idx));
                }
            }
        }
    }

    fn get_latest_accepted_message(&mut self, n: Ballot) -> Option<&mut Accepted> {
        if let Some((ballot, outgoing_idx)) = &self.latest_accepted_meta {
            if *ballot == n {
                if let Message::SequencePaxos(PaxosMessage {
                    msg: PaxosMsg::Accepted(a),
                    ..
                }) = self.outgoing.get_mut(*outgoing_idx).unwrap()
                {
                    return Some(a);
                } else {
                    #[cfg(feature = "logging")]
                    debug!(self.logger, "Cached idx is not an Accepted message!");
                }
            }
        }
        None
    }

    /// Also returns whether the message's ballot was promised
    fn check_valid_ballot(&mut self, message_ballot: Ballot) -> bool {
        let my_promise = self.internal_storage.get_promise();
        match my_promise.cmp(&message_ballot) {
            std::cmp::Ordering::Equal => true,
            std::cmp::Ordering::Greater => {
                let not_acc = NotAccepted { n: my_promise };
                #[cfg(feature = "logging")]
                trace!(
                    self.logger,
                    "NotAccepted. My promise: {:?}, theirs: {:?}",
                    my_promise,
                    message_ballot
                );
                self.try_push_message(Message::SequencePaxos(PaxosMessage {
                    from: self.pid,
                    to: message_ballot.pid,
                    msg: PaxosMsg::NotAccepted(not_acc),
                }));
                false
            }
            std::cmp::Ordering::Less => {
                // Should never happen, but to be safe send PrepareReq
                #[cfg(feature = "logging")]
                warn!(
                    self.logger,
                    "Received non-prepare message from a leader I've never promised. My: {:?}, theirs: {:?}", my_promise, message_ballot
                );
                self.reconnected(message_ballot.pid);
                false
            }
        }
    }

    /// Also returns the MessageStatus of the sequence based on the incoming sequence number.
    fn handle_sequence_num(&mut self, seq_num: SequenceNumber, _from: NodeId) -> MessageStatus {
        let msg_status = self.current_seq_num.check_msg_status(seq_num);
        match msg_status {
            MessageStatus::Expected => self.current_seq_num = seq_num,
            // A gap in the accept sequence means we missed one or more messages.
            // Trigger re-sync from the leader: transition to Recover phase and
            // send PrepareReq, which causes the leader to re-prepare us via
            // Prepare → Promise → AcceptSync with the missing entries.
            MessageStatus::DroppedPreceding => {
                self.reconnected(_from);
            }
            MessageStatus::Outdated => (),
        };
        msg_status
    }

    pub(crate) fn resend_messages_follower(&mut self) {
        match self.state.1 {
            Phase::Prepare => {
                // Resend Promise
                match &self.cached_promise_message {
                    Some(promise) => {
                        self.try_push_message(Message::SequencePaxos(PaxosMessage {
                            from: self.pid,
                            to: promise.n.pid,
                            msg: PaxosMsg::Promise(promise.clone()),
                        }));
                    }
                    None => {
                        // Shouldn't be possible to be in prepare phase without having
                        // cached the promise sent as a response to the prepare
                        #[cfg(feature = "logging")]
                        warn!(self.logger, "In Prepare phase without a cached promise!");
                        self.state = (Role::Follower, Phase::Recover);
                        self.send_preparereq_to_all_peers();
                    }
                }
            }
            Phase::Recover => {
                // Resend PrepareReq
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
        for i in 0..self.peers.len() {
            let peer = self.peers[i];
            self.try_push_message(Message::SequencePaxos(PaxosMessage {
                from: self.pid,
                to: peer,
                msg: PaxosMsg::PrepareReq(prepreq),
            }));
        }
    }

    pub(crate) async fn flush_batch_follower(&mut self) -> StorageResult<()> {
        let accepted_idx = self.internal_storage.get_accepted_idx();
        let new_accepted_idx = self.internal_storage.flush_batch().await?;
        if new_accepted_idx > accepted_idx {
            self.reply_accepted(self.get_promise(), new_accepted_idx);
        }
        Ok(())
    }
}
