use super::super::{
    ballot_leader_election::Ballot,
    util::{LeaderState, PromiseMetaData},
};
use crate::storage::StorageResult;
use crate::util::AcceptedMetaData;
use std::sync::Arc;

use super::*;

impl<T, B> SequencePaxos<T, B>
where
    T: Entry,
    B: Storage<T>,
{
    /// Handle a new leader. Should be called when the leader election has elected a new leader with the ballot `n`
    /*** Leader ***/
    pub(crate) async fn handle_leader(&mut self, n: Ballot) -> StorageResult<()> {
        if n <= self.leader_state.n_leader || n <= self.internal_storage.get_promise() {
            return Ok(());
        }
        #[cfg(feature = "logging")]
        debug!(self.logger, "Newly elected leader: {:?}", n);
        if self.pid == n.pid {
            self.leader_state =
                LeaderState::with(n, &self.peers, self.pid, self.leader_state.quorum);
            // Atomically flush pending writes and set promise (fix B1)
            let _ = self.internal_storage.flush_batch_and_set_promise(n).await?;
            /* insert my promise */
            let na = self.internal_storage.get_accepted_round();
            let decided_idx = self.get_decided_idx();
            let accepted_idx = self.internal_storage.get_accepted_idx();
            let my_promise = Promise {
                n,
                n_accepted: na,
                decided_idx,
                accepted_idx,
                log_sync: None,
            };
            let received_majority = self.leader_state.set_promise(my_promise, self.pid, true);
            /* initialise longest chosen sequence and update state */
            self.state = (Role::Leader, Phase::Prepare);
            if received_majority {
                // Single-node: self-promise alone forms a quorum.
                self.handle_majority_promises().await?;
            } else {
                let prep = Prepare {
                    n,
                    decided_idx,
                    n_accepted: na,
                    accepted_idx,
                };
                /* send prepare */
                for i in 0..self.peers.len() {
                    let pid = self.peers[i];
                    self.try_push_message(Message::SequencePaxos(PaxosMessage {
                        from: self.pid,
                        to: pid,
                        msg: PaxosMsg::Prepare(prep),
                    }));
                }
            }
        } else {
            self.become_follower();
        }
        Ok(())
    }

    pub(crate) fn become_follower(&mut self) {
        self.state.0 = Role::Follower;
    }

    pub(crate) fn handle_preparereq(&mut self, prepreq: PrepareReq, from: NodeId) {
        #[cfg(feature = "logging")]
        debug!(self.logger, "Incoming message PrepareReq from {}", from);
        if self.state.0 == Role::Leader && prepreq.n <= self.leader_state.n_leader {
            self.leader_state.reset_promise(from);
            self.leader_state.set_latest_accept_meta(from, None);
            self.send_prepare(from);
        }
    }

    pub(crate) async fn handle_forwarded_proposal(
        &mut self,
        mut entries: Vec<T>,
    ) -> StorageResult<()> {
        if !self.accepted_reconfiguration() {
            match self.state {
                (Role::Leader, Phase::Prepare) => self.buffered_proposals.append(&mut entries),
                (Role::Leader, Phase::Accept) => self.accept_entries_leader(entries).await?,
                _ => self.forward_proposals(entries),
            }
        }
        Ok(())
    }

    pub(crate) async fn handle_forwarded_stopsign(&mut self, ss: StopSign) -> StorageResult<()> {
        if self.accepted_reconfiguration() {
            return Ok(());
        }
        match self.state {
            (Role::Leader, Phase::Prepare) => self.buffered_stopsign = Some(ss),
            (Role::Leader, Phase::Accept) => self.accept_stopsign_leader(ss).await?,
            _ => self.forward_stopsign(ss),
        }
        Ok(())
    }

    pub(crate) fn send_prepare(&mut self, to: NodeId) {
        let prep = Prepare {
            n: self.leader_state.n_leader,
            decided_idx: self.internal_storage.get_decided_idx(),
            n_accepted: self.internal_storage.get_accepted_round(),
            accepted_idx: self.internal_storage.get_accepted_idx(),
        };
        self.try_push_message(Message::SequencePaxos(PaxosMessage {
            from: self.pid,
            to,
            msg: PaxosMsg::Prepare(prep),
        }));
    }

    pub(crate) async fn accept_entry_leader(&mut self, entry: T) -> StorageResult<()> {
        let accepted_metadata = self
            .internal_storage
            .append_entry_with_batching(entry)
            .await?;
        if let Some(metadata) = accepted_metadata {
            self.leader_state
                .set_accepted_idx(self.pid, metadata.accepted_idx);
            self.send_acceptdecide(metadata);
            self.try_decide_self_accepted().await?;
        }
        Ok(())
    }

    pub(crate) async fn accept_entries_leader(&mut self, entries: Vec<T>) -> StorageResult<()> {
        let accepted_metadata = self
            .internal_storage
            .append_entries_with_batching(entries)
            .await?;
        if let Some(metadata) = accepted_metadata {
            self.leader_state
                .set_accepted_idx(self.pid, metadata.accepted_idx);
            self.send_acceptdecide(metadata);
            self.try_decide_self_accepted().await?;
        }
        Ok(())
    }

    pub(crate) async fn accept_stopsign_leader(&mut self, ss: StopSign) -> StorageResult<()> {
        let accepted_metadata = self.internal_storage.append_stopsign(ss.clone()).await?;
        if let Some(metadata) = accepted_metadata {
            self.send_acceptdecide(metadata);
        }
        let accepted_idx = self.internal_storage.get_accepted_idx();
        self.leader_state.set_accepted_idx(self.pid, accepted_idx);
        self.try_decide_self_accepted().await?;
        let followers: Vec<NodeId> = self.leader_state.get_promised_followers().to_vec();
        for pid in followers {
            self.send_accept_stopsign(pid, ss.clone(), false);
        }
        Ok(())
    }

    async fn send_accsync(&mut self, to: NodeId) -> StorageResult<()> {
        let current_n = self.leader_state.n_leader;
        let PromiseMetaData {
            n_accepted: prev_round_max_promise_n,
            accepted_idx: prev_round_max_accepted_idx,
            ..
        } = &self.leader_state.get_max_promise_meta();
        let PromiseMetaData {
            n_accepted: followers_promise_n,
            accepted_idx: followers_accepted_idx,
            pid,
            ..
        } = self.leader_state.get_promise_meta(to);
        let followers_decided_idx = self
            .leader_state
            .get_decided_idx(*pid)
            .expect("Received PromiseMetaData but not found in ld");
        // Follower can have valid accepted entries depending on which leader they were previously following
        let followers_valid_entries_idx = if *followers_promise_n == current_n {
            *followers_accepted_idx
        } else if *followers_promise_n == *prev_round_max_promise_n {
            *prev_round_max_accepted_idx.min(followers_accepted_idx)
        } else {
            followers_decided_idx
        };
        let log_sync = self
            .create_log_sync(followers_valid_entries_idx, followers_decided_idx)
            .await?;
        self.leader_state.increment_seq_num_session(to);
        let acc_sync = AcceptSync {
            n: current_n,
            seq_num: self.leader_state.next_seq_num(to),
            decided_idx: self.get_decided_idx(),
            log_sync,
        };
        let msg = Message::SequencePaxos(PaxosMessage {
            from: self.pid,
            to,
            msg: PaxosMsg::AcceptSync(acc_sync),
        });
        self.try_push_message(msg);
        Ok(())
    }

    fn send_acceptdecide(&mut self, accepted: AcceptedMetaData<T>) {
        let decided_idx = self.internal_storage.get_decided_idx();
        let followers: Vec<NodeId> = self.leader_state.get_promised_followers().to_vec();
        for pid in followers {
            let latest_accdec = self.get_latest_accdec_message(pid);
            match latest_accdec {
                // Modify existing AcceptDecide message to follower
                Some(accdec) => {
                    Arc::make_mut(&mut accdec.entries).extend(accepted.entries.iter().cloned());
                    accdec.decided_idx = decided_idx;
                }
                // Add new AcceptDecide message to follower
                None => {
                    let outgoing_idx = self.outgoing.len();
                    let acc = AcceptDecide {
                        n: self.leader_state.n_leader,
                        seq_num: self.leader_state.next_seq_num(pid),
                        decided_idx,
                        entries: Arc::clone(&accepted.entries),
                    };
                    let pushed = self.try_push_message(Message::SequencePaxos(PaxosMessage {
                        from: self.pid,
                        to: pid,
                        msg: PaxosMsg::AcceptDecide(acc),
                    }));
                    if pushed {
                        self.leader_state
                            .set_latest_accept_meta(pid, Some(outgoing_idx));
                    }
                }
            }
        }
    }

    fn send_accept_stopsign(&mut self, to: NodeId, ss: StopSign, resend: bool) {
        let seq_num = match resend {
            true => self.leader_state.get_seq_num(to),
            false => self.leader_state.next_seq_num(to),
        };
        let acc_ss = PaxosMsg::AcceptStopSign(AcceptStopSign {
            seq_num,
            n: self.leader_state.n_leader,
            ss,
        });
        self.try_push_message(Message::SequencePaxos(PaxosMessage {
            from: self.pid,
            to,
            msg: acc_ss,
        }));
    }

    pub(crate) fn send_decide(&mut self, to: NodeId, decided_idx: usize, resend: bool) {
        let seq_num = match resend {
            true => self.leader_state.get_seq_num(to),
            false => self.leader_state.next_seq_num(to),
        };
        let d = Decide {
            n: self.leader_state.n_leader,
            seq_num,
            decided_idx,
        };
        self.try_push_message(Message::SequencePaxos(PaxosMessage {
            from: self.pid,
            to,
            msg: PaxosMsg::Decide(d),
        }));
    }

    async fn handle_majority_promises(&mut self) -> StorageResult<()> {
        let max_promise_sync = self.leader_state.take_max_promise_sync();
        let decided_idx = self.leader_state.get_max_decided_idx();
        let mut new_accepted_idx = self
            .internal_storage
            .sync_log(self.leader_state.n_leader, decided_idx, max_promise_sync)
            .await?;
        if !self.accepted_reconfiguration() {
            if !self.buffered_proposals.is_empty() {
                let entries = std::mem::take(&mut self.buffered_proposals);
                new_accepted_idx = self
                    .internal_storage
                    .append_entries_without_batching(entries)
                    .await?;
            }
            if let Some(ss) = self.buffered_stopsign.take() {
                self.internal_storage.append_stopsign(ss).await?;
                new_accepted_idx = self.internal_storage.get_accepted_idx();
            }
        }
        self.state = (Role::Leader, Phase::Accept);
        self.leader_state
            .set_accepted_idx(self.pid, new_accepted_idx);
        self.try_decide_self_accepted().await?;
        let followers: Vec<NodeId> = self.leader_state.get_promised_followers().to_vec();
        for pid in followers {
            self.send_accsync(pid).await?;
        }
        Ok(())
    }

    pub(crate) async fn handle_promise_prepare(
        &mut self,
        prom: Promise<T>,
        from: NodeId,
    ) -> StorageResult<()> {
        #[cfg(feature = "logging")]
        debug!(
            self.logger,
            "Handling promise from {} in Prepare phase", from
        );
        if prom.n == self.leader_state.n_leader {
            let received_majority = self.leader_state.set_promise(prom, from, true);
            if received_majority {
                self.handle_majority_promises().await?;
            }
        }
        Ok(())
    }

    pub(crate) async fn handle_promise_accept(
        &mut self,
        prom: Promise<T>,
        from: NodeId,
    ) -> StorageResult<()> {
        #[cfg(feature = "logging")]
        {
            let (r, p) = &self.state;
            debug!(
                self.logger,
                "Self role {:?}, phase {:?}. Incoming message Promise Accept from {}", r, p, from
            );
        }
        if prom.n == self.leader_state.n_leader {
            self.leader_state.set_promise(prom, from, false);
            self.send_accsync(from).await?;
        }
        Ok(())
    }

    pub(crate) async fn handle_accepted(
        &mut self,
        accepted: Accepted,
        from: NodeId,
    ) -> StorageResult<()> {
        #[cfg(feature = "logging")]
        trace!(
            self.logger,
            "Got Accepted from {}, idx: {}, chosen_idx: {}",
            from,
            accepted.accepted_idx,
            self.internal_storage.get_decided_idx(),
        );
        if accepted.n == self.leader_state.n_leader && self.state == (Role::Leader, Phase::Accept) {
            self.leader_state
                .set_accepted_idx(from, accepted.accepted_idx);
            if accepted.accepted_idx > self.internal_storage.get_decided_idx()
                && self.leader_state.is_chosen(accepted.accepted_idx)
            {
                let decided_idx = accepted.accepted_idx;
                self.internal_storage.set_decided_idx(decided_idx).await?;
                let followers: Vec<NodeId> = self.leader_state.get_promised_followers().to_vec();
                for pid in followers {
                    let latest_accdec = self.get_latest_accdec_message(pid);
                    match latest_accdec {
                        Some(accdec) => accdec.decided_idx = decided_idx,
                        None => self.send_decide(pid, decided_idx, false),
                    }
                }
            }
        }
        Ok(())
    }

    fn get_latest_accdec_message(&mut self, to: NodeId) -> Option<&mut AcceptDecide<T>> {
        if let Some((bal, outgoing_idx)) = self.leader_state.get_latest_accept_meta(to) {
            if bal == self.leader_state.n_leader {
                if let Message::SequencePaxos(PaxosMessage {
                    msg: PaxosMsg::AcceptDecide(accdec),
                    ..
                }) = self.outgoing.get_mut(outgoing_idx).unwrap()
                {
                    return Some(accdec);
                } else {
                    #[cfg(feature = "logging")]
                    debug!(self.logger, "Cached idx is not an AcceptedDecide!");
                }
            }
        }
        None
    }

    pub(crate) fn handle_notaccepted(&mut self, not_acc: NotAccepted, from: NodeId) {
        if self.state.0 == Role::Leader && self.leader_state.n_leader < not_acc.n {
            self.leader_state.lost_promise(from);
        }
    }

    pub(crate) fn resend_messages_leader(&mut self) {
        match self.state.1 {
            Phase::Prepare => {
                // Resend Prepare
                let preparable_peers = self.leader_state.get_preparable_peers(&self.peers);
                for peer in preparable_peers {
                    self.send_prepare(peer);
                }
            }
            Phase::Accept => {
                // Resend AcceptStopSign or StopSign's decide
                if let Some(ss) = self.internal_storage.get_stopsign() {
                    let decided_idx = self.internal_storage.get_decided_idx();
                    let followers: Vec<NodeId> =
                        self.leader_state.get_promised_followers().to_vec();
                    for follower in followers {
                        if self.internal_storage.stopsign_is_decided() {
                            self.send_decide(follower, decided_idx, true);
                        } else if self.leader_state.get_accepted_idx(follower)
                            != self.internal_storage.get_accepted_idx()
                        {
                            self.send_accept_stopsign(follower, ss.clone(), true);
                        }
                    }
                }
                // Resend Prepare
                let preparable_peers = self.leader_state.get_preparable_peers(&self.peers);
                for peer in preparable_peers {
                    self.send_prepare(peer);
                }
            }
            Phase::Recover => (),
            Phase::None => (),
        }
    }

    /// Check if the leader's own accept is sufficient to form a quorum (single-node case)
    /// and advance decided_idx if so.
    async fn try_decide_self_accepted(&mut self) -> StorageResult<()> {
        let accepted_idx = self.leader_state.get_accepted_idx(self.pid);
        if accepted_idx > self.internal_storage.get_decided_idx()
            && self.leader_state.is_chosen(accepted_idx)
        {
            self.internal_storage.set_decided_idx(accepted_idx).await?;
        }
        Ok(())
    }

    pub(crate) async fn flush_batch_leader(&mut self) -> StorageResult<()> {
        if self.internal_storage.get_batched_count() == 0 {
            return Ok(());
        }
        let projected_accepted_idx =
            self.internal_storage.get_accepted_idx() + self.internal_storage.get_batched_count();
        // Temporarily set projected accepted_idx so is_chosen() can see it.
        let old_accepted_idx = self.leader_state.get_accepted_idx(self.pid);
        self.leader_state
            .set_accepted_idx(self.pid, projected_accepted_idx);
        // If the leader alone forms a quorum (e.g. single-node), decided_idx
        // advances immediately. Otherwise it stays at the current value and
        // followers' Accepted messages will advance it via handle_accepted.
        let decided_idx = if projected_accepted_idx > self.internal_storage.get_decided_idx()
            && self.leader_state.is_chosen(projected_accepted_idx)
        {
            projected_accepted_idx
        } else {
            self.internal_storage.get_decided_idx()
        };
        let result = self
            .internal_storage
            .flush_batch_and_decide_with_entries(decided_idx)
            .await;
        match result {
            Ok(accepted_metadata) => {
                if let Some(metadata) = accepted_metadata {
                    self.send_acceptdecide(metadata);
                }
                Ok(())
            }
            Err(e) => {
                // Revert leader_state on failure — entries were not persisted
                self.leader_state
                    .set_accepted_idx(self.pid, old_accepted_idx);
                Err(e)
            }
        }
    }
}
