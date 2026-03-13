use super::{
    ballot_leader_election::Ballot,
    messages::sequence_paxos::Promise,
    storage::{Entry, SnapshotType, StopSign},
};
#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};
use std::{cmp::Ordering, collections::HashMap, fmt::Debug, marker::PhantomData, sync::Arc};

/// Struct used to help another server synchronize their log with the current state of our own log.
#[derive(Clone, Debug)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub struct LogSync<T>
where
    T: Entry,
{
    /// The decided snapshot.
    pub decided_snapshot: Option<SnapshotType<T>>,
    /// The log suffix.
    pub suffix: Vec<T>,
    /// The index of the log where the entries from `suffix` should be applied at (also the compacted idx of `decided_snapshot` if it exists).
    pub sync_idx: usize,
    /// The accepted StopSign.
    pub stopsign: Option<StopSign>,
}

#[derive(Debug, Clone, Default)]
/// Promise without the log update
pub(crate) struct PromiseMetaData {
    pub n_accepted: Ballot,
    pub accepted_idx: usize,
    pub decided_idx: usize,
    pub pid: NodeId,
}

impl PartialOrd for PromiseMetaData {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        let ordering = if self.n_accepted == other.n_accepted
            && self.accepted_idx == other.accepted_idx
            && self.pid == other.pid
        {
            Ordering::Equal
        } else if self.n_accepted > other.n_accepted
            || (self.n_accepted == other.n_accepted && self.accepted_idx > other.accepted_idx)
        {
            Ordering::Greater
        } else {
            Ordering::Less
        };
        Some(ordering)
    }
}

impl PartialEq for PromiseMetaData {
    fn eq(&self, other: &Self) -> bool {
        self.n_accepted == other.n_accepted
            && self.accepted_idx == other.accepted_idx
            && self.pid == other.pid
    }
}

#[derive(Debug, Clone)]
/// The promise state of a node.
enum PromiseState {
    /// Not promised to any leader
    NotPromised,
    /// Promised to my ballot
    Promised(PromiseMetaData),
    /// Promised to a leader who's ballot is greater than mine
    PromisedHigher,
}

#[derive(Debug, Clone)]
pub(crate) struct LeaderState<T>
where
    T: Entry,
{
    pub n_leader: Ballot,
    /// All cluster member PIDs (including self)
    peers: Vec<NodeId>,
    /// Map from PID to compact index
    pid_to_idx: HashMap<NodeId, usize>,
    promises_meta: Vec<PromiseState>,
    follower_seq_nums: Vec<SequenceNumber>,
    accepted_indexes: Vec<usize>,
    max_promise_meta: PromiseMetaData,
    max_promise_sync: Option<LogSync<T>>,
    latest_accept_meta: Vec<Option<(Ballot, usize)>>,
    /// Cached list of promised follower PIDs, updated on promise state changes.
    promised_followers: Vec<NodeId>,
    pub quorum: Quorum,
}

impl<T> LeaderState<T>
where
    T: Entry,
{
    pub fn with(n_leader: Ballot, peers: &[NodeId], self_pid: NodeId, quorum: Quorum) -> Self {
        // Build all_nodes: self + peers
        let mut all_nodes = Vec::with_capacity(peers.len() + 1);
        all_nodes.push(self_pid);
        all_nodes.extend_from_slice(peers);
        let num = all_nodes.len();
        let pid_to_idx: HashMap<NodeId, usize> =
            all_nodes.iter().enumerate().map(|(i, &p)| (p, i)).collect();

        Self {
            n_leader,
            peers: all_nodes,
            pid_to_idx,
            promises_meta: vec![PromiseState::NotPromised; num],
            follower_seq_nums: vec![SequenceNumber::default(); num],
            accepted_indexes: vec![0; num],
            max_promise_meta: PromiseMetaData::default(),
            max_promise_sync: None,
            latest_accept_meta: vec![None; num],
            promised_followers: Vec::new(),
            quorum,
        }
    }

    fn idx(&self, pid: NodeId) -> usize {
        *self
            .pid_to_idx
            .get(&pid)
            .unwrap_or_else(|| panic!("Unknown PID {} in LeaderState", pid))
    }

    pub fn increment_seq_num_session(&mut self, pid: NodeId) {
        let idx = self.idx(pid);
        self.follower_seq_nums[idx].session += 1;
        self.follower_seq_nums[idx].counter = 0;
    }

    pub fn next_seq_num(&mut self, pid: NodeId) -> SequenceNumber {
        let idx = self.idx(pid);
        self.follower_seq_nums[idx].counter += 1;
        self.follower_seq_nums[idx]
    }

    pub fn get_seq_num(&mut self, pid: NodeId) -> SequenceNumber {
        self.follower_seq_nums[self.idx(pid)]
    }

    pub fn set_promise(&mut self, prom: Promise<T>, from: NodeId, check_max_prom: bool) -> bool {
        let promise_meta = PromiseMetaData {
            n_accepted: prom.n_accepted,
            accepted_idx: prom.accepted_idx,
            decided_idx: prom.decided_idx,
            pid: from,
        };
        if check_max_prom && promise_meta > self.max_promise_meta {
            self.max_promise_meta = promise_meta.clone();
            self.max_promise_sync = prom.log_sync;
        }
        let idx = self.idx(from);
        self.promises_meta[idx] = PromiseState::Promised(promise_meta);
        self.rebuild_promised_followers();
        let num_promised = self
            .promises_meta
            .iter()
            .filter(|p| matches!(p, PromiseState::Promised(_)))
            .count();
        self.quorum.is_prepare_quorum(num_promised)
    }

    pub fn reset_promise(&mut self, pid: NodeId) {
        let idx = self.idx(pid);
        self.promises_meta[idx] = PromiseState::NotPromised;
        self.rebuild_promised_followers();
    }

    pub fn lost_promise(&mut self, pid: NodeId) {
        let idx = self.idx(pid);
        self.promises_meta[idx] = PromiseState::PromisedHigher;
        self.rebuild_promised_followers();
    }

    fn rebuild_promised_followers(&mut self) {
        let leader_idx = self.idx(self.n_leader.pid);
        self.promised_followers = self
            .promises_meta
            .iter()
            .enumerate()
            .filter_map(|(i, p)| {
                if i != leader_idx && matches!(p, PromiseState::Promised(_)) {
                    Some(self.peers[i])
                } else {
                    None
                }
            })
            .collect();
    }

    pub fn take_max_promise_sync(&mut self) -> Option<LogSync<T>> {
        std::mem::take(&mut self.max_promise_sync)
    }

    pub fn get_max_promise_meta(&self) -> &PromiseMetaData {
        &self.max_promise_meta
    }

    pub fn get_max_decided_idx(&self) -> usize {
        self.promises_meta
            .iter()
            .filter_map(|p| match p {
                PromiseState::Promised(m) => Some(m.decided_idx),
                _ => None,
            })
            .max()
            .unwrap_or_default()
    }

    pub fn get_promise_meta(&self, pid: NodeId) -> &PromiseMetaData {
        let idx = self.idx(pid);
        match &self.promises_meta[idx] {
            PromiseState::Promised(metadata) => metadata,
            _ => panic!("No Metadata found for promised follower {}", pid),
        }
    }

    /// Returns the minimum accepted index across all promised nodes (including self).
    pub fn get_min_all_accepted_idx(&self) -> usize {
        self.promises_meta
            .iter()
            .enumerate()
            .filter_map(|(i, p)| match p {
                PromiseState::Promised(_) => Some(self.accepted_indexes[i]),
                _ => None,
            })
            .min()
            .unwrap_or(0)
    }

    pub fn reset_latest_accept_meta(&mut self) {
        self.latest_accept_meta.fill(None);
    }

    pub fn get_promised_followers(&self) -> &[NodeId] {
        &self.promised_followers
    }

    pub(crate) fn get_preparable_peers(&self, peers: &[NodeId]) -> Vec<NodeId> {
        peers
            .iter()
            .filter_map(|pid| {
                let idx = self.idx(*pid);
                match &self.promises_meta[idx] {
                    PromiseState::NotPromised => Some(*pid),
                    _ => None,
                }
            })
            .collect()
    }

    pub fn set_latest_accept_meta(&mut self, pid: NodeId, idx: Option<usize>) {
        let meta = idx.map(|x| (self.n_leader, x));
        let pidx = self.idx(pid);
        self.latest_accept_meta[pidx] = meta;
    }

    pub fn set_accepted_idx(&mut self, pid: NodeId, idx: usize) {
        let pidx = self.idx(pid);
        self.accepted_indexes[pidx] = idx;
    }

    pub fn get_latest_accept_meta(&self, pid: NodeId) -> Option<(Ballot, usize)> {
        self.latest_accept_meta[self.idx(pid)]
    }

    pub fn get_decided_idx(&self, pid: NodeId) -> Option<usize> {
        let idx = self.idx(pid);
        match &self.promises_meta[idx] {
            PromiseState::Promised(metadata) => Some(metadata.decided_idx),
            _ => None,
        }
    }

    pub fn get_accepted_idx(&self, pid: NodeId) -> usize {
        self.accepted_indexes[self.idx(pid)]
    }

    /// Returns true if a quorum of *promised* nodes have accepted up to `idx`.
    pub fn is_chosen(&self, idx: usize) -> bool {
        let mut count = 0;
        for (i, p) in self.promises_meta.iter().enumerate() {
            if matches!(p, PromiseState::Promised(_)) && self.accepted_indexes[i] >= idx {
                count += 1;
                if self.quorum.is_accept_quorum(count) {
                    return true;
                }
            }
        }
        false
    }
}

/// The entry read in the log.
#[derive(Debug, Clone)]
pub enum LogEntry<T>
where
    T: Entry,
{
    /// The entry is decided.
    Decided(T),
    /// The entry is NOT decided. Might be removed from the log at a later time.
    Undecided(T),
    /// The entry has been trimmed.
    Trimmed(TrimmedIndex),
    /// The entry has been snapshotted.
    Snapshotted(SnapshottedEntry<T>),
    /// This Sequence Paxos instance has been stopped for reconfiguration. The accompanying bool
    /// indicates whether the reconfiguration has been decided or not. If it is `true`, then the OmniPaxos instance for the new configuration can be started.
    StopSign(StopSign, bool),
}

impl<T: PartialEq + Entry> PartialEq for LogEntry<T>
where
    <T as Entry>::Snapshot: PartialEq,
{
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (LogEntry::Decided(v1), LogEntry::Decided(v2)) => v1 == v2,
            (LogEntry::Undecided(v1), LogEntry::Undecided(v2)) => v1 == v2,
            (LogEntry::Trimmed(idx1), LogEntry::Trimmed(idx2)) => idx1 == idx2,
            (LogEntry::Snapshotted(s1), LogEntry::Snapshotted(s2)) => s1 == s2,
            (LogEntry::StopSign(ss1, b1), LogEntry::StopSign(ss2, b2)) => ss1 == ss2 && b1 == b2,
            _ => false,
        }
    }
}

/// Convenience struct for checking if a certain index exists, is compacted or is a StopSign.
#[derive(Debug, Clone)]
pub(crate) enum IndexEntry {
    Entry,
    Compacted,
    StopSign(StopSign),
}

#[allow(missing_docs)]
#[derive(Debug, Clone)]
pub struct SnapshottedEntry<T>
where
    T: Entry,
{
    pub trimmed_idx: TrimmedIndex,
    pub snapshot: T::Snapshot,
    _p: PhantomData<T>,
}

impl<T> SnapshottedEntry<T>
where
    T: Entry,
{
    pub(crate) fn with(trimmed_idx: usize, snapshot: T::Snapshot) -> Self {
        Self {
            trimmed_idx,
            snapshot,
            _p: PhantomData,
        }
    }
}

impl<T: Entry> PartialEq for SnapshottedEntry<T>
where
    <T as Entry>::Snapshot: PartialEq,
{
    fn eq(&self, other: &Self) -> bool {
        self.trimmed_idx == other.trimmed_idx && self.snapshot == other.snapshot
    }
}

pub(crate) mod defaults {
    pub(crate) const BUFFER_SIZE: usize = 100000;
    pub(crate) const BLE_BUFFER_SIZE: usize = 100;
    pub(crate) const ELECTION_TIMEOUT: u64 = 1;
    pub(crate) const RESEND_MESSAGE_TIMEOUT: u64 = 100;
    pub(crate) const FLUSH_BATCH_TIMEOUT: u64 = 200;
}

#[allow(missing_docs)]
pub type TrimmedIndex = usize;

/// ID for an OmniPaxos node
pub type NodeId = u64;
/// ID for an OmniPaxos configuration (i.e., the set of servers in an OmniPaxos cluster)
pub type ConfigurationId = u32;

/// Used for checking the ordering of message sequences in the accept phase
#[derive(PartialEq, Eq)]
pub(crate) enum MessageStatus {
    /// Expected message sequence progression
    Expected,
    /// Identified a message sequence break
    DroppedPreceding,
    /// An already identified message sequence break
    Outdated,
}

/// Keeps track of the ordering of messages in the accept phase
#[derive(Copy, Clone, Debug, Default, PartialEq, Eq, PartialOrd, Ord)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub struct SequenceNumber {
    /// Meant to refer to a TCP session
    pub session: u64,
    /// The sequence number with respect to a session
    pub counter: u64,
}

impl SequenceNumber {
    /// Compares this sequence number with the sequence number of an incoming message.
    pub(crate) fn check_msg_status(&self, msg_seq_num: SequenceNumber) -> MessageStatus {
        if msg_seq_num.session == self.session && msg_seq_num.counter == self.counter + 1 {
            MessageStatus::Expected
        } else if msg_seq_num <= *self {
            MessageStatus::Outdated
        } else {
            MessageStatus::DroppedPreceding
        }
    }
}

pub(crate) struct LogicalClock {
    time: u64,
    timeout: u64,
}

impl LogicalClock {
    pub fn with(timeout: u64) -> Self {
        Self { time: 0, timeout }
    }

    pub fn tick_and_check_timeout(&mut self) -> bool {
        self.time += 1;
        if self.time == self.timeout {
            self.time = 0;
            true
        } else {
            false
        }
    }
}

/// Flexible quorums can be used to increase/decrease the read and write quorum sizes,
/// for different latency vs fault tolerance tradeoffs.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
#[cfg_attr(any(feature = "serde", feature = "toml_config"), derive(Deserialize))]
#[cfg_attr(feature = "serde", derive(Serialize))]
pub struct FlexibleQuorum {
    /// The number of nodes a leader needs to consult to get an up-to-date view of the log.
    pub read_quorum_size: usize,
    /// The number of acknowledgments a leader needs to commit an entry to the log
    pub write_quorum_size: usize,
}

/// The type of quorum used by the OmniPaxos cluster.
#[derive(Copy, Clone, Debug)]
pub(crate) enum Quorum {
    /// Both the read quorum and the write quorums are a majority of nodes
    Majority(usize),
    /// The read and write quorum sizes are defined by a `FlexibleQuorum`
    Flexible(FlexibleQuorum),
}

impl Quorum {
    pub(crate) fn with(flexible_quorum_config: Option<FlexibleQuorum>, num_nodes: usize) -> Self {
        match flexible_quorum_config {
            Some(FlexibleQuorum {
                read_quorum_size,
                write_quorum_size,
            }) => Quorum::Flexible(FlexibleQuorum {
                read_quorum_size,
                write_quorum_size,
            }),
            None => Quorum::Majority(num_nodes / 2 + 1),
        }
    }

    pub(crate) fn is_prepare_quorum(&self, num_nodes: usize) -> bool {
        match self {
            Quorum::Majority(majority) => num_nodes >= *majority,
            Quorum::Flexible(flex_quorum) => num_nodes >= flex_quorum.read_quorum_size,
        }
    }

    pub(crate) fn is_accept_quorum(&self, num_nodes: usize) -> bool {
        match self {
            Quorum::Majority(majority) => num_nodes >= *majority,
            Quorum::Flexible(flex_quorum) => num_nodes >= flex_quorum.write_quorum_size,
        }
    }
}

/// The entries flushed due to an append operation
pub(crate) struct AcceptedMetaData<T: Entry> {
    pub accepted_idx: usize,
    pub entries: Arc<Vec<T>>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::NoSnapshot;

    type Value = ();

    impl Entry for Value {
        type Snapshot = NoSnapshot;
    }

    #[test]
    fn preparable_peers_test() {
        let self_pid = 5;
        let peers = vec![6, 7, 8];
        let quorum = Quorum::Majority(2);
        let leader_state =
            LeaderState::<Value>::with(Ballot::with(1, 1, 1, self_pid), &peers, self_pid, quorum);
        let prep_peers = leader_state.get_preparable_peers(&peers);
        assert_eq!(prep_peers, peers);

        let self_pid = 2;
        let peers = vec![7, 1, 100, 4, 6];
        let quorum = Quorum::Majority(3);
        let leader_state =
            LeaderState::<Value>::with(Ballot::with(1, 1, 1, self_pid), &peers, self_pid, quorum);
        let prep_peers = leader_state.get_preparable_peers(&peers);
        assert_eq!(prep_peers, peers);
    }

    #[test]
    fn non_contiguous_pid_is_chosen() {
        let self_pid = 3;
        let peers = vec![7, 100];
        let quorum = Quorum::Majority(2);
        let mut ls =
            LeaderState::<Value>::with(Ballot::with(1, 1, 1, self_pid), &peers, self_pid, quorum);
        // Self promised
        let prom = crate::messages::sequence_paxos::Promise {
            n: Ballot::with(1, 1, 1, self_pid),
            n_accepted: Ballot::default(),
            decided_idx: 0,
            accepted_idx: 0,
            log_sync: None,
        };
        ls.set_promise(prom, self_pid, true);
        ls.set_accepted_idx(self_pid, 5);
        // Node 7 promised
        let prom7 = crate::messages::sequence_paxos::Promise {
            n: Ballot::with(1, 1, 1, self_pid),
            n_accepted: Ballot::default(),
            decided_idx: 0,
            accepted_idx: 0,
            log_sync: None,
        };
        ls.set_promise(prom7, 7, true);
        ls.set_accepted_idx(7, 5);
        assert!(ls.is_chosen(5));
        assert!(!ls.is_chosen(6));
    }
}
