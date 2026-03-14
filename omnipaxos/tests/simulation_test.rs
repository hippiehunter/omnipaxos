use omnipaxos::{
    ballot_leader_election::Ballot,
    messages::{
        sequence_paxos::{PaxosMessage, PaxosMsg},
        Message,
    },
    storage::{
        memory_storage::MemoryStorage, StopSign, Storage, StorageOp, StorageResult,
    },
    util::{FlexibleQuorum, LogEntry},
    ClusterConfig, CompactionErr, OmniPaxos, OmniPaxosConfig, ProposeErr, ReadError,
    ReconfigurationStatus, ServerConfig,
};
use std::cell::RefCell;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::rc::Rc;

mod test_utils;
use test_utils::{Value, ValueSnapshot};

// ===========================================================================
// SharedMemoryStorage — wraps MemoryStorage behind Rc<RefCell<>> so the
// harness can clone the storage state when simulating a crash.
// ===========================================================================

struct SharedMemoryStorage {
    inner: Rc<RefCell<MemoryStorage<Value>>>,
}

impl SharedMemoryStorage {
    fn new_with_handle() -> (Self, Rc<RefCell<MemoryStorage<Value>>>) {
        let rc = Rc::new(RefCell::new(MemoryStorage::default()));
        let handle = rc.clone();
        (SharedMemoryStorage { inner: rc }, handle)
    }

    fn from_memory_storage(ms: MemoryStorage<Value>) -> (Self, Rc<RefCell<MemoryStorage<Value>>>) {
        let rc = Rc::new(RefCell::new(ms));
        let handle = rc.clone();
        (SharedMemoryStorage { inner: rc }, handle)
    }
}

impl Storage<Value> for SharedMemoryStorage {
    async fn write_atomically(&mut self, ops: Vec<StorageOp<Value>>) -> StorageResult<()> {
        self.inner.borrow_mut().write_atomically(ops).await
    }
    async fn load_state(&self) -> StorageResult<omnipaxos::storage::PersistedState<Value>> {
        self.inner.borrow().load_state().await
    }
    async fn get_entries(&self, from: usize, to: usize) -> StorageResult<Vec<Value>> {
        self.inner.borrow().get_entries(from, to).await
    }
    async fn get_suffix(&self, from: usize) -> StorageResult<Vec<Value>> {
        self.inner.borrow().get_suffix(from).await
    }
    async fn get_snapshot(&self) -> StorageResult<Option<ValueSnapshot>> {
        self.inner.borrow().get_snapshot().await
    }
    async fn get_log_len(&self) -> StorageResult<usize> {
        self.inner.borrow().get_log_len().await
    }
}

// ===========================================================================
// Message filter types
// ===========================================================================

type MessageFilter = Box<dyn Fn(&Message<Value>) -> bool>;

fn is_accept_decide(msg: &Message<Value>) -> bool {
    matches!(
        msg,
        Message::SequencePaxos(PaxosMessage {
            msg: PaxosMsg::AcceptDecide(_),
            ..
        })
    )
}

#[allow(dead_code)]
fn is_ble(msg: &Message<Value>) -> bool {
    matches!(msg, Message::BLE(_))
}

// ===========================================================================
// SimCluster — deterministic simulation harness
// ===========================================================================

type SimNode = OmniPaxos<Value, SharedMemoryStorage>;

struct SimCluster {
    nodes: BTreeMap<u64, SimNode>,
    storage_handles: BTreeMap<u64, Rc<RefCell<MemoryStorage<Value>>>>,
    crashed_storage: HashMap<u64, MemoryStorage<Value>>,
    cluster_config: ClusterConfig,
    server_configs: BTreeMap<u64, ServerConfig>,
    filters: Vec<Option<MessageFilter>>,
    dropped_messages: Vec<Message<Value>>,
    duplication_rng: Option<(SimpleRng, f64)>,
}

impl SimCluster {
    async fn new(node_ids: Vec<u64>) -> Self {
        let cluster_config = ClusterConfig {
            configuration_id: 1,
            nodes: node_ids.clone(),
            ..Default::default()
        };
        let mut server_configs = BTreeMap::new();
        for &pid in &node_ids {
            server_configs.insert(
                pid,
                ServerConfig {
                    pid,
                    // election_tick_timeout must be >= 2 so BLE heartbeat replies
                    // can be delivered and processed before the round increments.
                    // With timeout=1, replies are always stale (round mismatch).
                    election_tick_timeout: 5,
                    ..Default::default()
                },
            );
        }
        Self::with_configs(cluster_config, server_configs).await
    }

    async fn with_configs(
        cluster_config: ClusterConfig,
        server_configs: BTreeMap<u64, ServerConfig>,
    ) -> Self {
        let mut nodes = BTreeMap::new();
        let mut storage_handles = BTreeMap::new();

        for (&pid, server_config) in &server_configs {
            let (storage, handle) = SharedMemoryStorage::new_with_handle();
            let config = OmniPaxosConfig {
                cluster_config: cluster_config.clone(),
                server_config: server_config.clone(),
            };
            let op = config.build(storage).await.unwrap();
            nodes.insert(pid, op);
            storage_handles.insert(pid, handle);
        }

        SimCluster {
            nodes,
            storage_handles,
            crashed_storage: HashMap::new(),
            cluster_config,
            server_configs,
            filters: Vec::new(),
            dropped_messages: Vec::new(),
            duplication_rng: None,
        }
    }

    // -- Ticking and routing ------------------------------------------------

    async fn tick_all(&mut self) {
        for node in self.nodes.values_mut() {
            node.tick().await.unwrap();
        }
    }

    async fn route_messages(&mut self) {
        let mut all_msgs: Vec<Message<Value>> = Vec::new();
        let mut buf = Vec::new();
        for node in self.nodes.values_mut() {
            node.take_outgoing_messages(&mut buf);
            all_msgs.append(&mut buf);
        }
        // Duplicate messages if duplication_rng is configured
        if let Some((ref mut rng, rate)) = self.duplication_rng {
            let mut duplicates = Vec::new();
            for msg in &all_msgs {
                // Use rng to decide whether to duplicate
                let r = (rng.next() % 1000) as f64 / 1000.0;
                if r < rate {
                    duplicates.push(msg.clone());
                }
            }
            all_msgs.extend(duplicates);
        }
        for msg in all_msgs {
            let dominated = self
                .filters
                .iter()
                .any(|f| f.as_ref().map_or(false, |pred| !pred(&msg)));
            if dominated {
                self.dropped_messages.push(msg);
                continue;
            }
            let receiver = msg.get_receiver();
            if let Some(node) = self.nodes.get_mut(&receiver) {
                node.handle_incoming(msg).await.unwrap();
            }
        }
    }

    async fn tick_and_route(&mut self, rounds: usize) {
        for _ in 0..rounds {
            self.tick_all().await;
            self.route_messages().await;
        }
    }

    // -- Filter management --------------------------------------------------

    fn add_filter(&mut self, filter: MessageFilter) -> usize {
        let handle = self.filters.len();
        self.filters.push(Some(filter));
        handle
    }

    fn remove_filter(&mut self, handle: usize) {
        if handle < self.filters.len() {
            self.filters[handle] = None;
        }
    }

    fn clear_filters(&mut self) {
        self.filters.clear();
    }

    fn isolate_node(&mut self, pid: u64) -> usize {
        self.add_filter(Box::new(move |msg| {
            msg.get_sender() != pid && msg.get_receiver() != pid
        }))
    }

    fn partition(&mut self, group_a: Vec<u64>, group_b: Vec<u64>) -> usize {
        self.add_filter(Box::new(move |msg| {
            let s = msg.get_sender();
            let r = msg.get_receiver();
            let a_to_b = group_a.contains(&s) && group_b.contains(&r);
            let b_to_a = group_b.contains(&s) && group_a.contains(&r);
            !(a_to_b || b_to_a)
        }))
    }

    // -- Crash / Recovery ---------------------------------------------------

    fn crash_node(&mut self, pid: u64) {
        // Clone the current storage state (simulates persisted data surviving crash)
        let storage_state = self.storage_handles[&pid].borrow().clone();
        self.crashed_storage.insert(pid, storage_state);
        // Drop the OmniPaxos instance (transient state lost)
        self.nodes.remove(&pid);
        self.storage_handles.remove(&pid);
    }

    async fn recover_node(&mut self, pid: u64) {
        let saved_storage = self
            .crashed_storage
            .remove(&pid)
            .expect("no crashed storage for this pid");
        let (storage, handle) = SharedMemoryStorage::from_memory_storage(saved_storage);
        let config = OmniPaxosConfig {
            cluster_config: self.cluster_config.clone(),
            server_config: self.server_configs[&pid].clone(),
        };
        let op = config.build(storage).await.unwrap();
        self.nodes.insert(pid, op);
        self.storage_handles.insert(pid, handle);
        // Notify live peers about reconnection
        for (&other_pid, node) in self.nodes.iter_mut() {
            if other_pid != pid {
                node.reconnected(pid);
            }
        }
    }

    fn is_crashed(&self, pid: u64) -> bool {
        !self.nodes.contains_key(&pid)
    }

    fn live_nodes(&self) -> Vec<u64> {
        self.nodes.keys().copied().collect()
    }

    // -- Inspection ---------------------------------------------------------

    fn get_elected_leader(&self) -> Option<u64> {
        let mut leader_counts: HashMap<u64, usize> = HashMap::new();
        for node in self.nodes.values() {
            if let Some((leader_pid, _)) = node.get_current_leader() {
                *leader_counts.entry(leader_pid).or_insert(0) += 1;
            }
        }
        let majority = self.nodes.len() / 2 + 1;
        for (&leader, &count) in &leader_counts {
            // Only return leaders that are actually live nodes
            if count >= majority && self.nodes.contains_key(&leader) {
                return Some(leader);
            }
        }
        None
    }

    fn get_decided_indices(&self) -> BTreeMap<u64, usize> {
        self.nodes
            .iter()
            .map(|(&pid, node)| (pid, node.get_decided_idx()))
            .collect()
    }

    async fn logs_are_consistent(&self) -> bool {
        if self.nodes.is_empty() {
            return true;
        }
        // After crash recovery with snapshots, some nodes may have compacted
        // entries that others still have as Decided. We can only compare
        // entries above the highest compacted index across all nodes.
        let mut max_compacted = 0usize;
        for node in self.nodes.values() {
            max_compacted = max_compacted.max(node.get_compacted_idx());
        }
        let mut reference: Option<Vec<u64>> = None;
        for node in self.nodes.values() {
            let decided_idx = node.get_decided_idx();
            let ids: Vec<u64> = if decided_idx == 0 {
                vec![]
            } else {
                let entries = node
                    .read_decided_suffix(max_compacted)
                    .await
                    .unwrap()
                    .unwrap_or_default();
                entries
                    .iter()
                    .filter_map(|e| match e {
                        LogEntry::Decided(v) => Some(v.id),
                        _ => None,
                    })
                    .collect()
            };
            match &reference {
                None => reference = Some(ids),
                Some(ref_ids) => {
                    // The shorter log must be a prefix of the longer
                    let min_len = ref_ids.len().min(ids.len());
                    if ref_ids[..min_len] != ids[..min_len] {
                        return false;
                    }
                }
            }
        }
        true
    }

    // -- Proposing ----------------------------------------------------------

    async fn propose(&mut self, node_id: u64, entry: Value) {
        let node = self.nodes.get_mut(&node_id).expect("node not found");
        let _ = node.append(entry).await.unwrap();
    }

    async fn try_propose(&mut self, node_id: u64, entry: Value) -> Result<(), ProposeErr<Value>> {
        let node = self.nodes.get_mut(&node_id).expect("node not found");
        node.append(entry).await.map(|_| ())
    }

    #[allow(dead_code)]
    async fn assert_logs_consistent_and_converged(&self) {
        assert!(
            self.logs_are_consistent().await,
            "decided logs are not consistent across nodes"
        );
        let decided_indices: Vec<usize> = self
            .nodes
            .values()
            .map(|node| node.get_decided_idx())
            .collect();
        if let (Some(&min), Some(&max)) = (decided_indices.iter().min(), decided_indices.iter().max())
        {
            assert_eq!(
                min, max,
                "not all live nodes have converged: decided indices range from {} to {}",
                min, max
            );
        }
    }
}

// ===========================================================================
// Helpers
// ===========================================================================

async fn elect_leader(cluster: &mut SimCluster) -> u64 {
    for _ in 0..500 {
        cluster.tick_all().await;
        cluster.route_messages().await;
        if let Some(leader) = cluster.get_elected_leader() {
            return leader;
        }
    }
    panic!("Leader was not elected within 500 rounds");
}

#[allow(dead_code)]
async fn wait_for_decided(cluster: &mut SimCluster, target: usize, max_rounds: usize) {
    for _ in 0..max_rounds {
        cluster.tick_and_route(1).await;
        if cluster
            .get_decided_indices()
            .values()
            .all(|&idx| idx >= target)
        {
            return;
        }
    }
    panic!(
        "Not all nodes reached decided_idx {} within {} rounds. Current: {:?}",
        target,
        max_rounds,
        cluster.get_decided_indices()
    );
}

fn get_decided_values(node: &SimNode) -> Vec<u64> {
    smol::block_on(async {
        let entries = node
            .read_decided_suffix(0)
            .await
            .unwrap()
            .unwrap_or_default();
        entries
            .iter()
            .filter_map(|e| match e {
                LogEntry::Decided(v) => Some(v.id),
                _ => None,
            })
            .collect()
    })
}

/// Verify all live nodes have identical decided logs.
async fn assert_logs_consistent(cluster: &SimCluster) {
    // Collect per-node data for diagnostic
    let mut all_logs: Vec<(u64, Vec<u64>)> = Vec::new();
    for (&pid, node) in &cluster.nodes {
        let decided_idx = node.get_decided_idx();
        if decided_idx == 0 {
            all_logs.push((pid, vec![]));
            continue;
        }
        let entries = node
            .read_decided_suffix(0)
            .await
            .unwrap()
            .unwrap_or_default();
        let ids: Vec<u64> = entries
            .iter()
            .filter_map(|e| match e {
                LogEntry::Decided(v) => Some(v.id),
                _ => None,
            })
            .collect();
        all_logs.push((pid, ids));
    }
    let consistent = cluster.logs_are_consistent().await;
    if !consistent {
        // Print divergence details
        for (pid, ids) in &all_logs {
            let preview: Vec<u64> = ids.iter().take(20).copied().collect();
            eprintln!(
                "  node {}: decided_idx={}, log[..20]={:?}{}",
                pid,
                ids.len(),
                preview,
                if ids.len() > 20 { "..." } else { "" }
            );
        }
        panic!("decided logs are not consistent across nodes (see stderr for details)");
    }
}

/// Verify all live nodes have the same decided_idx.
fn assert_all_decided(cluster: &SimCluster, expected: usize) {
    for (&pid, node) in &cluster.nodes {
        assert_eq!(
            node.get_decided_idx(),
            expected,
            "node {} has decided_idx {}, expected {}",
            pid,
            node.get_decided_idx(),
            expected
        );
    }
}

// ===========================================================================
// Group 1: Crash Recovery
// ===========================================================================

#[test]
fn test_leader_crash_mid_replication() {
    smol::block_on(async {
        let mut cluster = SimCluster::new(vec![1, 2, 3]).await;
        let leader = elect_leader(&mut cluster).await;

        // Propose 5 entries through the leader.
        for i in 1..=5 {
            cluster.propose(leader, Value::with_id(i)).await;
        }
        // One round — AcceptDecide sent but maybe not all Accepted returned.
        cluster.tick_and_route(1).await;

        // Crash the leader.
        cluster.crash_node(leader);
        assert!(cluster.is_crashed(leader));

        // Elect a new leader from the survivors.
        let new_leader = elect_leader(&mut cluster).await;
        assert_ne!(
            new_leader, leader,
            "new leader should differ from crashed leader"
        );

        // Propose 3 new entries through the new leader.
        for i in 100..=102 {
            cluster.propose(new_leader, Value::with_id(i)).await;
        }
        cluster.tick_and_route(200).await;

        // New entries should be decided on live nodes.
        for node in cluster.nodes.values() {
            assert!(
                node.get_decided_idx() >= 3,
                "at least new entries should be decided"
            );
        }

        // Recover the old leader.
        cluster.recover_node(leader).await;
        cluster.tick_and_route(200).await;

        // All nodes should now be consistent.
        assert_logs_consistent(&cluster).await;

        // All live nodes should agree on decided_idx.
        let indices = cluster.get_decided_indices();
        let max_idx = *indices.values().max().unwrap();
        for (&pid, &idx) in &indices {
            assert_eq!(
                idx, max_idx,
                "node {} should have caught up to {}",
                pid, max_idx
            );
        }
    });
}

#[test]
fn test_follower_crash_and_rejoin() {
    smol::block_on(async {
        let mut cluster = SimCluster::new(vec![1, 2, 3]).await;
        let leader = elect_leader(&mut cluster).await;

        // Propose and decide 5 entries.
        for i in 1..=5 {
            cluster.propose(leader, Value::with_id(i)).await;
        }
        cluster.tick_and_route(100).await;
        assert_all_decided(&cluster, 5);

        // Crash a follower.
        let follower = *cluster.live_nodes().iter().find(|&&p| p != leader).unwrap();
        cluster.crash_node(follower);

        // Propose 10 more entries — majority of 2 still works.
        for i in 6..=15 {
            cluster.propose(leader, Value::with_id(i)).await;
        }
        cluster.tick_and_route(200).await;

        // 2 live nodes should have decided all 15 entries.
        for node in cluster.nodes.values() {
            assert_eq!(node.get_decided_idx(), 15);
        }

        // Recover the follower and let it catch up.
        cluster.recover_node(follower).await;
        cluster.tick_and_route(200).await;

        assert_all_decided(&cluster, 15);
        assert_logs_consistent(&cluster).await;
    });
}

#[test]
fn test_leader_crash_chain() {
    smol::block_on(async {
        let mut cluster = SimCluster::new(vec![1, 2, 3, 4, 5]).await;
        let l1 = elect_leader(&mut cluster).await;

        // Round 1: propose through L1.
        for i in 1..=3 {
            cluster.propose(l1, Value::with_id(i)).await;
        }
        cluster.tick_and_route(100).await;

        // Crash L1, elect L2.
        cluster.crash_node(l1);
        let l2 = elect_leader(&mut cluster).await;
        assert_ne!(l2, l1);

        for i in 4..=6 {
            cluster.propose(l2, Value::with_id(i)).await;
        }
        cluster.tick_and_route(100).await;

        // Crash L2, elect L3.
        cluster.crash_node(l2);
        let l3 = elect_leader(&mut cluster).await;
        assert_ne!(l3, l2);

        for i in 7..=9 {
            cluster.propose(l3, Value::with_id(i)).await;
        }
        cluster.tick_and_route(100).await;

        // Recover L1 and L2.
        cluster.recover_node(l1).await;
        cluster.recover_node(l2).await;
        cluster.tick_and_route(300).await;

        // All 5 nodes should have all 9 entries decided consistently.
        let max_decided = cluster
            .get_decided_indices()
            .values()
            .copied()
            .max()
            .unwrap();
        assert!(max_decided >= 9, "should have at least 9 decided entries");
        assert_all_decided(&cluster, max_decided);
        assert_logs_consistent(&cluster).await;
    });
}

// ===========================================================================
// Group 2: Network Partitions
// ===========================================================================

#[test]
fn test_minority_partition() {
    smol::block_on(async {
        let mut cluster = SimCluster::new(vec![1, 2, 3]).await;
        let leader = elect_leader(&mut cluster).await;

        // Decide initial entries.
        for i in 1..=5 {
            cluster.propose(leader, Value::with_id(i)).await;
        }
        cluster.tick_and_route(100).await;
        assert_all_decided(&cluster, 5);

        // Isolate a non-leader follower.
        let follower = *cluster.live_nodes().iter().find(|&&p| p != leader).unwrap();
        let filter_handle = cluster.isolate_node(follower);

        // Propose more entries — majority (leader + 1 other) still works.
        for i in 6..=15 {
            cluster.propose(leader, Value::with_id(i)).await;
        }
        cluster.tick_and_route(200).await;

        // Isolated follower should be stale.
        let follower_decided = cluster.nodes[&follower].get_decided_idx();
        assert_eq!(follower_decided, 5, "isolated follower should not advance");

        // Heal partition.
        cluster.remove_filter(filter_handle);
        // Notify reconnection.
        for &pid in &cluster.live_nodes() {
            if pid != follower {
                cluster.nodes.get_mut(&pid).unwrap().reconnected(follower);
            }
        }
        cluster
            .nodes
            .get_mut(&follower)
            .unwrap()
            .reconnected(leader);

        cluster.tick_and_route(300).await;

        assert_all_decided(&cluster, 15);
        assert_logs_consistent(&cluster).await;
    });
}

#[test]
fn test_leader_isolated() {
    smol::block_on(async {
        let mut cluster = SimCluster::new(vec![1, 2, 3]).await;
        let old_leader = elect_leader(&mut cluster).await;

        // Decide initial entries.
        for i in 1..=5 {
            cluster.propose(old_leader, Value::with_id(i)).await;
        }
        cluster.tick_and_route(100).await;
        assert_all_decided(&cluster, 5);

        // Isolate the leader.
        let filter_handle = cluster.isolate_node(old_leader);

        // Tick until a new leader is elected among the majority.
        // Use get_elected_leader which checks majority of live nodes.
        // The isolated node still reports old leader but the 2 non-isolated
        // nodes need to agree on a new leader that exists in self.nodes.
        let mut new_leader = None;
        for _ in 0..2000 {
            cluster.tick_all().await;
            cluster.route_messages().await;
            // Check if the two non-isolated nodes agree on a new leader.
            let mut counts: HashMap<u64, usize> = HashMap::new();
            for (&pid, node) in &cluster.nodes {
                if pid == old_leader {
                    continue; // skip isolated node's view
                }
                if let Some((lpid, _)) = node.get_current_leader() {
                    if lpid != old_leader {
                        *counts.entry(lpid).or_insert(0) += 1;
                    }
                }
            }
            for (&lpid, &count) in &counts {
                if count >= 2 {
                    new_leader = Some(lpid);
                }
            }
            if new_leader.is_some() {
                break;
            }
        }
        let new_leader = new_leader.expect("new leader should be elected in majority partition");
        assert_ne!(new_leader, old_leader);

        // Propose through the new leader.
        for i in 6..=10 {
            cluster.propose(new_leader, Value::with_id(i)).await;
        }
        cluster.tick_and_route(200).await;

        // Heal partition.
        cluster.remove_filter(filter_handle);
        for &pid in &cluster.live_nodes() {
            if pid != old_leader {
                cluster.nodes.get_mut(&pid).unwrap().reconnected(old_leader);
            }
        }
        cluster
            .nodes
            .get_mut(&old_leader)
            .unwrap()
            .reconnected(new_leader);

        cluster.tick_and_route(300).await;

        // All nodes should converge.
        assert_logs_consistent(&cluster).await;
        let indices = cluster.get_decided_indices();
        let max_idx = *indices.values().max().unwrap();
        assert!(max_idx >= 10, "should have at least 10 decided entries");
    });
}

#[test]
fn test_symmetric_partition_no_progress() {
    smol::block_on(async {
        let mut cluster = SimCluster::new(vec![1, 2, 3, 4]).await;
        let _leader = elect_leader(&mut cluster).await;

        // Decide 5 entries before partition.
        for i in 1..=5 {
            let live = cluster.live_nodes();
            let proposer = live[0];
            cluster.propose(proposer, Value::with_id(i)).await;
        }
        cluster.tick_and_route(100).await;

        let pre_partition_decided: BTreeMap<u64, usize> = cluster.get_decided_indices();

        // Partition [1,2] from [3,4] — neither has majority (need 3 of 4).
        let _filter_handle = cluster.partition(vec![1, 2], vec![3, 4]);

        // Tick a lot — no new entries should be decidable.
        cluster.tick_and_route(200).await;

        let post_partition_decided = cluster.get_decided_indices();
        for (&pid, &idx) in &post_partition_decided {
            assert_eq!(
                idx, pre_partition_decided[&pid],
                "node {} should not have advanced decided_idx during symmetric partition",
                pid
            );
        }

        // Heal and converge.
        cluster.clear_filters();
        for &pid in &[1u64, 2, 3, 4] {
            for &other in &[1u64, 2, 3, 4] {
                if pid != other {
                    if let Some(node) = cluster.nodes.get_mut(&pid) {
                        node.reconnected(other);
                    }
                }
            }
        }
        cluster.tick_and_route(300).await;

        // Should be able to elect a leader and make progress now.
        let leader = elect_leader(&mut cluster).await;
        cluster.propose(leader, Value::with_id(100)).await;
        cluster.tick_and_route(100).await;

        assert_logs_consistent(&cluster).await;
    });
}

#[test]
fn test_partition_heal_replication_continues() {
    smol::block_on(async {
        let mut cluster = SimCluster::new(vec![1, 2, 3, 4, 5]).await;
        let leader = elect_leader(&mut cluster).await;

        for i in 1..=10 {
            cluster.propose(leader, Value::with_id(i)).await;
        }
        cluster.tick_and_route(100).await;

        // Isolate 2 followers — quorum of 3 still available.
        let followers: Vec<u64> = cluster
            .live_nodes()
            .into_iter()
            .filter(|&p| p != leader)
            .collect();
        let isolated = vec![followers[0], followers[1]];
        let h1 = cluster.isolate_node(isolated[0]);
        let h2 = cluster.isolate_node(isolated[1]);

        // Propose more — should still decide with 3/5 quorum.
        for i in 11..=20 {
            cluster.propose(leader, Value::with_id(i)).await;
        }
        cluster.tick_and_route(200).await;

        // Leader + 2 remaining followers should have decided.
        let leader_decided = cluster.nodes[&leader].get_decided_idx();
        assert_eq!(
            leader_decided, 20,
            "leader should have decided all 20 entries"
        );

        // Heal and let isolated nodes catch up.
        cluster.remove_filter(h1);
        cluster.remove_filter(h2);
        for &iso in &isolated {
            for (&pid, node) in cluster.nodes.iter_mut() {
                if pid != iso {
                    node.reconnected(iso);
                }
            }
            cluster.nodes.get_mut(&iso).unwrap().reconnected(leader);
        }
        cluster.tick_and_route(300).await;

        assert_all_decided(&cluster, 20);
        assert_logs_consistent(&cluster).await;
    });
}

// ===========================================================================
// Group 3: Concurrent Proposals
// ===========================================================================

#[test]
fn test_concurrent_proposals_all_nodes() {
    smol::block_on(async {
        let mut cluster = SimCluster::new(vec![1, 2, 3, 4, 5]).await;
        let _leader = elect_leader(&mut cluster).await;
        // Extra rounds to ensure leader is in Accept phase.
        cluster.tick_and_route(20).await;

        // Each node proposes 10 entries with unique IDs.
        let nodes: Vec<u64> = cluster.live_nodes();
        for &pid in &nodes {
            for j in 0..10 {
                let id = pid * 100 + j;
                cluster.propose(pid, Value::with_id(id)).await;
            }
        }

        cluster.tick_and_route(300).await;

        // All 50 entries should be decided.
        let decided_idx = cluster.nodes.values().next().unwrap().get_decided_idx();
        assert_eq!(decided_idx, 50, "all 50 entries should be decided");

        // Verify consistency and no duplicates.
        assert_logs_consistent(&cluster).await;
        let ids = get_decided_values(cluster.nodes.values().next().unwrap());
        let id_set: HashSet<u64> = ids.iter().copied().collect();
        assert_eq!(id_set.len(), 50, "no duplicate entries");
    });
}

#[test]
fn test_proposals_during_leader_change() {
    smol::block_on(async {
        let mut cluster = SimCluster::new(vec![1, 2, 3]).await;
        let old_leader = elect_leader(&mut cluster).await;

        // Propose 5 entries through old leader.
        for i in 1..=5 {
            cluster.propose(old_leader, Value::with_id(i)).await;
        }
        cluster.tick_and_route(100).await;
        assert_all_decided(&cluster, 5);

        // Isolate old leader to force new election.
        let filter_handle = cluster.isolate_node(old_leader);
        cluster.tick_and_route(200).await;

        // Find the new leader among non-isolated nodes.
        let survivors: Vec<u64> = cluster
            .live_nodes()
            .into_iter()
            .filter(|&p| p != old_leader)
            .collect();
        let mut new_leader = None;
        for &pid in &survivors {
            if let Some((lpid, _)) = cluster.nodes[&pid].get_current_leader() {
                if lpid != old_leader && survivors.contains(&lpid) {
                    new_leader = Some(lpid);
                    break;
                }
            }
        }

        let nl = new_leader.expect("new leader must be elected among survivors");

        // Propose through new leader.
        for i in 6..=10 {
            cluster.propose(nl, Value::with_id(i)).await;
        }
        cluster.tick_and_route(200).await;

        // Heal and converge.
        cluster.remove_filter(filter_handle);
        cluster
            .nodes
            .get_mut(&old_leader)
            .unwrap()
            .reconnected(nl);
        cluster.tick_and_route(300).await;

        assert_logs_consistent(&cluster).await;
    });
}

// ===========================================================================
// Group 4: Reconfiguration
// ===========================================================================

#[test]
fn test_reconfigure_add_nodes() {
    smol::block_on(async {
        let mut cluster = SimCluster::new(vec![1, 2, 3]).await;
        let leader = elect_leader(&mut cluster).await;

        // Propose and decide entries.
        for i in 1..=5 {
            cluster.propose(leader, Value::with_id(i)).await;
        }
        cluster.tick_and_route(100).await;
        assert_all_decided(&cluster, 5);

        // Reconfigure to add nodes 4 and 5.
        let new_config = ClusterConfig {
            configuration_id: 2,
            nodes: vec![1, 2, 3, 4, 5],
            ..Default::default()
        };
        let leader_node = cluster.nodes.get_mut(&leader).unwrap();
        leader_node.reconfigure(new_config, None).await.unwrap();

        cluster.tick_and_route(100).await;

        // StopSign should be decided.
        let leader_node = cluster.nodes.get(&leader).unwrap();
        let ss = leader_node.is_reconfigured();
        assert!(ss.is_some(), "reconfiguration should be decided");
        let ss = ss.unwrap();
        assert_eq!(ss.next_config.nodes, vec![1, 2, 3, 4, 5]);

        // Further appends should fail.
        let result: Result<(), ProposeErr<Value>> =
            cluster.try_propose(leader, Value::with_id(99)).await;
        assert!(result.is_err(), "append after reconfiguration should fail");
    });
}

#[test]
fn test_reconfigure_remove_nodes() {
    smol::block_on(async {
        let mut cluster = SimCluster::new(vec![1, 2, 3, 4, 5]).await;
        let leader = elect_leader(&mut cluster).await;

        for i in 1..=10 {
            cluster.propose(leader, Value::with_id(i)).await;
        }
        cluster.tick_and_route(100).await;

        // Reconfigure to remove nodes 4 and 5.
        let new_config = ClusterConfig {
            configuration_id: 2,
            nodes: vec![1, 2, 3],
            ..Default::default()
        };
        let leader_node = cluster.nodes.get_mut(&leader).unwrap();
        leader_node.reconfigure(new_config, None).await.unwrap();

        cluster.tick_and_route(100).await;

        let leader_node = cluster.nodes.get(&leader).unwrap();
        let ss = leader_node.is_reconfigured();
        assert!(ss.is_some(), "reconfiguration should be decided");
        assert_eq!(ss.unwrap().next_config.nodes, vec![1, 2, 3]);

        // All 10 entries should be decided before the StopSign.
        assert!(leader_node.get_decided_idx() >= 10);
    });
}

#[test]
fn test_double_reconfiguration_rejected() {
    smol::block_on(async {
        let mut cluster = SimCluster::new(vec![1, 2, 3]).await;
        let leader = elect_leader(&mut cluster).await;
        // Ensure leader is in Accept phase.
        cluster.tick_and_route(20).await;

        let config_a = ClusterConfig {
            configuration_id: 2,
            nodes: vec![1, 2, 3, 4],
            ..Default::default()
        };
        let config_b = ClusterConfig {
            configuration_id: 3,
            nodes: vec![1, 2, 3, 4, 5],
            ..Default::default()
        };

        // First reconfigure should succeed.
        let leader_node = cluster.nodes.get_mut(&leader).unwrap();
        leader_node.reconfigure(config_a, None).await.unwrap();

        // Second reconfigure should be rejected.
        let result = leader_node.reconfigure(config_b, None).await;
        assert!(
            result.is_err(),
            "second reconfiguration should be rejected while first is pending"
        );
    });
}

// ===========================================================================
// Group 5: Flexible Quorum
// ===========================================================================

#[test]
fn test_flexible_quorum_write_with_minority() {
    smol::block_on(async {
        let cluster_config = ClusterConfig {
            configuration_id: 1,
            nodes: vec![1, 2, 3, 4, 5],
            flexible_quorum: Some(FlexibleQuorum {
                read_quorum_size: 4,
                write_quorum_size: 2,
            }),
        };
        let mut server_configs = BTreeMap::new();
        for pid in 1..=5 {
            server_configs.insert(
                pid,
                ServerConfig {
                    pid,
                    ..Default::default()
                },
            );
        }
        let mut cluster = SimCluster::with_configs(cluster_config, server_configs).await;
        let leader = elect_leader(&mut cluster).await;

        // Propose 10 entries with all 5 nodes alive.
        for i in 1..=10 {
            cluster.propose(leader, Value::with_id(i)).await;
        }
        cluster.tick_and_route(100).await;
        assert_all_decided(&cluster, 10);

        // Crash 3 non-leader followers — only leader + 1 follower remain.
        let followers: Vec<u64> = cluster
            .live_nodes()
            .into_iter()
            .filter(|&p| p != leader)
            .collect();
        for &f in &followers[..3] {
            cluster.crash_node(f);
        }
        assert_eq!(cluster.live_nodes().len(), 2);

        // Propose 5 more — write_quorum=2 means leader+1 follower is enough.
        for i in 11..=15 {
            cluster.propose(leader, Value::with_id(i)).await;
        }
        cluster.tick_and_route(200).await;

        // Both live nodes should have decided all 15 entries.
        for node in cluster.nodes.values() {
            assert_eq!(
                node.get_decided_idx(),
                15,
                "entries should be decided with write_quorum=2"
            );
        }
    });
}

// ===========================================================================
// Group 6: Stress Test
// ===========================================================================

#[test]
fn test_stress_crashes_and_recoveries() {
    smol::block_on(async {
        let mut cluster = SimCluster::new(vec![1, 2, 3, 4, 5, 6, 7]).await;
        let mut current_leader = elect_leader(&mut cluster).await;
        let mut entry_id = 1u64;

        for iteration in 0..100 {
            // Propose 10 entries through a live node.
            let live = cluster.live_nodes();
            let proposer = live[iteration % live.len()];
            for _ in 0..10 {
                // If proposer is the leader or a follower, append should work.
                let _ = cluster
                    .try_propose(proposer, Value::with_id(entry_id))
                    .await;
                entry_id += 1;
            }

            // Every 20th iteration: crash a non-leader node (max 2 crashed).
            if iteration % 20 == 0 && iteration > 0 {
                let crashed_count = (1..=7).filter(|pid| cluster.is_crashed(*pid)).count();
                if crashed_count < 2 {
                    let live = cluster.live_nodes();
                    if let Some(&victim) = live.iter().find(|&&p| p != current_leader) {
                        cluster.crash_node(victim);
                    }
                }
            }

            // Every 25th iteration: recover a crashed node.
            if iteration % 25 == 0 && iteration > 0 {
                let crashed: Vec<u64> = (1..=7).filter(|pid| cluster.is_crashed(*pid)).collect();
                if let Some(&pid) = crashed.first() {
                    cluster.recover_node(pid).await;
                }
            }

            cluster.tick_and_route(10).await;

            // Re-elect leader if needed.
            if cluster.is_crashed(current_leader) || cluster.get_elected_leader().is_none() {
                for _ in 0..100 {
                    cluster.tick_and_route(1).await;
                    if let Some(l) = cluster.get_elected_leader() {
                        current_leader = l;
                        break;
                    }
                }
            }
        }

        // Recover all crashed nodes.
        for pid in 1..=7 {
            if cluster.is_crashed(pid) {
                cluster.recover_node(pid).await;
            }
        }
        cluster.tick_and_route(500).await;

        // All 7 nodes should be consistent.
        assert_logs_consistent(&cluster).await;

        // All nodes should have the same decided_idx.
        let indices = cluster.get_decided_indices();
        let max_idx = *indices.values().max().unwrap();
        assert!(max_idx > 0, "some entries should have been decided");
        for (&pid, &idx) in &indices {
            assert_eq!(idx, max_idx, "node {} should have caught up", pid);
        }
    });
}

// ===========================================================================
// Group 7: Leader Election Edge Cases
// ===========================================================================

#[test]
fn test_leader_priority() {
    smol::block_on(async {
        let cluster_config = ClusterConfig {
            configuration_id: 1,
            nodes: vec![1, 2, 3],
            ..Default::default()
        };
        let mut server_configs = BTreeMap::new();
        server_configs.insert(
            1,
            ServerConfig {
                pid: 1,
                leader_priority: 0,
                ..Default::default()
            },
        );
        server_configs.insert(
            2,
            ServerConfig {
                pid: 2,
                leader_priority: 10,
                ..Default::default()
            },
        );
        server_configs.insert(
            3,
            ServerConfig {
                pid: 3,
                leader_priority: 0,
                ..Default::default()
            },
        );

        let mut cluster = SimCluster::with_configs(cluster_config, server_configs).await;
        let leader = elect_leader(&mut cluster).await;

        assert_eq!(
            leader, 2,
            "node 2 with highest priority should become leader"
        );
    });
}

#[test]
fn test_competing_try_become_leader() {
    smol::block_on(async {
        let mut cluster = SimCluster::new(vec![1, 2, 3]).await;
        let initial_leader = elect_leader(&mut cluster).await;

        // Both non-leaders try to become leader.
        let non_leaders: Vec<u64> = cluster
            .live_nodes()
            .into_iter()
            .filter(|&p| p != initial_leader)
            .collect();
        for &pid in &non_leaders {
            cluster
                .nodes
                .get_mut(&pid)
                .unwrap()
                .try_become_leader()
                .await
                .unwrap();
        }

        // Tick until exactly one leader emerges.
        let leader = elect_leader(&mut cluster).await;
        // All nodes should agree.
        for node in cluster.nodes.values() {
            let (lpid, _) = node.get_current_leader().unwrap();
            assert_eq!(lpid, leader, "all nodes must agree on leader");
        }
    });
}

// ===========================================================================
// Group 8: Snapshot / Compaction with Recovery
// ===========================================================================

#[test]
fn test_snapshot_catchup_after_compaction() {
    smol::block_on(async {
        let mut cluster = SimCluster::new(vec![1, 2, 3]).await;
        let leader = elect_leader(&mut cluster).await;

        // Crash a follower.
        let follower = *cluster.live_nodes().iter().find(|&&p| p != leader).unwrap();
        cluster.crash_node(follower);

        // Propose 20 entries, decide on 2 live nodes.
        for i in 1..=20 {
            cluster.propose(leader, Value::with_id(i)).await;
        }
        cluster.tick_and_route(200).await;

        // Snapshot the first 15 entries on the leader.
        cluster
            .nodes
            .get_mut(&leader)
            .unwrap()
            .snapshot(Some(15), false)
            .await
            .unwrap();
        cluster.tick_and_route(100).await;

        // Recover the follower and let it catch up.
        cluster.recover_node(follower).await;
        cluster.tick_and_route(300).await;

        // Follower should have caught up.
        let follower_decided = cluster.nodes[&follower].get_decided_idx();
        assert_eq!(follower_decided, 20, "follower should catch up to 20");

        assert_logs_consistent(&cluster).await;
    });
}

#[test]
fn test_trim_then_catchup() {
    smol::block_on(async {
        let mut cluster = SimCluster::new(vec![1, 2, 3]).await;
        let leader = elect_leader(&mut cluster).await;

        // Crash a follower.
        let follower = *cluster.live_nodes().iter().find(|&&p| p != leader).unwrap();
        cluster.crash_node(follower);

        // Propose 20 entries, decide on live nodes.
        for i in 1..=20 {
            cluster.propose(leader, Value::with_id(i)).await;
        }
        cluster.tick_and_route(200).await;

        // Trim while follower is crashed should fail: trim is conservative and
        // requires ALL nodes (including disconnected ones) to have accepted entries.
        let trim_result = cluster
            .nodes
            .get_mut(&leader)
            .unwrap()
            .trim(Some(15))
            .await;
        assert!(
            matches!(trim_result, Err(CompactionErr::NotAllDecided(0))),
            "trim must fail while a disconnected node hasn't accepted the entries"
        );

        // Recover the follower and let it catch up.
        cluster.recover_node(follower).await;
        cluster.tick_and_route(300).await;

        let follower_decided = cluster.nodes[&follower].get_decided_idx();
        assert_eq!(follower_decided, 20, "follower should catch up to 20");

        // Now trim should succeed since all nodes have accepted.
        cluster
            .nodes
            .get_mut(&leader)
            .unwrap()
            .trim(Some(15))
            .await
            .unwrap();
        cluster.tick_and_route(100).await;

        // Reading a trimmed index on the leader should return Trimmed.
        let entry = cluster.nodes[&leader].read(1).await.unwrap();
        assert!(
            matches!(entry, Some(LogEntry::Trimmed(_))),
            "trimmed entry should be Trimmed"
        );
    });
}

// ===========================================================================
// Group 9: Error Paths
// ===========================================================================

#[test]
fn test_compaction_error_variants() {
    smol::block_on(async {
        let mut cluster = SimCluster::new(vec![1, 2, 3]).await;
        let leader = elect_leader(&mut cluster).await;

        for i in 1..=10 {
            cluster.propose(leader, Value::with_id(i)).await;
        }
        cluster.tick_and_route(100).await;
        assert_all_decided(&cluster, 10);

        // Trim beyond decided index should fail.
        let result = cluster.nodes.get_mut(&leader).unwrap().trim(Some(20)).await;
        assert!(result.is_err(), "trim beyond decided should fail");

        // Trim on a follower should fail (not leader).
        let follower = *cluster.live_nodes().iter().find(|&&p| p != leader).unwrap();
        let result = cluster
            .nodes
            .get_mut(&follower)
            .unwrap()
            .trim(Some(5))
            .await;
        assert!(result.is_err(), "trim on follower should fail");

        // Snapshot beyond decided index should fail.
        let result = cluster
            .nodes
            .get_mut(&leader)
            .unwrap()
            .snapshot(Some(20), true)
            .await;
        assert!(result.is_err(), "snapshot beyond decided should fail");
    });
}

#[test]
fn test_batch_flush_timeout() {
    smol::block_on(async {
        let cluster_config = ClusterConfig {
            configuration_id: 1,
            nodes: vec![1, 2, 3],
            ..Default::default()
        };
        let mut server_configs = BTreeMap::new();
        for pid in 1..=3 {
            server_configs.insert(
                pid,
                ServerConfig {
                    pid,
                    batch_size: 100,
                    flush_batch_tick_timeout: 3,
                    ..Default::default()
                },
            );
        }
        let mut cluster = SimCluster::with_configs(cluster_config, server_configs).await;
        let leader = elect_leader(&mut cluster).await;
        // Extra rounds to get leader into Accept phase.
        cluster.tick_and_route(20).await;

        // Propose 5 entries (well below batch_size=100).
        for i in 1..=5 {
            cluster.propose(leader, Value::with_id(i)).await;
        }

        // After 1 tick, entries should NOT be decided yet.
        cluster.tick_and_route(1).await;
        assert_eq!(
            cluster.nodes[&leader].get_decided_idx(),
            0,
            "entries should not be decided before flush timeout"
        );

        // After enough ticks for flush timeout + replication.
        cluster.tick_and_route(100).await;

        assert_all_decided(&cluster, 5);
    });
}

// ===========================================================================
// Group 10: Message Drops
// ===========================================================================

#[test]
fn test_acceptdecide_drops_resend_recovery() {
    smol::block_on(async {
        let cluster_config = ClusterConfig {
            configuration_id: 1,
            nodes: vec![1, 2, 3],
            ..Default::default()
        };
        let mut server_configs = BTreeMap::new();
        for pid in 1..=3 {
            server_configs.insert(
                pid,
                ServerConfig {
                    pid,
                    resend_message_tick_timeout: 5,
                    ..Default::default()
                },
            );
        }
        let mut cluster = SimCluster::with_configs(cluster_config, server_configs).await;
        let leader = elect_leader(&mut cluster).await;
        cluster.tick_and_route(20).await;

        // Block AcceptDecide messages.
        let filter_handle = cluster.add_filter(Box::new(|msg| !is_accept_decide(msg)));

        // Propose entries — they'll be accepted by leader but AcceptDecide won't reach followers.
        for i in 1..=10 {
            cluster.propose(leader, Value::with_id(i)).await;
        }
        cluster.tick_and_route(50).await;

        // Remove filter and simulate reconnection — followers re-sync with leader.
        cluster.remove_filter(filter_handle);
        for (&pid, node) in cluster.nodes.iter_mut() {
            if pid != leader {
                node.reconnected(leader);
            }
        }
        cluster.tick_and_route(500).await;

        // All entries should eventually be decided.
        let decided = cluster.nodes[&leader].get_decided_idx();
        assert!(
            decided >= 10,
            "entries should be decided after filter removed, got {}",
            decided
        );
        assert_logs_consistent(&cluster).await;
    });
}

#[test]
fn test_ble_blackout_no_leader() {
    smol::block_on(async {
        let mut cluster = SimCluster::new(vec![1, 2, 3]).await;

        // Block ALL messages — prevents both BLE heartbeats and Paxos Prepare/Promise.
        let filter_handle = cluster.add_filter(Box::new(|_msg| false));

        // Tick 100 rounds — no leader should emerge without any communication.
        cluster.tick_and_route(100).await;
        assert!(
            cluster.get_elected_leader().is_none(),
            "no leader should be elected during total communication blackout"
        );

        // Remove filter — leader should be elected.
        cluster.remove_filter(filter_handle);
        let leader = elect_leader(&mut cluster).await;
        assert!(
            leader > 0,
            "leader should emerge after communication restored"
        );
    });
}

// ===========================================================================
// FIXED: BLE heartbeats now work with election_tick_timeout=1 (the default)
//
// Previously, handle_reply() only accepted replies from the current round,
// but with election_tick_timeout=1 replies always arrive one round late.
// Fixed by accepting previous-round replies, with safeguards against stale
// happy flags: consecutive_unhappy_rounds >= 2, leader reachability check
// (not just happiness), and randomized backoff in check_takeover.
// ===========================================================================

/// Verifies that BLE-based crash detection works with the default
/// election_tick_timeout=1. After crashing the leader, survivors detect
/// the failure via missing heartbeat replies and elect a new leader.
#[test]
fn test_default_timeout_leader_crash_reelection() {
    smol::block_on(async {
        // Use DEFAULT ServerConfig (election_tick_timeout=1)
        let node_ids = vec![1, 2, 3];
        let cluster_config = ClusterConfig {
            configuration_id: 1,
            nodes: node_ids.clone(),
            ..Default::default()
        };
        let mut server_configs = BTreeMap::new();
        for &pid in &node_ids {
            server_configs.insert(
                pid,
                ServerConfig {
                    pid,
                    ..Default::default()
                },
            );
        }
        let mut cluster = SimCluster::with_configs(cluster_config, server_configs).await;

        // Initial election works (via seq_paxos_promise sync, not BLE heartbeats)
        let leader = elect_leader(&mut cluster).await;

        // Propose and decide some entries
        for i in 1..=5 {
            cluster.propose(leader, Value::with_id(i)).await;
        }
        cluster.tick_and_route(100).await;

        // Crash the leader
        cluster.crash_node(leader);

        // BLE detects the crash and elects a new leader
        let new_leader = elect_leader(&mut cluster).await;
        assert_ne!(
            new_leader, leader,
            "should elect a different leader after crash"
        );
    });
}

// ===========================================================================
// FIXED: Leader batch flush now uses write_atomically
//
// Previously, flush_batch_leader() used separate append_entries() and
// set_decided_idx() calls. Now uses flush_batch_and_decide_with_entries()
// which wraps both in write_atomically() for crash safety.
// ===========================================================================

/// Verifies that the single-node leader batch flush path writes entries
/// and decided_idx atomically. After the fix, this test passes because
/// flush_batch_leader() uses flush_batch_and_decide_with_entries() which
/// calls write_atomically() for the combined operation.
///
/// The test verifies that after the batch flush, both entries and decided_idx
/// are updated in the same storage state (no window where entries exist but
/// decided_idx lags behind).
#[test]
fn test_single_node_flush_decide_atomicity() {
    smol::block_on(async {
        let cluster_config = ClusterConfig {
            configuration_id: 1,
            nodes: vec![1],
            ..Default::default()
        };
        let server_config = ServerConfig {
            pid: 1,
            batch_size: 100,
            flush_batch_tick_timeout: 3,
            ..Default::default()
        };
        let mut server_configs = BTreeMap::new();
        server_configs.insert(1u64, server_config);
        let mut cluster = SimCluster::with_configs(cluster_config, server_configs).await;
        let leader = elect_leader(&mut cluster).await;
        assert_eq!(leader, 1);

        // Record storage state before proposing
        let pre_log_len = cluster.storage_handles[&1]
            .borrow()
            .get_log_len()
            .await
            .unwrap();

        // Propose entries (buffered in batch, not flushed yet)
        for i in 1..=5 {
            cluster.propose(1, Value::with_id(i)).await;
        }

        // Tick enough for the batch flush timeout to fire
        // On each tick, verify entries and decided_idx move together
        let mut saw_entries_without_decided = false;
        for _ in 0..10 {
            cluster.tick_all().await;
            cluster.route_messages().await;

            let storage = cluster.storage_handles[&1].borrow();
            let log_len = storage.get_log_len().await.unwrap();
            let state = storage.load_state().await.unwrap();
            let decided_idx = state.decided_idx;

            // If entries were written but decided_idx hasn't caught up,
            // the flush is non-atomic
            if log_len > pre_log_len && decided_idx < pre_log_len + (log_len - pre_log_len) {
                saw_entries_without_decided = true;
            }
        }

        // Verify final state is correct
        let storage = cluster.storage_handles[&1].borrow();
        assert_eq!(storage.get_log_len().await.unwrap(), 5);
        assert_eq!(storage.load_state().await.unwrap().decided_idx, 5);

        // With the atomic fix, entries and decided_idx are written together
        assert!(
            !saw_entries_without_decided,
            "entries and decided_idx should be written atomically"
        );
    });
}

// ===========================================================================
// Coverage gap: Majority loss and recovery
// ===========================================================================

/// Crash 2 of 3 nodes (majority lost), verify system is frozen,
/// then recover one node and verify progress resumes.
#[test]
fn test_majority_loss_and_recovery() {
    smol::block_on(async {
        let mut cluster = SimCluster::new(vec![1, 2, 3]).await;
        let leader = elect_leader(&mut cluster).await;

        // Propose and decide entries
        for i in 1..=5 {
            cluster.propose(leader, Value::with_id(i)).await;
        }
        cluster.tick_and_route(100).await;
        assert_all_decided(&cluster, 5);

        // Crash 2 nodes — only 1 remains (no majority)
        let followers: Vec<u64> = cluster
            .live_nodes()
            .into_iter()
            .filter(|&p| p != leader)
            .collect();
        cluster.crash_node(followers[0]);
        cluster.crash_node(followers[1]);

        // Lone survivor cannot make progress
        let pre_decided = cluster.nodes[&leader].get_decided_idx();
        let _ = cluster.try_propose(leader, Value::with_id(100)).await;
        cluster.tick_and_route(200).await;
        // decided_idx should NOT advance (no quorum)
        assert_eq!(
            cluster.nodes[&leader].get_decided_idx(),
            pre_decided,
            "lone node should not decide without quorum"
        );

        // Recover one node — quorum restored (2 of 3)
        cluster.recover_node(followers[0]).await;
        cluster.tick_and_route(500).await;

        // Now entries should be decided and logs consistent
        let leader_decided = cluster.nodes[&leader].get_decided_idx();
        assert!(
            leader_decided > pre_decided,
            "should make progress after quorum restored"
        );
        assert_logs_consistent(&cluster).await;
    });
}

// ===========================================================================
// Coverage gap: Stress test that also crashes leaders
// ===========================================================================

/// Like test_stress_crashes_and_recoveries but intentionally crashes
/// leaders too, not just followers.
#[test]
fn test_stress_with_leader_crashes() {
    smol::block_on(async {
        let mut cluster = SimCluster::new(vec![1, 2, 3, 4, 5]).await;
        let mut current_leader = elect_leader(&mut cluster).await;
        let mut entry_id = 1u64;

        for iteration in 0..50 {
            // Propose through a live node
            let live = cluster.live_nodes();
            if live.is_empty() {
                continue;
            }
            let proposer = live[iteration % live.len()];
            for _ in 0..5 {
                let _ = cluster
                    .try_propose(proposer, Value::with_id(entry_id))
                    .await;
                entry_id += 1;
            }

            // Every 10th iteration: crash a random live node INCLUDING the leader
            if iteration % 10 == 0 && iteration > 0 {
                let crashed_count = (1..=5).filter(|pid| cluster.is_crashed(*pid)).count();
                // Keep at least 3 alive (majority of 5)
                if crashed_count < 2 {
                    let live = cluster.live_nodes();
                    // Intentionally include leader as crash candidate
                    let victim = live[iteration % live.len()];
                    cluster.crash_node(victim);
                    if victim == current_leader {
                        current_leader = 0; // force re-election
                    }
                }
            }

            // Every 15th iteration: recover a crashed node
            if iteration % 15 == 0 && iteration > 0 {
                let crashed: Vec<u64> = (1..=5).filter(|pid| cluster.is_crashed(*pid)).collect();
                if let Some(&pid) = crashed.first() {
                    cluster.recover_node(pid).await;
                }
            }

            cluster.tick_and_route(10).await;

            // Re-elect if needed
            if current_leader == 0
                || cluster.is_crashed(current_leader)
                || cluster.get_elected_leader().is_none()
            {
                for _ in 0..200 {
                    cluster.tick_and_route(1).await;
                    if let Some(l) = cluster.get_elected_leader() {
                        current_leader = l;
                        break;
                    }
                }
            }
        }

        // Recover all, converge
        for pid in 1..=5 {
            if cluster.is_crashed(pid) {
                cluster.recover_node(pid).await;
            }
        }
        cluster.tick_and_route(500).await;

        // All logs must be consistent
        assert_logs_consistent(&cluster).await;

        let indices = cluster.get_decided_indices();
        let max_idx = *indices.values().max().unwrap();
        assert!(max_idx > 0, "some entries should have been decided");
        for (&pid, &idx) in &indices {
            assert_eq!(idx, max_idx, "node {} should have caught up", pid);
        }
    });
}

// ===========================================================================
// Coverage gap: Decided entries never lost or reordered
// ===========================================================================

/// Verifies that decided entries from the same proposer are never
/// reordered relative to each other, even when multiple nodes propose
/// concurrently with non-monotonic IDs across proposers.
#[test]
fn test_decided_entries_never_reorder() {
    smol::block_on(async {
        let mut cluster = SimCluster::new(vec![1, 2, 3]).await;
        let _leader = elect_leader(&mut cluster).await;
        // Extra rounds to ensure leader is in Accept phase.
        cluster.tick_and_route(20).await;

        let live = cluster.live_nodes();
        assert!(live.len() >= 3, "need at least 3 live nodes");

        // Each node proposes entries with unique IDs that are NOT globally
        // monotonic. The IDs within each proposer ARE in order.
        // Node live[0] proposes 100, 200, 300
        // Node live[1] proposes 150, 250, 350
        // Node live[2] proposes 175, 275, 375
        let proposals: Vec<(u64, Vec<u64>)> = vec![
            (live[0], vec![100, 200, 300]),
            (live[1], vec![150, 250, 350]),
            (live[2], vec![175, 275, 375]),
        ];

        for (node_id, ids) in &proposals {
            for &id in ids {
                cluster.propose(*node_id, Value::with_id(id)).await;
            }
        }

        cluster.tick_and_route(300).await;

        // All 9 entries should be decided.
        assert_all_decided(&cluster, 9);
        assert_logs_consistent(&cluster).await;

        // Verify: entries from the SAME proposer appear in the same
        // relative order in the decided log across ALL nodes.
        for (&pid, node) in &cluster.nodes {
            let values = get_decided_values(node);
            for (proposer, expected_order) in &proposals {
                // Extract the subsequence of entries from this proposer
                let actual_order: Vec<u64> = values
                    .iter()
                    .copied()
                    .filter(|id| expected_order.contains(id))
                    .collect();
                assert_eq!(
                    actual_order, *expected_order,
                    "node {}: entries from proposer {} are reordered: got {:?}, expected {:?}",
                    pid, proposer, actual_order, expected_order
                );
            }
        }
    });
}

// ===========================================================================
// Coverage gap: Crash during recovery
// ===========================================================================

/// Crash a node, start recovery, crash it AGAIN mid-recovery, then
/// recover a second time. The node must reach a consistent state.
#[test]
fn test_crash_during_recovery() {
    smol::block_on(async {
        let mut cluster = SimCluster::new(vec![1, 2, 3]).await;
        let leader = elect_leader(&mut cluster).await;

        // Propose and decide entries
        for i in 1..=10 {
            cluster.propose(leader, Value::with_id(i)).await;
        }
        cluster.tick_and_route(200).await;
        assert_all_decided(&cluster, 10);

        // Pick a follower
        let follower = cluster
            .live_nodes()
            .into_iter()
            .find(|&p| p != leader)
            .unwrap();

        // First crash
        cluster.crash_node(follower);

        // Propose more entries while follower is down
        for i in 11..=20 {
            cluster.propose(leader, Value::with_id(i)).await;
        }
        cluster.tick_and_route(100).await;

        // Start recovery — only tick a few rounds (mid-recovery)
        cluster.recover_node(follower).await;
        cluster.tick_and_route(3).await;

        // Crash AGAIN during recovery
        cluster.crash_node(follower);

        // Propose even more entries
        for i in 21..=25 {
            cluster.propose(leader, Value::with_id(i)).await;
        }
        cluster.tick_and_route(100).await;

        // Recover a second time
        cluster.recover_node(follower).await;
        cluster.tick_and_route(500).await;

        // Node must reach consistent state
        assert_logs_consistent(&cluster).await;
        let follower_decided = cluster.nodes[&follower].get_decided_idx();
        let leader_decided = cluster.nodes[&leader].get_decided_idx();
        assert_eq!(
            follower_decided, leader_decided,
            "follower should catch up after double-crash recovery"
        );
    });
}

// ===========================================================================
// Coverage gap: Partition heals then immediately re-partitions
// ===========================================================================

/// Heal a partition, then immediately create a different partition.
/// The cluster must not lose data or enter an inconsistent state.
#[test]
fn test_partition_heal_then_repartition() {
    smol::block_on(async {
        let mut cluster = SimCluster::new(vec![1, 2, 3, 4, 5]).await;
        let leader = elect_leader(&mut cluster).await;

        // Propose and decide entries
        for i in 1..=10 {
            cluster.propose(leader, Value::with_id(i)).await;
        }
        cluster.tick_and_route(200).await;

        // Partition: [1,2] from [3,4,5]
        let handle1 = cluster.partition(vec![1, 2], vec![3, 4, 5]);
        cluster.tick_and_route(50).await;

        // Heal first partition
        cluster.remove_filter(handle1);

        // Immediately re-partition differently: [1,3,5] from [2,4]
        let _handle2 = cluster.partition(vec![1, 3, 5], vec![2, 4]);

        // Tick — majority side [1,3,5] should be able to elect leader and decide
        cluster.tick_and_route(500).await;

        // Heal all
        cluster.clear_filters();
        for pid in 1..=5 {
            if !cluster.is_crashed(pid) {
                for other in 1..=5 {
                    if other != pid {
                        cluster.nodes.get_mut(&pid).unwrap().reconnected(other);
                    }
                }
            }
        }
        cluster.tick_and_route(500).await;

        // All logs consistent
        assert_logs_consistent(&cluster).await;
    });
}

// ===========================================================================
// Infrastructure additions for advanced tests
// ===========================================================================

impl SimCluster {
    /// Block messages in one direction only: from `from` to `to`.
    fn block_one_way(&mut self, from: u64, to: u64) -> usize {
        self.add_filter(Box::new(move |msg| {
            !(msg.get_sender() == from && msg.get_receiver() == to)
        }))
    }

    /// Route messages with a delay buffer. Messages matching `delay_pred`
    /// are held for `delay_rounds` before delivery. Others are delivered immediately.
    async fn tick_and_route_with_delays(
        &mut self,
        rounds: usize,
        delay_buffer: &mut Vec<(usize, Message<Value>)>,
        delay_rounds: usize,
        delay_pred: &dyn Fn(&Message<Value>) -> bool,
    ) {
        for round in 0..rounds {
            self.tick_all().await;
            // Collect outgoing
            let mut all_msgs: Vec<Message<Value>> = Vec::new();
            let mut buf = Vec::new();
            for node in self.nodes.values_mut() {
                node.take_outgoing_messages(&mut buf);
                all_msgs.append(&mut buf);
            }
            // Sort into immediate vs delayed
            for msg in all_msgs {
                let dominated = self
                    .filters
                    .iter()
                    .any(|f| f.as_ref().map_or(false, |pred| !pred(&msg)));
                if dominated {
                    self.dropped_messages.push(msg);
                    continue;
                }
                if delay_pred(&msg) {
                    delay_buffer.push((round + delay_rounds, msg));
                } else {
                    let receiver = msg.get_receiver();
                    if let Some(node) = self.nodes.get_mut(&receiver) {
                        node.handle_incoming(msg).await.unwrap();
                    }
                }
            }
            // Deliver delayed messages whose time has come
            let mut remaining = Vec::new();
            for (deliver_at, msg) in delay_buffer.drain(..) {
                if deliver_at <= round {
                    let receiver = msg.get_receiver();
                    if let Some(node) = self.nodes.get_mut(&receiver) {
                        node.handle_incoming(msg).await.unwrap();
                    }
                } else {
                    remaining.push((deliver_at, msg));
                }
            }
            *delay_buffer = remaining;
        }
        // Deliver any remaining delayed messages
        for (_deliver_at, msg) in delay_buffer.drain(..) {
            let receiver = msg.get_receiver();
            if let Some(node) = self.nodes.get_mut(&receiver) {
                node.handle_incoming(msg).await.unwrap();
            }
        }
    }
}

fn test_seed(default: u64) -> u64 {
    std::env::var("OMNIPAXOS_TEST_SEED")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(default)
}

/// Simple deterministic LCG PRNG for reproducible tests.
struct SimpleRng {
    state: u64,
}

impl SimpleRng {
    fn new(seed: u64) -> Self {
        SimpleRng { state: seed }
    }
    fn next(&mut self) -> u64 {
        self.state = self
            .state
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        self.state >> 33
    }
    fn next_range(&mut self, max: u64) -> u64 {
        if max == 0 {
            return 0;
        }
        self.next() % max
    }
}

// ===========================================================================
// 6.1 Stress Tests
// ===========================================================================

#[test]
fn test_stress_random_crashes_500() {
    smol::block_on(async {
        let nodes = vec![1, 2, 3, 4, 5, 6, 7];
        let mut cluster = SimCluster::new(nodes.clone()).await;
        let mut rng = SimpleRng::new(test_seed(42));
        let mut proposal_id = 1u64;

        for _iter in 0..500 {
            // Randomly crash or recover a node
            let target = nodes[rng.next_range(nodes.len() as u64) as usize];
            if cluster.is_crashed(target) {
                cluster.recover_node(target).await;
            } else {
                // Keep majority alive (need 4 of 7)
                let live_count = cluster.live_nodes().len();
                if live_count > 4 {
                    cluster.crash_node(target);
                }
            }

            // Propose on a random live node
            let live = cluster.live_nodes();
            if !live.is_empty() {
                let proposer = live[rng.next_range(live.len() as u64) as usize];
                let _ = cluster
                    .try_propose(proposer, Value::with_id(proposal_id))
                    .await;
                proposal_id += 1;
            }

            cluster.tick_and_route(5).await;
        }

        // Recover all crashed nodes and converge
        for &pid in &nodes {
            if cluster.is_crashed(pid) {
                cluster.recover_node(pid).await;
            }
        }
        cluster.tick_and_route(500).await;

        cluster.assert_logs_consistent_and_converged().await;
    });
}

#[test]
fn test_stress_random_partitions_500() {
    smol::block_on(async {
        let nodes = vec![1, 2, 3, 4, 5];
        let mut cluster = SimCluster::new(nodes.clone()).await;
        let mut rng = SimpleRng::new(test_seed(123));
        let mut proposal_id = 1u64;
        let mut active_filter: Option<usize> = None;

        for _iter in 0..500 {
            // Randomly partition or heal
            if rng.next_range(3) == 0 {
                if let Some(f) = active_filter.take() {
                    cluster.remove_filter(f);
                    // Reconnect all
                    for &pid in &nodes {
                        if !cluster.is_crashed(pid) {
                            for &other in &nodes {
                                if other != pid && !cluster.is_crashed(other) {
                                    cluster.nodes.get_mut(&pid).unwrap().reconnected(other);
                                }
                            }
                        }
                    }
                } else {
                    // Create a random partition
                    let split = (rng.next_range(3) + 1) as usize; // 1..=3
                    let group_a: Vec<u64> = nodes[..split].to_vec();
                    let group_b: Vec<u64> = nodes[split..].to_vec();
                    active_filter = Some(cluster.partition(group_a, group_b));
                }
            }

            // Propose on a random live node
            let live = cluster.live_nodes();
            let proposer = live[rng.next_range(live.len() as u64) as usize];
            let _ = cluster
                .try_propose(proposer, Value::with_id(proposal_id))
                .await;
            proposal_id += 1;

            cluster.tick_and_route(3).await;
        }

        // Heal and converge
        cluster.clear_filters();
        for &pid in &nodes {
            if !cluster.is_crashed(pid) {
                for &other in &nodes {
                    if other != pid && !cluster.is_crashed(other) {
                        cluster.nodes.get_mut(&pid).unwrap().reconnected(other);
                    }
                }
            }
        }
        cluster.tick_and_route(500).await;

        cluster.assert_logs_consistent_and_converged().await;
    });
}

#[test]
fn test_stress_leader_crashes_during_proposals_100() {
    smol::block_on(async {
        let nodes = vec![1, 2, 3, 4, 5];
        let mut cluster = SimCluster::new(nodes.clone()).await;
        let mut proposal_id = 1u64;

        for _iter in 0..100 {
            let leader = elect_leader(&mut cluster).await;

            // Propose immediately then crash
            let _ = cluster
                .try_propose(leader, Value::with_id(proposal_id))
                .await;
            proposal_id += 1;
            cluster.crash_node(leader);

            // Let survivors elect and make progress
            cluster.tick_and_route(100).await;

            // Recover crashed leader
            cluster.recover_node(leader).await;
            cluster.tick_and_route(50).await;
        }

        // Final convergence
        for &pid in &nodes {
            if cluster.is_crashed(pid) {
                cluster.recover_node(pid).await;
            }
        }
        cluster.tick_and_route(500).await;

        cluster.assert_logs_consistent_and_converged().await;
    });
}

#[test]
fn test_stress_multiple_simultaneous_crashes() {
    smol::block_on(async {
        let nodes = vec![1, 2, 3, 4, 5, 6, 7];
        let mut cluster = SimCluster::new(nodes.clone()).await;
        let mut rng = SimpleRng::new(test_seed(999));
        let mut proposal_id = 1u64;

        for _iter in 0..100 {
            elect_leader(&mut cluster).await;

            // Propose some entries
            let live = cluster.live_nodes();
            if !live.is_empty() {
                let proposer = live[0];
                for _ in 0..3 {
                    let _ = cluster
                        .try_propose(proposer, Value::with_id(proposal_id))
                        .await;
                    proposal_id += 1;
                }
            }
            cluster.tick_and_route(20).await;

            // Crash up to f=3 nodes simultaneously
            let crash_count = (rng.next_range(3) + 1) as usize; // 1..=3
            let live = cluster.live_nodes();
            let mut crashed_this_round = Vec::new();
            for _ in 0..crash_count.min(live.len().saturating_sub(4)) {
                let idx = (rng.next_range(live.len() as u64)) as usize;
                let target = live[idx % live.len()];
                if !crashed_this_round.contains(&target) && cluster.live_nodes().len() > 4 {
                    cluster.crash_node(target);
                    crashed_this_round.push(target);
                }
            }

            cluster.tick_and_route(100).await;

            // Recover all crashed nodes
            for pid in crashed_this_round {
                cluster.recover_node(pid).await;
            }
            cluster.tick_and_route(50).await;
        }

        // Final convergence
        for &pid in &nodes {
            if cluster.is_crashed(pid) {
                cluster.recover_node(pid).await;
            }
        }
        cluster.tick_and_route(500).await;

        cluster.assert_logs_consistent_and_converged().await;
    });
}

#[test]
fn test_stress_rapid_leader_changes() {
    smol::block_on(async {
        let nodes = vec![1, 2, 3, 4, 5];
        let mut cluster = SimCluster::new(nodes.clone()).await;
        let mut rng = SimpleRng::new(test_seed(7777));
        let mut proposal_id = 1u64;

        elect_leader(&mut cluster).await;

        for _iter in 0..100 {
            // try_become_leader on a random node
            let target = nodes[rng.next_range(nodes.len() as u64) as usize];
            let _ = cluster
                .nodes
                .get_mut(&target)
                .unwrap()
                .try_become_leader()
                .await;
            cluster.tick_and_route(10).await;

            // Propose on a random node
            let proposer = nodes[rng.next_range(nodes.len() as u64) as usize];
            let _ = cluster
                .try_propose(proposer, Value::with_id(proposal_id))
                .await;
            proposal_id += 1;

            cluster.tick_and_route(10).await;
        }

        // Converge
        cluster.tick_and_route(500).await;

        cluster.assert_logs_consistent_and_converged().await;
        // At least some entries decided
        let max_decided = cluster
            .get_decided_indices()
            .values()
            .copied()
            .max()
            .unwrap_or(0);
        assert!(
            max_decided >= 20,
            "rapid leader changes test should decide at least 20 entries, got {}",
            max_decided
        );
    });
}

// ===========================================================================
// 6.2 Asymmetric Partition Tests
// ===========================================================================

#[test]
fn test_asymmetric_leader_cannot_receive() {
    smol::block_on(async {
        let mut cluster = SimCluster::new(vec![1, 2, 3]).await;
        let leader = elect_leader(&mut cluster).await;

        // Block all messages TO the leader (it can still send)
        let followers: Vec<u64> = vec![1, 2, 3].into_iter().filter(|&p| p != leader).collect();
        let mut filter_handles = Vec::new();
        for &f in &followers {
            filter_handles.push(cluster.block_one_way(f, leader));
        }

        // The leader can send AcceptDecide but never gets Accepted back
        let _ = cluster.try_propose(leader, Value::with_id(1)).await;
        cluster.tick_and_route(200).await;

        // Leader should lose leadership (BLE happiness check fails without quorum)
        // A new leader should emerge among the followers
        cluster.clear_filters();
        for &pid in &[1u64, 2, 3] {
            for &other in &[1u64, 2, 3] {
                if pid != other {
                    cluster.nodes.get_mut(&pid).unwrap().reconnected(other);
                }
            }
        }
        cluster.tick_and_route(500).await;

        assert_logs_consistent(&cluster).await;
    });
}

#[test]
fn test_asymmetric_follower_cannot_send() {
    smol::block_on(async {
        let mut cluster = SimCluster::new(vec![1, 2, 3]).await;
        let leader = elect_leader(&mut cluster).await;

        let followers: Vec<u64> = vec![1, 2, 3].into_iter().filter(|&p| p != leader).collect();
        // Block one follower from sending (it can still receive)
        let blocked_follower = followers[0];
        let other_follower = followers[1];
        // Block messages FROM blocked_follower TO everyone
        for &target in &[leader, other_follower] {
            cluster.block_one_way(blocked_follower, target);
        }

        // Leader sends AcceptDecide to both followers.
        // Only other_follower's Accepted arrives — but that's enough for quorum (2/3).
        for i in 1..=5 {
            let _ = cluster.try_propose(leader, Value::with_id(i)).await;
        }
        cluster.tick_and_route(200).await;

        // Leader + other_follower should have decided
        assert!(
            cluster.nodes[&leader].get_decided_idx() >= 5,
            "leader should decide with one working follower"
        );
        assert!(
            cluster.nodes[&other_follower].get_decided_idx() >= 5,
            "working follower should decide"
        );
    });
}

#[test]
fn test_asymmetric_one_way_ble_loss() {
    smol::block_on(async {
        let mut cluster = SimCluster::new(vec![1, 2, 3]).await;

        // Block BLE messages from node 3 to everyone (it can receive BLE)
        // But allow Paxos messages through
        let _h = cluster.add_filter(Box::new(move |msg| {
            match msg {
                Message::BLE(ble) if ble.from == 3 => false, // drop
                _ => true,
            }
        }));

        // Election should still work (nodes 1 and 2 can exchange BLE)
        let leader = elect_leader(&mut cluster).await;

        for i in 1..=5 {
            cluster.propose(leader, Value::with_id(i)).await;
        }
        cluster.tick_and_route(200).await;

        // All nodes should be decided (Paxos messages are unaffected)
        for node in cluster.nodes.values() {
            assert!(
                node.get_decided_idx() >= 5,
                "all nodes should decide despite one-way BLE loss"
            );
        }
        assert_logs_consistent(&cluster).await;
    });
}

#[test]
fn test_asymmetric_between_followers() {
    smol::block_on(async {
        let mut cluster = SimCluster::new(vec![1, 2, 3]).await;
        let leader = elect_leader(&mut cluster).await;

        let followers: Vec<u64> = vec![1, 2, 3].into_iter().filter(|&p| p != leader).collect();
        // Block all communication between the two followers (both directions)
        cluster.block_one_way(followers[0], followers[1]);
        cluster.block_one_way(followers[1], followers[0]);

        // This should not affect consensus — followers only talk to the leader
        for i in 1..=10 {
            cluster.propose(leader, Value::with_id(i)).await;
        }
        cluster.tick_and_route(200).await;

        for node in cluster.nodes.values() {
            assert!(
                node.get_decided_idx() >= 10,
                "follower-to-follower partition should not affect consensus"
            );
        }
        assert_logs_consistent(&cluster).await;
    });
}

#[test]
fn test_asymmetric_leader_cannot_send() {
    smol::block_on(async {
        let mut cluster = SimCluster::new(vec![1, 2, 3]).await;
        let leader = elect_leader(&mut cluster).await;

        // Block all messages FROM the leader
        let _h = cluster.add_filter(Box::new(move |msg| msg.get_sender() != leader));

        // Leader can receive but cannot send — followers should eventually
        // elect a new leader since they don't hear from the old one
        cluster.tick_and_route(200).await;

        // Heal and let a new leader emerge
        cluster.clear_filters();
        for &pid in &[1u64, 2, 3] {
            for &other in &[1u64, 2, 3] {
                if pid != other {
                    cluster.nodes.get_mut(&pid).unwrap().reconnected(other);
                }
            }
        }
        cluster.tick_and_route(500).await;

        // Propose through the new leader
        let new_leader = elect_leader(&mut cluster).await;
        for i in 1..=5 {
            cluster.propose(new_leader, Value::with_id(i)).await;
        }
        cluster.tick_and_route(200).await;

        for node in cluster.nodes.values() {
            assert!(
                node.get_decided_idx() >= 5,
                "new leader should make progress after old leader's outbound was blocked"
            );
        }
        assert_logs_consistent(&cluster).await;
    });
}

// ===========================================================================
// 6.3 Out-of-Order Delivery Tests
// ===========================================================================

#[test]
fn test_reordered_acceptdecide() {
    smol::block_on(async {
        let mut cluster = SimCluster::new(vec![1, 2, 3]).await;
        let leader = elect_leader(&mut cluster).await;

        // Delay AcceptDecide messages by varying amounts
        let mut delay_buffer: Vec<(usize, Message<Value>)> = Vec::new();
        for i in 1..=10 {
            cluster.propose(leader, Value::with_id(i)).await;
        }

        cluster
            .tick_and_route_with_delays(
                300,
                &mut delay_buffer,
                3, // delay AcceptDecide by 3 rounds
                &|msg| is_accept_decide(msg),
            )
            .await;

        // Despite delays, sequence numbers should handle reordering
        assert_logs_consistent(&cluster).await;
        let max_decided = cluster
            .get_decided_indices()
            .values()
            .copied()
            .max()
            .unwrap_or(0);
        assert!(
            max_decided >= 10,
            "all entries should eventually be decided"
        );
    });
}

#[test]
fn test_decide_before_acceptdecide() {
    smol::block_on(async {
        let mut cluster = SimCluster::new(vec![1, 2, 3]).await;
        let leader = elect_leader(&mut cluster).await;

        // Delay AcceptDecide heavily but let Decide through
        let mut delay_buffer: Vec<(usize, Message<Value>)> = Vec::new();
        for i in 1..=5 {
            cluster.propose(leader, Value::with_id(i)).await;
        }

        // Delay AcceptDecide by 10 rounds — Decide messages will arrive first
        cluster
            .tick_and_route_with_delays(500, &mut delay_buffer, 10, &|msg| is_accept_decide(msg))
            .await;

        // The system should still converge (resend mechanism recovers)
        assert_logs_consistent(&cluster).await;
    });
}

#[test]
fn test_late_promise_after_quorum() {
    smol::block_on(async {
        let mut cluster = SimCluster::new(vec![1, 2, 3, 4, 5]).await;

        // Delay Promise messages from node 5 by many rounds
        let mut delay_buffer: Vec<(usize, Message<Value>)> = Vec::new();
        // The election should succeed with 3/5 promises; node 5's late promise
        // should be handled gracefully
        cluster
            .tick_and_route_with_delays(500, &mut delay_buffer, 50, &|msg| {
                matches!(
                    msg,
                    Message::SequencePaxos(PaxosMessage {
                        from: 5,
                        msg: PaxosMsg::Promise(_),
                        ..
                    })
                )
            })
            .await;

        let leader = cluster.get_elected_leader();
        assert!(
            leader.is_some(),
            "leader should be elected despite slow promise from node 5"
        );

        // Propose and decide entries
        let leader = leader.unwrap();
        for i in 1..=5 {
            cluster.propose(leader, Value::with_id(i)).await;
        }
        cluster.tick_and_route(300).await;

        assert_logs_consistent(&cluster).await;
    });
}

#[test]
fn test_acceptsync_after_newer_acceptdecide() {
    smol::block_on(async {
        let mut cluster = SimCluster::new(vec![1, 2, 3]).await;
        let leader = elect_leader(&mut cluster).await;

        // Propose entries
        for i in 1..=5 {
            cluster.propose(leader, Value::with_id(i)).await;
        }
        cluster.tick_and_route(100).await;

        // Crash and recover a follower — it will get AcceptSync
        let followers: Vec<u64> = vec![1, 2, 3].into_iter().filter(|&p| p != leader).collect();
        let target = followers[0];
        cluster.crash_node(target);
        cluster.tick_and_route(10).await;
        cluster.recover_node(target).await;

        // Delay AcceptSync to the recovered node
        let mut delay_buffer: Vec<(usize, Message<Value>)> = Vec::new();

        // Meanwhile, propose more entries that generate AcceptDecide
        for i in 100..=105 {
            let _ = cluster.try_propose(leader, Value::with_id(i)).await;
        }

        cluster.tick_and_route_with_delays(
            300,
            &mut delay_buffer,
            20,
            &|msg| {
                matches!(msg,
                    Message::SequencePaxos(PaxosMessage { msg: PaxosMsg::AcceptSync(_), to, .. }) if *to == target
                )
            },
        ).await;

        assert_logs_consistent(&cluster).await;
    });
}

// ===========================================================================
// 6.4 Concurrent Proposal + Reconfiguration Tests
// ===========================================================================

#[test]
fn test_proposals_in_flight_when_reconfigure() {
    smol::block_on(async {
        let mut cluster = SimCluster::new(vec![1, 2, 3]).await;
        let leader = elect_leader(&mut cluster).await;

        // Propose entries then immediately reconfigure
        for i in 1..=5 {
            cluster.propose(leader, Value::with_id(i)).await;
        }

        let new_config = ClusterConfig {
            configuration_id: 2,
            nodes: vec![1, 2, 3, 4, 5],
            ..Default::default()
        };
        let reconfig_result = cluster
            .nodes
            .get_mut(&leader)
            .unwrap()
            .reconfigure(new_config, None)
            .await;
        assert!(reconfig_result.is_ok(), "reconfigure should succeed");

        // Let everything settle
        cluster.tick_and_route(500).await;

        // All entries should be decided AND the stopsign should be decided
        for (&pid, node) in &cluster.nodes {
            assert!(
                node.get_decided_idx() >= 5,
                "node {} should have decided the proposed entries",
                pid
            );
            assert!(
                node.is_reconfigured().is_some(),
                "node {} should see the reconfiguration as decided",
                pid
            );
        }
        assert_logs_consistent(&cluster).await;
    });
}

#[test]
fn test_proposal_rejected_after_stopsign() {
    smol::block_on(async {
        let mut cluster = SimCluster::new(vec![1, 2, 3]).await;
        let leader = elect_leader(&mut cluster).await;

        // Accept reconfiguration
        let new_config = ClusterConfig {
            configuration_id: 2,
            nodes: vec![1, 2, 3, 4],
            ..Default::default()
        };
        cluster
            .nodes
            .get_mut(&leader)
            .unwrap()
            .reconfigure(new_config, None)
            .await
            .unwrap();
        cluster.tick_and_route(200).await;

        // Further proposals should be rejected
        let result = cluster.try_propose(leader, Value::with_id(999)).await;
        assert!(
            result.is_err(),
            "proposals after accepted reconfiguration should be rejected"
        );
    });
}

#[test]
fn test_concurrent_reconfigure_from_follower_and_leader() {
    smol::block_on(async {
        let mut cluster = SimCluster::new(vec![1, 2, 3]).await;
        let leader = elect_leader(&mut cluster).await;
        let follower = if leader == 1 { 2 } else { 1 };

        // Both try to reconfigure simultaneously
        let config1 = ClusterConfig {
            configuration_id: 2,
            nodes: vec![1, 2, 3, 4],
            ..Default::default()
        };
        let config2 = ClusterConfig {
            configuration_id: 2,
            nodes: vec![1, 2, 3, 5],
            ..Default::default()
        };

        let _r1 = cluster
            .nodes
            .get_mut(&leader)
            .unwrap()
            .reconfigure(config1, None)
            .await;
        let _r2 = cluster
            .nodes
            .get_mut(&follower)
            .unwrap()
            .reconfigure(config2, None)
            .await;

        cluster.tick_and_route(500).await;

        // Exactly one config should win — all nodes agree
        let mut stop_signs: Vec<Option<StopSign>> = Vec::new();
        for node in cluster.nodes.values() {
            stop_signs.push(node.is_reconfigured());
        }

        // All reconfigured nodes should agree on the same stopsign
        let decided_ss: Vec<&StopSign> = stop_signs.iter().filter_map(|s| s.as_ref()).collect();
        assert!(
            decided_ss.len() >= 2,
            "at least 2 nodes must have decided a StopSign, but only {} did",
            decided_ss.len()
        );
        if decided_ss.len() >= 2 {
            for ss in &decided_ss[1..] {
                assert_eq!(
                    decided_ss[0], *ss,
                    "all nodes must agree on the same reconfiguration"
                );
            }
        }
    });
}

#[test]
fn test_leader_crash_during_reconfig() {
    smol::block_on(async {
        let mut cluster = SimCluster::new(vec![1, 2, 3]).await;
        let leader = elect_leader(&mut cluster).await;

        // Leader accepts StopSign
        let new_config = ClusterConfig {
            configuration_id: 2,
            nodes: vec![1, 2, 3, 4],
            ..Default::default()
        };
        cluster
            .nodes
            .get_mut(&leader)
            .unwrap()
            .reconfigure(new_config, None)
            .await
            .unwrap();

        // One round of replication then crash
        cluster.tick_and_route(1).await;
        cluster.crash_node(leader);

        // Survivors should eventually elect a new leader and decide the StopSign
        cluster.tick_and_route(500).await;

        // Recover crashed leader
        cluster.recover_node(leader).await;
        cluster.tick_and_route(500).await;

        // All should agree on the reconfiguration
        assert_logs_consistent(&cluster).await;
    });
}

#[test]
fn test_reconfig_with_concurrent_crash_recovery() {
    smol::block_on(async {
        let mut cluster = SimCluster::new(vec![1, 2, 3, 4, 5]).await;
        let leader = elect_leader(&mut cluster).await;

        // Crash a follower
        let follower = if leader == 1 { 2 } else { 1 };
        cluster.crash_node(follower);

        // Reconfigure while follower is crashed
        let new_config = ClusterConfig {
            configuration_id: 2,
            nodes: vec![1, 2, 3, 4, 5, 6],
            ..Default::default()
        };
        cluster
            .nodes
            .get_mut(&leader)
            .unwrap()
            .reconfigure(new_config, None)
            .await
            .unwrap();

        cluster.tick_and_route(200).await;

        // Recover the crashed follower
        cluster.recover_node(follower).await;
        cluster.tick_and_route(500).await;

        // All nodes should agree
        assert_logs_consistent(&cluster).await;
        // The reconfiguration should be decided
        for node in cluster.nodes.values() {
            assert!(
                node.is_reconfigured().is_some(),
                "all nodes should see the reconfiguration"
            );
        }
    });
}

// ===========================================================================
// 6.5 Bug Fix Regression Tests
// ===========================================================================

#[test]
fn test_unknown_pid_handled_gracefully() {
    smol::block_on(async {
        let mut cluster = SimCluster::new(vec![1, 2, 3]).await;
        elect_leader(&mut cluster).await;

        // Send a message from an unknown PID (999) — should not panic
        let fake_msg = Message::SequencePaxos(PaxosMessage {
            from: 999,
            to: 1,
            msg: PaxosMsg::PrepareReq(omnipaxos::messages::sequence_paxos::PrepareReq {
                n: Ballot::default(),
            }),
        });
        let result = cluster
            .nodes
            .get_mut(&1)
            .unwrap()
            .handle_incoming(fake_msg)
            .await;
        assert!(
            result.is_ok(),
            "unknown PID should be silently ignored, not panic"
        );
    });
}

#[test]
fn test_buffer_overflow_recovered_via_resend() {
    smol::block_on(async {
        // Small buffer to trigger overflow
        let cluster_config = ClusterConfig {
            configuration_id: 1,
            nodes: vec![1, 2, 3],
            ..Default::default()
        };
        let mut server_configs = BTreeMap::new();
        for pid in [1, 2, 3] {
            server_configs.insert(
                pid,
                ServerConfig {
                    pid,
                    buffer_size: 10,
                    election_tick_timeout: 5,
                    ..Default::default()
                },
            );
        }
        let mut cluster = SimCluster::with_configs(cluster_config, server_configs).await;
        let leader = elect_leader(&mut cluster).await;

        // Flood proposals — some messages will be dropped
        for i in 1..=50 {
            let _ = cluster.try_propose(leader, Value::with_id(i)).await;
        }

        // Give plenty of time for resends to recover
        cluster.tick_and_route(1000).await;

        // The dropped_message_count should be > 0 (overflow occurred)
        let drop_count = cluster.nodes[&leader].get_dropped_message_count();
        // Note: We can't guarantee drops since buffer might be drained fast enough,
        // but the system should remain consistent regardless
        assert_logs_consistent(&cluster).await;

        // Verify at least some entries decided
        let max_decided = cluster
            .get_decided_indices()
            .values()
            .copied()
            .max()
            .unwrap_or(0);
        assert!(
            max_decided >= 10,
            "at least 10 entries should be decided despite potential buffer overflow (drops: {}), got {}",
            drop_count,
            max_decided
        );
    });
}

#[test]
fn test_compaction_err_storage_error_variant() {
    // Verify the Storage variant works in the CompactionErr enum
    use omnipaxos::errors::{AnyError, StorageError};
    use omnipaxos::CompactionErr;

    let any_err = AnyError::new(std::io::Error::new(
        std::io::ErrorKind::Other,
        "test error",
    ));
    let storage_err = StorageError::write_batch(any_err);
    let compaction_err = CompactionErr::Storage(storage_err);

    // Should display properly
    let display = format!("{}", compaction_err);
    assert!(
        display.contains("storage error"),
        "CompactionErr::Storage should display properly: {}",
        display
    );

    // Should be Debug-able
    let debug = format!("{:?}", compaction_err);
    assert!(
        !debug.is_empty(),
        "CompactionErr::Storage should be debuggable"
    );
}

// ===========================================================================
// 6.6 Flexible Quorum Edge Cases Under Partitions
// ===========================================================================

#[test]
fn test_flexible_quorum_write_quorum_2_of_5() {
    // write_quorum=2, read_quorum=4 (2+4=6 > 5, overlap satisfied)
    // With write_quorum=2, leader + 1 follower can decide.
    // Verify no split-brain when the other 3 are partitioned away.
    smol::block_on(async {
        let cluster_config = ClusterConfig {
            configuration_id: 1,
            nodes: vec![1, 2, 3, 4, 5],
            flexible_quorum: Some(FlexibleQuorum {
                read_quorum_size: 4,
                write_quorum_size: 2,
            }),
        };
        let mut server_configs = BTreeMap::new();
        for pid in 1..=5 {
            server_configs.insert(
                pid,
                ServerConfig {
                    pid,
                    election_tick_timeout: 5,
                    ..Default::default()
                },
            );
        }
        let mut cluster = SimCluster::with_configs(cluster_config, server_configs).await;
        let leader = elect_leader(&mut cluster).await;

        // Propose and decide with full cluster
        for i in 1..=5 {
            cluster.propose(leader, Value::with_id(i)).await;
        }
        cluster.tick_and_route(200).await;

        // Partition: isolate 3 followers from leader
        let followers: Vec<u64> = (1..=5).filter(|&p| p != leader).collect();
        let isolated = vec![followers[1], followers[2], followers[3]];
        let connected = vec![leader, followers[0]];
        let filter = cluster.partition(connected.clone(), isolated.clone());

        // Leader + 1 follower should still be able to decide (write_quorum=2)
        for i in 10..=15 {
            cluster.propose(leader, Value::with_id(i)).await;
        }
        cluster.tick_and_route(300).await;

        // Leader and connected follower should have decided
        let leader_decided = cluster.nodes[&leader].get_decided_idx();
        assert!(
            leader_decided >= 11,
            "leader should decide with write_quorum=2, got {}",
            leader_decided
        );
        let follower_decided = cluster.nodes[&followers[0]].get_decided_idx();
        assert!(
            follower_decided >= 11,
            "connected follower should decide with write_quorum=2, got {}",
            follower_decided
        );

        // Isolated nodes should NOT have advanced
        for &pid in &isolated {
            assert_eq!(
                cluster.nodes[&pid].get_decided_idx(),
                5,
                "isolated node {} should still be at decided_idx=5",
                pid
            );
        }

        // Heal partition and verify convergence
        cluster.remove_filter(filter);
        for &pid in &[1, 2, 3, 4, 5] {
            for &other in &[1, 2, 3, 4, 5] {
                if pid != other {
                    cluster.nodes.get_mut(&pid).unwrap().reconnected(other);
                }
            }
        }
        cluster.tick_and_route(500).await;
        assert_logs_consistent(&cluster).await;
    });
}

#[test]
fn test_flexible_quorum_leader_isolated_from_write_quorum() {
    // write_quorum=3, read_quorum=3 (3+3=6 > 5)
    // Partition leader from all followers — leader can NOT decide alone.
    // After healing, a new leader must be elected and data must converge.
    smol::block_on(async {
        let cluster_config = ClusterConfig {
            configuration_id: 1,
            nodes: vec![1, 2, 3, 4, 5],
            flexible_quorum: Some(FlexibleQuorum {
                read_quorum_size: 3,
                write_quorum_size: 3,
            }),
        };
        let mut server_configs = BTreeMap::new();
        for pid in 1..=5 {
            server_configs.insert(
                pid,
                ServerConfig {
                    pid,
                    election_tick_timeout: 5,
                    ..Default::default()
                },
            );
        }
        let mut cluster = SimCluster::with_configs(cluster_config, server_configs).await;
        let leader = elect_leader(&mut cluster).await;

        // Decide initial entries
        for i in 1..=5 {
            cluster.propose(leader, Value::with_id(i)).await;
        }
        cluster.tick_and_route(200).await;

        // Isolate the leader completely
        let filter = cluster.isolate_node(leader);

        // Leader proposes but can't reach quorum (needs 3, only has self)
        for i in 100..=105 {
            let _ = cluster.try_propose(leader, Value::with_id(i)).await;
        }
        cluster.tick_and_route(300).await;

        // Leader should NOT have decided past initial entries
        // (write_quorum=3, leader alone is only 1)
        let leader_decided = cluster.nodes[&leader].get_decided_idx();
        assert_eq!(
            leader_decided, 5,
            "isolated leader should not decide with write_quorum=3"
        );

        // Heal and converge
        cluster.remove_filter(filter);
        for pid in 1..=5 {
            for other in 1..=5 {
                if pid != other {
                    cluster.nodes.get_mut(&pid).unwrap().reconnected(other);
                }
            }
        }
        cluster.tick_and_route(500).await;
        assert_logs_consistent(&cluster).await;
    });
}

#[test]
fn test_flexible_quorum_minority_write_quorum_no_split_brain() {
    // 3-node cluster with write_quorum=2, read_quorum=2 (2+2=4 > 3)
    // Partition into [1] vs [2,3]. Node 1 was leader.
    // [2,3] should elect a new leader (read_quorum=2 to prepare).
    // Node 1 alone cannot decide (write_quorum=2).
    // When healed, there must be NO divergent decided entries.
    smol::block_on(async {
        let cluster_config = ClusterConfig {
            configuration_id: 1,
            nodes: vec![1, 2, 3],
            flexible_quorum: Some(FlexibleQuorum {
                read_quorum_size: 2,
                write_quorum_size: 2,
            }),
        };
        let mut server_configs = BTreeMap::new();
        for pid in 1..=3 {
            server_configs.insert(
                pid,
                ServerConfig {
                    pid,
                    election_tick_timeout: 5,
                    ..Default::default()
                },
            );
        }
        let mut cluster = SimCluster::with_configs(cluster_config, server_configs).await;
        let leader = elect_leader(&mut cluster).await;

        // Decide initial entries
        for i in 1..=3 {
            cluster.propose(leader, Value::with_id(i)).await;
        }
        cluster.tick_and_route(200).await;
        assert_all_decided(&cluster, 3);

        // Partition leader from the other two
        let others: Vec<u64> = vec![1, 2, 3].into_iter().filter(|&p| p != leader).collect();
        let filter = cluster.partition(vec![leader], others.clone());

        // Old leader tries to propose — should fail to decide (only 1 of 2 needed)
        for i in 100..=102 {
            let _ = cluster.try_propose(leader, Value::with_id(i)).await;
        }

        // Majority side proposes through new leader
        cluster.tick_and_route(300).await;
        let new_leader = cluster.get_elected_leader();

        if let Some(nl) = new_leader {
            for i in 200..=202 {
                let _ = cluster.try_propose(nl, Value::with_id(i)).await;
            }
            cluster.tick_and_route(200).await;
        }

        // Heal and converge
        cluster.remove_filter(filter);
        for pid in 1..=3 {
            for other in 1..=3 {
                if pid != other {
                    cluster.nodes.get_mut(&pid).unwrap().reconnected(other);
                }
            }
        }
        cluster.tick_and_route(500).await;

        // Critical: no split-brain — all nodes must agree
        assert_logs_consistent(&cluster).await;
    });
}

// ===========================================================================
// 6.7 Snapshot + Compaction Races During Leader Changes
// ===========================================================================

#[test]
fn test_trim_without_snapshot_then_follower_recovery() {
    // Trim (without snapshot) compacts entries. When a slow follower
    // recovers, the leader's `get_suffix(from)` returns empty if
    // `from < trimmed_idx` — verify the system still converges using
    // the snapshot-based sync path.
    smol::block_on(async {
        let mut cluster = SimCluster::new(vec![1, 2, 3]).await;
        let leader = elect_leader(&mut cluster).await;

        // Decide entries
        for i in 1..=10 {
            cluster.propose(leader, Value::with_id(i)).await;
        }
        cluster.tick_and_route(200).await;
        assert_all_decided(&cluster, 10);

        // Crash a follower before trimming
        let follower = *cluster.live_nodes().iter().find(|&&p| p != leader).unwrap();
        cluster.crash_node(follower);

        // Leader trims decided entries — this removes log data without creating
        // a snapshot, so entries are gone.
        let trim_result = cluster.nodes.get_mut(&leader).unwrap().trim(Some(10)).await;
        assert!(
            trim_result.is_ok(),
            "trim should succeed: {:?}",
            trim_result
        );
        cluster.tick_and_route(50).await;

        // Propose more entries
        for i in 11..=15 {
            cluster.propose(leader, Value::with_id(i)).await;
        }
        cluster.tick_and_route(200).await;

        // Recover follower — it needs entries 0..10 but they are trimmed (no snapshot).
        // The leader creates a Complete snapshot via create_diff_snapshot when follower's
        // decided_idx is <= compacted_idx. This exercises the trim+sync path.
        cluster.recover_node(follower).await;
        cluster.tick_and_route(500).await;

        // All nodes should converge — the recovered follower may show
        // Trimmed/Snapshotted for early entries, but decided_idx should match.
        let leader_decided = cluster.nodes[&leader].get_decided_idx();
        let follower_decided = cluster.nodes[&follower].get_decided_idx();
        assert_eq!(
            leader_decided, follower_decided,
            "follower should catch up after trim-based recovery"
        );
        assert_logs_consistent(&cluster).await;
    });
}

#[test]
fn test_snapshot_then_leader_change_then_follower_sync() {
    // Create a snapshot, change leader, then recover a stale follower.
    // The new leader must use snapshot data to sync the follower.
    smol::block_on(async {
        let mut cluster = SimCluster::new(vec![1, 2, 3, 4, 5]).await;
        let leader = elect_leader(&mut cluster).await;

        // Decide entries
        for i in 1..=20 {
            cluster.propose(leader, Value::with_id(i)).await;
        }
        cluster.tick_and_route(200).await;

        // Crash a follower early (before snapshot)
        let followers: Vec<u64> = cluster
            .live_nodes()
            .into_iter()
            .filter(|&p| p != leader)
            .collect();
        let stale_follower = followers[0];
        cluster.crash_node(stale_follower);

        // Snapshot on leader and replicate compaction
        cluster
            .nodes
            .get_mut(&leader)
            .unwrap()
            .snapshot(Some(15), false)
            .await
            .unwrap();
        cluster.tick_and_route(100).await;

        // Now crash the original leader to force leader change
        cluster.crash_node(leader);
        cluster.tick_and_route(300).await;

        let new_leader = cluster.get_elected_leader();
        assert!(new_leader.is_some(), "new leader should be elected");
        let new_leader = new_leader.unwrap();

        // Propose more
        for i in 21..=25 {
            let _ = cluster.try_propose(new_leader, Value::with_id(i)).await;
        }
        cluster.tick_and_route(200).await;

        // Recover both crashed nodes
        cluster.recover_node(stale_follower).await;
        cluster.recover_node(leader).await;
        cluster.tick_and_route(500).await;

        assert_logs_consistent(&cluster).await;
    });
}

#[test]
fn test_concurrent_trim_and_leader_change() {
    // Leader trims log, then gets partitioned before the trim propagates.
    // New leader has untrimmed log. Verify consistency after heal.
    smol::block_on(async {
        let mut cluster = SimCluster::new(vec![1, 2, 3, 4, 5]).await;
        let leader = elect_leader(&mut cluster).await;

        // Decide entries
        for i in 1..=15 {
            cluster.propose(leader, Value::with_id(i)).await;
        }
        cluster.tick_and_route(200).await;

        // Leader creates snapshot
        cluster
            .nodes
            .get_mut(&leader)
            .unwrap()
            .snapshot(Some(10), false)
            .await
            .unwrap();

        // Route one tick to start propagating
        cluster.tick_and_route(1).await;

        // Immediately isolate the leader — compaction messages may not have reached all
        let filter = cluster.isolate_node(leader);
        cluster.tick_and_route(300).await;

        // Majority should elect a new leader
        let new_leader = cluster.get_elected_leader();
        assert!(new_leader.is_some(), "new leader should emerge");

        // New leader proposes
        let new_leader = new_leader.unwrap();
        for i in 100..=105 {
            let _ = cluster.try_propose(new_leader, Value::with_id(i)).await;
        }
        cluster.tick_and_route(200).await;

        // Heal
        cluster.remove_filter(filter);
        for pid in 1..=5 {
            for other in 1..=5 {
                if pid != other {
                    cluster.nodes.get_mut(&pid).unwrap().reconnected(other);
                }
            }
        }
        cluster.tick_and_route(500).await;
        assert_logs_consistent(&cluster).await;
    });
}

#[test]
fn test_snapshot_at_decided_boundary() {
    // Snapshot exactly at decided_idx (the maximum allowed).
    // Then propose more and verify the compacted boundary is handled.
    smol::block_on(async {
        let mut cluster = SimCluster::new(vec![1, 2, 3]).await;
        let leader = elect_leader(&mut cluster).await;

        for i in 1..=10 {
            cluster.propose(leader, Value::with_id(i)).await;
        }
        cluster.tick_and_route(200).await;
        assert_all_decided(&cluster, 10);

        // Snapshot at exactly decided_idx
        let decided_idx = cluster.nodes[&leader].get_decided_idx();
        cluster
            .nodes
            .get_mut(&leader)
            .unwrap()
            .snapshot(Some(decided_idx), false)
            .await
            .unwrap();
        cluster.tick_and_route(100).await;

        // Verify compacted_idx advanced
        let compacted = cluster.nodes[&leader].get_compacted_idx();
        assert_eq!(
            compacted, 10,
            "compacted_idx should be at decided_idx after snapshot"
        );

        // Propose more entries
        for i in 11..=20 {
            cluster.propose(leader, Value::with_id(i)).await;
        }
        cluster.tick_and_route(200).await;

        assert_all_decided(&cluster, 20);
        assert_logs_consistent(&cluster).await;
    });
}

// ===========================================================================
// 6.8 Batching Boundary Conditions
// ===========================================================================

#[test]
fn test_batch_size_1_correctness() {
    // batch_size=1 means every entry is flushed immediately (no batching).
    // Verify correctness under normal operation.
    smol::block_on(async {
        let cluster_config = ClusterConfig {
            configuration_id: 1,
            nodes: vec![1, 2, 3],
            ..Default::default()
        };
        let mut server_configs = BTreeMap::new();
        for pid in 1..=3 {
            server_configs.insert(
                pid,
                ServerConfig {
                    pid,
                    batch_size: 1,
                    election_tick_timeout: 5,
                    ..Default::default()
                },
            );
        }
        let mut cluster = SimCluster::with_configs(cluster_config, server_configs).await;
        let leader = elect_leader(&mut cluster).await;

        for i in 1..=20 {
            cluster.propose(leader, Value::with_id(i)).await;
        }
        cluster.tick_and_route(300).await;

        assert_all_decided(&cluster, 20);
        assert_logs_consistent(&cluster).await;
    });
}

#[test]
fn test_large_batch_size_with_few_entries() {
    // batch_size=100 but only propose 3 entries. The flush_batch_timeout
    // must eventually flush the partial batch.
    smol::block_on(async {
        let cluster_config = ClusterConfig {
            configuration_id: 1,
            nodes: vec![1, 2, 3],
            ..Default::default()
        };
        let mut server_configs = BTreeMap::new();
        for pid in 1..=3 {
            server_configs.insert(
                pid,
                ServerConfig {
                    pid,
                    batch_size: 100,
                    election_tick_timeout: 5,
                    flush_batch_tick_timeout: 10,
                    ..Default::default()
                },
            );
        }
        let mut cluster = SimCluster::with_configs(cluster_config, server_configs).await;
        let leader = elect_leader(&mut cluster).await;

        // Only propose 3 entries (well below batch_size=100)
        for i in 1..=3 {
            cluster.propose(leader, Value::with_id(i)).await;
        }

        // Give enough ticks for flush_batch_timeout to trigger
        cluster.tick_and_route(500).await;

        assert_all_decided(&cluster, 3);
        assert_logs_consistent(&cluster).await;
    });
}

#[test]
fn test_batch_size_1_single_node_cluster() {
    // Single-node cluster with batch_size=1: self-decided immediately.
    smol::block_on(async {
        let cluster_config = ClusterConfig {
            configuration_id: 1,
            nodes: vec![1],
            ..Default::default()
        };
        let mut server_configs = BTreeMap::new();
        server_configs.insert(
            1,
            ServerConfig {
                pid: 1,
                batch_size: 1,
                election_tick_timeout: 5,
                ..Default::default()
            },
        );
        let mut cluster = SimCluster::with_configs(cluster_config, server_configs).await;

        // Wait for self-election
        cluster.tick_and_route(50).await;

        for i in 1..=10 {
            cluster.propose(1, Value::with_id(i)).await;
        }
        cluster.tick_and_route(100).await;

        assert_all_decided(&cluster, 10);
    });
}

#[test]
fn test_batch_flush_during_leader_change() {
    // Leader has unflushed batched entries when it loses leadership.
    // The new leader must sync correctly even though the old leader
    // had entries only in its batch cache (not yet persisted).
    smol::block_on(async {
        let cluster_config = ClusterConfig {
            configuration_id: 1,
            nodes: vec![1, 2, 3],
            ..Default::default()
        };
        let mut server_configs = BTreeMap::new();
        for pid in 1..=3 {
            server_configs.insert(
                pid,
                ServerConfig {
                    pid,
                    batch_size: 50, // large batch — entries stay in cache
                    election_tick_timeout: 5,
                    flush_batch_tick_timeout: 200, // high so batch doesn't auto-flush
                    ..Default::default()
                },
            );
        }
        let mut cluster = SimCluster::with_configs(cluster_config, server_configs).await;
        let leader = elect_leader(&mut cluster).await;

        // Propose entries that stay in the batch cache (not flushed)
        for i in 1..=10 {
            cluster.propose(leader, Value::with_id(i)).await;
        }
        // Only tick a few rounds — not enough for flush_batch_timeout
        cluster.tick_and_route(3).await;

        // Crash the leader with unflushed batch
        cluster.crash_node(leader);

        // Elect new leader
        let new_leader = elect_leader(&mut cluster).await;
        assert_ne!(new_leader, leader);

        // Propose through new leader
        for i in 100..=110 {
            let _ = cluster.try_propose(new_leader, Value::with_id(i)).await;
        }
        cluster.tick_and_route(300).await;

        // Recover old leader
        cluster.recover_node(leader).await;
        cluster.tick_and_route(500).await;

        // Verify consistency — old leader's unflushed entries should NOT
        // appear in the decided log (they were never persisted)
        assert_logs_consistent(&cluster).await;
    });
}

#[test]
fn test_decided_idx_clamping_behavior() {
    // When flush_batch_and_set_decided_idx clamps decided_idx to
    // new_accepted_idx, the follower must still converge to the
    // correct decided_idx eventually.
    smol::block_on(async {
        let cluster_config = ClusterConfig {
            configuration_id: 1,
            nodes: vec![1, 2, 3],
            ..Default::default()
        };
        let mut server_configs = BTreeMap::new();
        for pid in 1..=3 {
            server_configs.insert(
                pid,
                ServerConfig {
                    pid,
                    batch_size: 5, // moderate batching
                    election_tick_timeout: 5,
                    flush_batch_tick_timeout: 10,
                    ..Default::default()
                },
            );
        }
        let mut cluster = SimCluster::with_configs(cluster_config, server_configs).await;
        let leader = elect_leader(&mut cluster).await;

        // Rapid-fire proposals to create batch pressure
        for i in 1..=30 {
            let _ = cluster.try_propose(leader, Value::with_id(i)).await;
        }

        // Tick enough for multiple flush cycles
        cluster.tick_and_route(500).await;

        // All entries should eventually be decided
        assert_all_decided(&cluster, 30);
        assert_logs_consistent(&cluster).await;
    });
}

#[test]
fn test_batch_size_exact_boundary() {
    // Propose exactly batch_size entries. The batch should flush exactly once.
    smol::block_on(async {
        let cluster_config = ClusterConfig {
            configuration_id: 1,
            nodes: vec![1, 2, 3],
            ..Default::default()
        };
        let mut server_configs = BTreeMap::new();
        for pid in 1..=3 {
            server_configs.insert(
                pid,
                ServerConfig {
                    pid,
                    batch_size: 5,
                    election_tick_timeout: 5,
                    flush_batch_tick_timeout: 10,
                    ..Default::default()
                },
            );
        }
        let mut cluster = SimCluster::with_configs(cluster_config, server_configs).await;
        let leader = elect_leader(&mut cluster).await;

        // Propose exactly batch_size entries — should trigger immediate flush
        for i in 1..=5 {
            cluster.propose(leader, Value::with_id(i)).await;
        }
        cluster.tick_and_route(200).await;

        assert_all_decided(&cluster, 5);

        // Propose batch_size+1 entries: 5 flush immediately, 1 flushes via timeout
        for i in 6..=11 {
            cluster.propose(leader, Value::with_id(i)).await;
        }
        cluster.tick_and_route(300).await;

        assert_all_decided(&cluster, 11);
        assert_logs_consistent(&cluster).await;
    });
}

// ===========================================================================
// Group: Replacement coverage for deleted test suites
// ===========================================================================

#[test]
fn test_reconnection_triggers_accept_sync() {
    smol::block_on(async {
        let mut cluster = SimCluster::new(vec![1, 2, 3]).await;
        let leader = elect_leader(&mut cluster).await;

        // Pick a follower to crash.
        let follower = *cluster.live_nodes().iter().find(|&&p| p != leader).unwrap();
        cluster.crash_node(follower);

        // Propose 10 entries, tick to decide on the 2 live nodes.
        for i in 1..=10 {
            cluster.propose(leader, Value::with_id(i)).await;
        }
        cluster.tick_and_route(200).await;

        // Verify the 2 live nodes decided all 10 entries.
        for node in cluster.nodes.values() {
            assert_eq!(node.get_decided_idx(), 10);
        }

        // Recover the follower (which calls reconnected()).
        cluster.recover_node(follower).await;

        // Tick to convergence.
        cluster.tick_and_route(300).await;

        // Assert the recovered follower has decided all 10 entries.
        assert_eq!(
            cluster.nodes[&follower].get_decided_idx(),
            10,
            "recovered follower should have decided all 10 entries"
        );

        // Assert all logs consistent.
        assert_logs_consistent(&cluster).await;
    });
}

#[test]
fn test_divergent_log_reconciliation() {
    smol::block_on(async {
        let mut cluster = SimCluster::new(vec![1, 2, 3, 4, 5]).await;
        let old_leader = elect_leader(&mut cluster).await;

        // Partition the leader from ALL followers.
        let followers: Vec<u64> = (1..=5).filter(|&p| p != old_leader).collect();
        let filter_handle = cluster.partition(vec![old_leader], followers.clone());

        // Have the old leader propose entries 1..=5 (won't decide because leader alone
        // isn't a quorum).
        for i in 1..=5 {
            let _ = cluster.try_propose(old_leader, Value::with_id(i)).await;
        }

        // Tick so a new leader is elected among the majority partition.
        let mut new_leader = None;
        for _ in 0..2000 {
            cluster.tick_all().await;
            cluster.route_messages().await;
            // Check if the four non-isolated nodes agree on a new leader.
            let mut counts: HashMap<u64, usize> = HashMap::new();
            for &pid in &followers {
                if let Some((lpid, _)) = cluster.nodes[&pid].get_current_leader() {
                    if lpid != old_leader {
                        *counts.entry(lpid).or_insert(0) += 1;
                    }
                }
            }
            for (&lpid, &count) in &counts {
                if count >= 3 {
                    new_leader = Some(lpid);
                }
            }
            if new_leader.is_some() {
                break;
            }
        }
        let new_leader = new_leader.expect("new leader should be elected in majority partition");
        assert_ne!(new_leader, old_leader);

        // Have the new leader propose entries 100..=105.
        for i in 100..=105 {
            cluster.propose(new_leader, Value::with_id(i)).await;
        }

        // Tick to decide in the majority partition.
        cluster.tick_and_route(300).await;

        // Heal the partition.
        cluster.remove_filter(filter_handle);
        for pid in 1..=5 {
            for other in 1..=5 {
                if pid != other {
                    cluster.nodes.get_mut(&pid).unwrap().reconnected(other);
                }
            }
        }

        // Tick to convergence.
        cluster.tick_and_route(500).await;

        // Assert all 5 nodes have consistent logs.
        cluster.assert_logs_consistent_and_converged().await;

        // Assert the final decided log contains entries 100..=105 from the new leader.
        // Note: entries 1..=5 from the old leader may or may not appear in the decided
        // log depending on whether the reconciliation leader discovers them via Promise.
        // This is correct Paxos behavior — what matters is that all nodes agree.
        let reference_node = cluster.nodes.values().next().unwrap();
        let decided_values = get_decided_values(reference_node);
        for expected in 100..=105 {
            assert!(
                decided_values.contains(&expected),
                "new leader's entry {} should be in the decided log, got {:?}",
                expected,
                decided_values
            );
        }
    });
}

#[test]
fn test_leader_crash_recovery_log_consistency() {
    smol::block_on(async {
        let mut cluster = SimCluster::new(vec![1, 2, 3, 4, 5]).await;
        let leader = elect_leader(&mut cluster).await;

        // Propose 10 entries, tick to decide on all nodes.
        for i in 1..=10 {
            cluster.propose(leader, Value::with_id(i)).await;
        }
        cluster.tick_and_route(200).await;
        assert_all_decided(&cluster, 10);

        // Crash the leader.
        cluster.crash_node(leader);

        // Elect a new leader among the remaining 4 nodes.
        let new_leader = elect_leader(&mut cluster).await;
        assert_ne!(new_leader, leader, "new leader should differ from crashed leader");

        // Propose 10 more entries to the new leader.
        for i in 11..=20 {
            cluster.propose(new_leader, Value::with_id(i)).await;
        }

        // Tick to decide on remaining nodes.
        cluster.tick_and_route(300).await;

        // Verify the 4 remaining nodes have decided all 20 entries.
        for node in cluster.nodes.values() {
            assert_eq!(
                node.get_decided_idx(),
                20,
                "remaining nodes should have decided all 20 entries"
            );
        }

        // Recover the crashed old leader.
        cluster.recover_node(leader).await;

        // Tick to convergence.
        cluster.tick_and_route(500).await;

        // Assert the recovered node has all 20 entries decided.
        assert_eq!(
            cluster.nodes[&leader].get_decided_idx(),
            20,
            "recovered old leader should have all 20 entries decided"
        );

        // Assert all logs consistent and converged.
        cluster.assert_logs_consistent_and_converged().await;
    });
}

#[test]
fn test_flexible_quorum_progress_with_failures() {
    smol::block_on(async {
        let cluster_config = ClusterConfig {
            configuration_id: 1,
            nodes: vec![1, 2, 3, 4, 5],
            flexible_quorum: Some(FlexibleQuorum {
                read_quorum_size: 4,
                write_quorum_size: 2,
            }),
        };
        let mut server_configs = BTreeMap::new();
        for pid in 1..=5 {
            server_configs.insert(
                pid,
                ServerConfig {
                    pid,
                    ..Default::default()
                },
            );
        }
        let mut cluster = SimCluster::with_configs(cluster_config, server_configs).await;
        let leader = elect_leader(&mut cluster).await;

        // Establish the leader in Accept phase by deciding initial entries.
        for i in 1..=5 {
            cluster.propose(leader, Value::with_id(i)).await;
        }
        cluster.tick_and_route(100).await;
        assert_all_decided(&cluster, 5);

        // Crash 3 followers (leaving leader + 1 follower = 2, which meets write_quorum_size).
        let followers: Vec<u64> = cluster
            .live_nodes()
            .into_iter()
            .filter(|&p| p != leader)
            .collect();
        let crashed: Vec<u64> = followers[..3].to_vec();
        for &f in &crashed {
            cluster.crash_node(f);
        }
        assert_eq!(cluster.live_nodes().len(), 2);

        // Propose 5 more entries.
        for i in 6..=10 {
            cluster.propose(leader, Value::with_id(i)).await;
        }

        // Tick to decide on the 2 live nodes.
        cluster.tick_and_route(200).await;

        // Assert leader's decided_idx == 10 (5 initial + 5 new).
        assert_eq!(
            cluster.nodes[&leader].get_decided_idx(),
            10,
            "leader should have decided_idx == 10 with write_quorum_size=2"
        );

        // Recover all crashed nodes, tick to convergence.
        for &f in &crashed {
            cluster.recover_node(f).await;
        }
        cluster.tick_and_route(500).await;

        // Assert all 5 nodes converged to decided_idx 10.
        for (&pid, node) in &cluster.nodes {
            assert_eq!(
                node.get_decided_idx(),
                10,
                "node {} should have converged to decided_idx 10",
                pid
            );
        }
        cluster.assert_logs_consistent_and_converged().await;
    });
}

#[test]
fn test_reconfig_stopsign_decided_and_append_rejected() {
    smol::block_on(async {
        let mut cluster = SimCluster::new(vec![1, 2, 3]).await;
        let leader = elect_leader(&mut cluster).await;

        // Propose 5 entries, tick to decide.
        for i in 1..=5 {
            cluster.propose(leader, Value::with_id(i)).await;
        }
        cluster.tick_and_route(200).await;
        assert_all_decided(&cluster, 5);

        // Call reconfigure with a new config (nodes: [1, 2, 3, 4]).
        let new_config = ClusterConfig {
            configuration_id: 2,
            nodes: vec![1, 2, 3, 4],
            ..Default::default()
        };
        cluster
            .nodes
            .get_mut(&leader)
            .unwrap()
            .reconfigure(new_config, None)
            .await
            .unwrap();

        // Tick until all nodes have is_reconfigured() returning Some(stopsign).
        cluster.tick_and_route(300).await;

        let mut stop_signs: Vec<StopSign> = Vec::new();
        for (&pid, node) in &cluster.nodes {
            let ss = node.is_reconfigured();
            assert!(
                ss.is_some(),
                "node {} should have is_reconfigured() returning Some",
                pid
            );
            stop_signs.push(ss.unwrap());
        }

        // Assert all nodes have the same StopSign.
        for ss in &stop_signs[1..] {
            assert_eq!(
                stop_signs[0], *ss,
                "all nodes must agree on the same StopSign"
            );
        }

        // Try appending another entry -- it should return Err(ProposeErr::PendingReconfigEntry(_)).
        let result = cluster.try_propose(leader, Value::with_id(99)).await;
        assert!(
            matches!(result, Err(ProposeErr::PendingReconfigEntry(_))),
            "appending after reconfiguration should return PendingReconfigEntry, got {:?}",
            result
        );
    });
}

#[test]
fn test_message_duplication_safety() {
    smol::block_on(async {
        let mut cluster = SimCluster::new(vec![1, 2, 3, 4, 5]).await;
        // Set duplication rate to 0.3.
        cluster.duplication_rng = Some((SimpleRng::new(test_seed(55)), 0.3));

        let leader = elect_leader(&mut cluster).await;

        // Propose 20 entries across 5 iterations of tick_and_route.
        for batch in 0..5 {
            for j in 0..4 {
                let id = (batch * 4 + j + 1) as u64;
                cluster.propose(leader, Value::with_id(id)).await;
            }
            cluster.tick_and_route(10).await;
        }

        // Tick to convergence (500 rounds).
        cluster.tick_and_route(500).await;

        // Assert logs consistent and converged.
        cluster.assert_logs_consistent_and_converged().await;

        // Verify all 20 entries decided.
        let decided_idx = cluster.nodes[&leader].get_decided_idx();
        assert_eq!(
            decided_idx, 20,
            "all 20 entries should be decided, got {}",
            decided_idx
        );
    });
}

#[test]
fn test_proposal_ordering_preserved_per_proposer() {
    smol::block_on(async {
        let mut cluster = SimCluster::new(vec![1, 2, 3, 4, 5]).await;
        let _leader = elect_leader(&mut cluster).await;
        // Extra rounds to ensure leader is in Accept phase.
        cluster.tick_and_route(20).await;

        // Each of the 5 nodes proposes 5 entries with unique IDs.
        // Node 1: ids 10, 20, 30, 40, 50
        // Node 2: ids 11, 21, 31, 41, 51
        // Node 3: ids 12, 22, 32, 42, 52
        // Node 4: ids 13, 23, 33, 43, 53
        // Node 5: ids 14, 24, 34, 44, 54
        let proposals: Vec<(u64, Vec<u64>)> = vec![
            (1, vec![10, 20, 30, 40, 50]),
            (2, vec![11, 21, 31, 41, 51]),
            (3, vec![12, 22, 32, 42, 52]),
            (4, vec![13, 23, 33, 43, 53]),
            (5, vec![14, 24, 34, 44, 54]),
        ];

        // Interleave: propose one entry from each node per round, then tick_and_route.
        for round in 0..5 {
            for &(node_id, ref ids) in &proposals {
                let _ = cluster
                    .try_propose(node_id, Value::with_id(ids[round]))
                    .await;
            }
            cluster.tick_and_route(10).await;
        }

        // Tick to convergence.
        cluster.tick_and_route(500).await;

        // All 25 entries should be decided.
        let decided_idx = cluster.nodes.values().next().unwrap().get_decided_idx();
        assert_eq!(decided_idx, 25, "all 25 entries should be decided");

        // For each node's decided log, verify that entries from the same proposer
        // appear in the correct relative order.
        for (&pid, node) in &cluster.nodes {
            let values = get_decided_values(node);

            for &(proposer_id, ref expected_order) in &proposals {
                // Extract the subsequence of entries from this proposer
                let actual_order: Vec<u64> = values
                    .iter()
                    .copied()
                    .filter(|id| expected_order.contains(id))
                    .collect();
                assert_eq!(
                    actual_order, *expected_order,
                    "node {}: entries from proposer {} are reordered: got {:?}, expected {:?}",
                    pid, proposer_id, actual_order, expected_order
                );
            }
        }
    });
}

// ===========================================================================
// Reconfiguration safety tests
// ===========================================================================

#[test]
fn test_reconfigure_config_id_monotonicity() {
    smol::block_on(async {
        let mut cluster = SimCluster::new(vec![1, 2, 3]).await;
        let leader = elect_leader(&mut cluster).await;

        // Attempt reconfig with same config_id as current (1) — should fail
        let same_config = ClusterConfig {
            configuration_id: 1,
            nodes: vec![1, 2, 3, 4],
            ..Default::default()
        };
        let result = cluster
            .nodes
            .get_mut(&leader)
            .unwrap()
            .reconfigure(same_config, None)
            .await;
        assert!(
            matches!(result, Err(ProposeErr::ConfigError(..))),
            "reconfigure with same config_id should be rejected, got {:?}",
            result
        );

        // Successful reconfig with config_id=2
        let good_config = ClusterConfig {
            configuration_id: 2,
            nodes: vec![1, 2, 3, 4],
            ..Default::default()
        };
        let result = cluster
            .nodes
            .get_mut(&leader)
            .unwrap()
            .reconfigure(good_config, None)
            .await;
        assert!(
            result.is_ok(),
            "reconfigure with higher config_id should succeed"
        );
    });
}

#[test]
fn test_reads_blocked_during_reconfiguration() {
    smol::block_on(async {
        let mut cluster = SimCluster::new(vec![1, 2, 3]).await;
        let leader = elect_leader(&mut cluster).await;

        // Propose some entries so leader is in Accept phase with decided entries
        for i in 1..=3 {
            cluster.propose(leader, Value::with_id(i)).await;
        }
        cluster.tick_and_route(100).await;

        // Reads should work before reconfiguration
        let leader_node = cluster.nodes.get_mut(&leader).unwrap();
        let read_result = leader_node.ensure_linearizable();
        assert!(
            read_result.is_ok(),
            "reads should work before reconfiguration"
        );

        // Accept reconfiguration
        let new_config = ClusterConfig {
            configuration_id: 2,
            nodes: vec![1, 2, 3, 4],
            ..Default::default()
        };
        leader_node.reconfigure(new_config, None).await.unwrap();

        // Now reads should be blocked
        let read_result = leader_node.ensure_linearizable();
        assert!(
            matches!(read_result, Err(ReadError::Reconfigured)),
            "reads should be blocked during reconfiguration, got {:?}",
            read_result
        );
    });
}

#[test]
fn test_reconfigure_during_network_partition() {
    smol::block_on(async {
        let mut cluster = SimCluster::new(vec![1, 2, 3]).await;
        let leader = elect_leader(&mut cluster).await;

        // Propose some entries first
        for i in 1..=5 {
            cluster.propose(leader, Value::with_id(i)).await;
        }
        cluster.tick_and_route(100).await;

        // Partition: isolate one follower
        let followers: Vec<u64> = cluster
            .nodes
            .keys()
            .copied()
            .filter(|&pid| pid != leader)
            .collect();
        let isolated = followers[0];
        let filter_handle = cluster.isolate_node(isolated);

        // Propose reconfiguration while partitioned
        let new_config = ClusterConfig {
            configuration_id: 2,
            nodes: vec![1, 2, 3, 4],
            ..Default::default()
        };
        cluster
            .nodes
            .get_mut(&leader)
            .unwrap()
            .reconfigure(new_config, None)
            .await
            .unwrap();

        // Tick while partitioned — leader + one follower should form quorum
        cluster.tick_and_route(200).await;

        // Heal partition
        cluster.remove_filter(filter_handle);
        cluster.tick_and_route(500).await;

        // All nodes should agree on the reconfiguration
        for (&pid, node) in &cluster.nodes {
            assert!(
                node.is_reconfigured().is_some(),
                "node {} should see reconfiguration as decided",
                pid
            );
        }
    });
}

#[test]
fn test_chain_reconfiguration_sealed() {
    smol::block_on(async {
        let mut cluster = SimCluster::new(vec![1, 2, 3]).await;
        let leader = elect_leader(&mut cluster).await;

        // Reconfig 1 -> 2
        let config2 = ClusterConfig {
            configuration_id: 2,
            nodes: vec![1, 2, 3],
            ..Default::default()
        };
        cluster
            .nodes
            .get_mut(&leader)
            .unwrap()
            .reconfigure(config2, None)
            .await
            .unwrap();
        cluster.tick_and_route(200).await;

        // Verify decided
        for node in cluster.nodes.values() {
            let ss = node.is_reconfigured();
            assert!(ss.is_some(), "reconfiguration 1->2 should be decided");
            assert_eq!(ss.unwrap().next_config.configuration_id, 2);
        }

        // Attempting further reconfiguration on the sealed cluster should fail
        let config3 = ClusterConfig {
            configuration_id: 3,
            nodes: vec![1, 2, 3, 4],
            ..Default::default()
        };
        let result = cluster
            .nodes
            .get_mut(&leader)
            .unwrap()
            .reconfigure(config3, None)
            .await;
        assert!(
            matches!(result, Err(ProposeErr::PendingReconfigConfig(..))),
            "reconfiguration on sealed cluster should fail, got {:?}",
            result
        );
    });
}

#[test]
fn test_follower_initiated_reconfiguration() {
    smol::block_on(async {
        let mut cluster = SimCluster::new(vec![1, 2, 3]).await;
        let leader = elect_leader(&mut cluster).await;
        let follower = cluster
            .nodes
            .keys()
            .copied()
            .find(|&pid| pid != leader)
            .unwrap();

        // Follower initiates reconfiguration
        let new_config = ClusterConfig {
            configuration_id: 2,
            nodes: vec![1, 2, 3, 4],
            ..Default::default()
        };
        cluster
            .nodes
            .get_mut(&follower)
            .unwrap()
            .reconfigure(new_config, None)
            .await
            .unwrap();

        // Let the forwarded StopSign propagate and get decided
        cluster.tick_and_route(500).await;

        // All nodes should agree on the reconfiguration
        for (&pid, node) in &cluster.nodes {
            assert!(
                node.is_reconfigured().is_some(),
                "node {} should see reconfiguration as decided after follower-initiated reconfig",
                pid
            );
        }
        let ss = cluster
            .nodes
            .values()
            .next()
            .unwrap()
            .is_reconfigured()
            .unwrap();
        assert_eq!(ss.next_config.nodes, vec![1, 2, 3, 4]);
    });
}

#[test]
fn test_reconfiguration_status_lifecycle() {
    smol::block_on(async {
        let mut cluster = SimCluster::new(vec![1, 2, 3]).await;
        let leader = elect_leader(&mut cluster).await;

        // Propose entries to ensure leader is in Accept phase
        for i in 1..=3 {
            cluster.propose(leader, Value::with_id(i)).await;
        }
        cluster.tick_and_route(100).await;

        // Initially: no reconfiguration
        let status = cluster
            .nodes
            .get(&leader)
            .unwrap()
            .reconfiguration_status();
        assert!(
            matches!(status, ReconfigurationStatus::Idle),
            "initial status should be Idle, got {:?}",
            status
        );

        // Isolate followers so leader can accept but not reach quorum for decision
        let followers: Vec<u64> = cluster
            .nodes
            .keys()
            .copied()
            .filter(|&pid| pid != leader)
            .collect();
        let filter_handle = cluster.partition(vec![leader], followers.clone());

        // Propose reconfiguration (leader accepts locally but can't reach quorum)
        let new_config = ClusterConfig {
            configuration_id: 2,
            nodes: vec![1, 2, 3, 4],
            ..Default::default()
        };
        cluster
            .nodes
            .get_mut(&leader)
            .unwrap()
            .reconfigure(new_config, None)
            .await
            .unwrap();

        // Status should be Pending on leader
        let status = cluster
            .nodes
            .get(&leader)
            .unwrap()
            .reconfiguration_status();
        assert!(
            matches!(status, ReconfigurationStatus::Pending(_)),
            "status should be Pending after accepting StopSign, got {:?}",
            status
        );

        // Heal partition and let it decide
        cluster.remove_filter(filter_handle);
        cluster.tick_and_route(500).await;

        // Status should be Decided on leader
        let status = cluster
            .nodes
            .get(&leader)
            .unwrap()
            .reconfiguration_status();
        assert!(
            matches!(status, ReconfigurationStatus::Decided(_)),
            "status should be Decided after quorum, got {:?}",
            status
        );
    });
}
