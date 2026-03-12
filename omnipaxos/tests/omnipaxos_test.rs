use omnipaxos::{
    messages::Message,
    storage::{memory_storage::MemoryStorage, Entry, Snapshot, Storage},
    util::LogEntry,
    ClusterConfig, OmniPaxos, OmniPaxosConfig, ServerConfig,
};
use std::collections::HashMap;

// ---------------------------------------------------------------------------
// Entry + Snapshot types
// ---------------------------------------------------------------------------

#[derive(Clone, Debug, PartialEq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
struct Value {
    id: u64,
}

impl Value {
    fn with_id(id: u64) -> Self {
        Value { id }
    }
}

#[derive(Clone, Debug, PartialEq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
struct ValueSnapshot {
    map: HashMap<u64, u64>,
}

impl Snapshot<Value> for ValueSnapshot {
    fn create(entries: &[Value]) -> Self {
        let mut map = HashMap::new();
        for e in entries {
            map.insert(e.id, e.id);
        }
        ValueSnapshot { map }
    }

    fn merge(&mut self, delta: Self) {
        self.map.extend(delta.map);
    }

    fn use_snapshots() -> bool {
        true
    }
}

impl Entry for Value {
    type Snapshot = ValueSnapshot;
}

// ---------------------------------------------------------------------------
// TestCluster
// ---------------------------------------------------------------------------

struct TestCluster {
    nodes: HashMap<u64, OmniPaxos<Value, MemoryStorage<Value>>>,
}

impl TestCluster {
    /// Creates a cluster with the given (possibly non-contiguous) node IDs.
    async fn new(node_ids: Vec<u64>) -> Self {
        let mut nodes = HashMap::new();
        for &pid in &node_ids {
            let cluster_config = ClusterConfig {
                configuration_id: 1,
                nodes: node_ids.clone(),
                ..Default::default()
            };
            let server_config = ServerConfig {
                pid,
                ..Default::default()
            };
            let config = OmniPaxosConfig {
                cluster_config,
                server_config,
            };
            let storage = MemoryStorage::default();
            let op = config.build(storage).await.unwrap();
            nodes.insert(pid, op);
        }
        TestCluster { nodes }
    }

    /// Tick every node once.
    async fn tick_all(&mut self) {
        for node in self.nodes.values_mut() {
            node.tick().await.unwrap();
        }
    }

    /// Collect outgoing messages from all nodes and deliver them to their recipients.
    async fn route_messages(&mut self) {
        let mut all_msgs: Vec<Message<Value>> = Vec::new();
        let mut buf = Vec::new();
        for node in self.nodes.values_mut() {
            node.take_outgoing_messages(&mut buf);
            all_msgs.append(&mut buf);
        }
        for msg in all_msgs {
            let receiver = msg.get_receiver();
            if let Some(node) = self.nodes.get_mut(&receiver) {
                node.handle_incoming(msg).await.unwrap();
            }
        }
    }

    /// Run tick + route for `rounds` iterations.
    async fn tick_and_route(&mut self, rounds: usize) {
        for _ in 0..rounds {
            self.tick_all().await;
            self.route_messages().await;
        }
    }

    /// Returns the leader PID if a majority of nodes agree on the same leader.
    fn get_elected_leader(&self) -> Option<u64> {
        let mut leader_counts: HashMap<u64, usize> = HashMap::new();
        for node in self.nodes.values() {
            if let Some((leader_pid, _)) = node.get_current_leader() {
                *leader_counts.entry(leader_pid).or_insert(0) += 1;
            }
        }
        let majority = self.nodes.len() / 2 + 1;
        for (&leader, &count) in &leader_counts {
            if count >= majority {
                return Some(leader);
            }
        }
        None
    }

    /// Propose an entry through a specific node.
    async fn propose(&mut self, node_id: u64, entry: Value) {
        let node = self.nodes.get_mut(&node_id).expect("node not found");
        node.append(entry).await.unwrap();
    }
}

// ---------------------------------------------------------------------------
// Helper: tick until leader elected
// ---------------------------------------------------------------------------

async fn elect_leader(cluster: &mut TestCluster) -> u64 {
    // Tick enough rounds for BLE to converge
    for _ in 0..100 {
        cluster.tick_all().await;
        cluster.route_messages().await;
        if let Some(leader) = cluster.get_elected_leader() {
            return leader;
        }
    }
    panic!("Leader was not elected within 100 rounds");
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[test]
fn test_leader_election() {
    smol::block_on(async {
        let mut cluster = TestCluster::new(vec![1, 2, 3]).await;
        let leader = elect_leader(&mut cluster).await;

        // Every node should agree on the leader.
        for node in cluster.nodes.values() {
            let (pid, _) = node.get_current_leader().expect("node has no leader");
            assert_eq!(pid, leader, "all nodes must agree on the leader");
        }
    });
}

#[test]
fn test_basic_replication() {
    smol::block_on(async {
        let mut cluster = TestCluster::new(vec![1, 2, 3]).await;
        let leader = elect_leader(&mut cluster).await;

        // Propose entries through the leader.
        let num_entries = 5;
        for i in 1..=num_entries {
            cluster.propose(leader, Value::with_id(i)).await;
        }

        // Tick/route enough rounds for entries to be decided.
        cluster.tick_and_route(100).await;

        // Verify all nodes have the same decided log with the expected entries.
        for (&pid, node) in &cluster.nodes {
            let decided_idx = node.get_decided_idx();
            assert_eq!(
                decided_idx, num_entries as usize,
                "node {} should have decided {} entries, got {}",
                pid, num_entries, decided_idx
            );
        }

        // Read decided entries from each node and verify they match.
        let mut logs: Vec<Vec<u64>> = Vec::new();
        for node in cluster.nodes.values() {
            let entries = node
                .read_decided_suffix(0)
                .await
                .unwrap()
                .expect("should have decided entries");
            let ids: Vec<u64> = entries
                .iter()
                .filter_map(|e| match e {
                    LogEntry::Decided(v) => Some(v.id),
                    _ => None,
                })
                .collect();
            assert_eq!(ids.len(), num_entries as usize, "all entries should be decided");
            logs.push(ids);
        }

        // All logs should be identical.
        for log in &logs {
            assert_eq!(log, &logs[0], "all nodes must have the same decided log");
        }
    });
}

#[test]
fn test_follower_forwarding() {
    smol::block_on(async {
        let mut cluster = TestCluster::new(vec![1, 2, 3]).await;
        let leader = elect_leader(&mut cluster).await;

        // Pick a follower.
        let follower = cluster
            .nodes
            .keys()
            .copied()
            .find(|&pid| pid != leader)
            .expect("no follower found");

        // Propose entries through the follower.
        let num_entries: u64 = 5;
        for i in 1..=num_entries {
            cluster.propose(follower, Value::with_id(i)).await;
        }

        // Tick/route enough rounds for forwarding + decision.
        cluster.tick_and_route(100).await;

        // All nodes should have the entries decided.
        for (&pid, node) in &cluster.nodes {
            let decided_idx = node.get_decided_idx();
            assert_eq!(
                decided_idx, num_entries as usize,
                "node {} should have decided {} entries, got {}",
                pid, num_entries, decided_idx
            );
        }

        // Verify content.
        let mut logs: Vec<Vec<u64>> = Vec::new();
        for node in cluster.nodes.values() {
            let entries = node
                .read_decided_suffix(0)
                .await
                .unwrap()
                .expect("should have decided entries");
            let ids: Vec<u64> = entries
                .iter()
                .filter_map(|e| match e {
                    LogEntry::Decided(v) => Some(v.id),
                    _ => None,
                })
                .collect();
            logs.push(ids);
        }
        for log in &logs {
            assert_eq!(log, &logs[0], "all nodes must have the same decided log");
        }
    });
}

#[test]
fn test_non_contiguous_pids() {
    smol::block_on(async {
        let mut cluster = TestCluster::new(vec![3, 7, 100]).await;
        let leader = elect_leader(&mut cluster).await;

        // Verify leader is one of our PIDs.
        assert!(
            [3u64, 7, 100].contains(&leader),
            "leader must be one of the cluster PIDs"
        );

        // Propose entries and verify replication.
        let num_entries: u64 = 3;
        for i in 1..=num_entries {
            cluster.propose(leader, Value::with_id(i)).await;
        }
        cluster.tick_and_route(100).await;

        for (&pid, node) in &cluster.nodes {
            let decided_idx = node.get_decided_idx();
            assert_eq!(
                decided_idx, num_entries as usize,
                "node {} should have decided {} entries, got {}",
                pid, num_entries, decided_idx
            );
        }
    });
}

#[test]
fn test_trim() {
    smol::block_on(async {
        let mut cluster = TestCluster::new(vec![1, 2, 3]).await;
        let leader = elect_leader(&mut cluster).await;

        // Propose entries.
        let num_entries: u64 = 10;
        for i in 1..=num_entries {
            cluster.propose(leader, Value::with_id(i)).await;
        }
        cluster.tick_and_route(100).await;

        // Verify all decided.
        for node in cluster.nodes.values() {
            assert_eq!(node.get_decided_idx(), num_entries as usize);
        }

        // Trim: must be called on the leader and only up to the min decided idx of all nodes.
        // Since all nodes have decided all entries, trim should succeed.
        let trim_idx = 5;
        {
            let leader_node = cluster.nodes.get_mut(&leader).unwrap();
            leader_node
                .trim(Some(trim_idx))
                .await
                .expect("trim should succeed");
        }

        // Route the trim compaction message so all nodes trim.
        cluster.tick_and_route(100).await;

        // Verify compacted index on the leader.
        let leader_node = cluster.nodes.get(&leader).unwrap();
        assert!(
            leader_node.get_compacted_idx() >= trim_idx,
            "leader compacted_idx should be >= trim_idx"
        );

        // Reading a trimmed index should return a Trimmed entry.
        let entry = leader_node.read(1).await.unwrap();
        match entry {
            Some(LogEntry::Trimmed(_)) => {} // expected
            other => panic!(
                "expected Trimmed entry at index 1, got {:?}",
                other
            ),
        }
    });
}

#[test]
fn test_snapshot() {
    smol::block_on(async {
        let mut cluster = TestCluster::new(vec![1, 2, 3]).await;
        let leader = elect_leader(&mut cluster).await;

        // Propose entries.
        let num_entries: u64 = 10;
        for i in 1..=num_entries {
            cluster.propose(leader, Value::with_id(i)).await;
        }
        cluster.tick_and_route(100).await;

        // Verify all decided.
        for node in cluster.nodes.values() {
            assert_eq!(node.get_decided_idx(), num_entries as usize);
        }

        // Snapshot the first 5 entries on all nodes.
        let snapshot_idx = 5;
        {
            let leader_node = cluster.nodes.get_mut(&leader).unwrap();
            leader_node
                .snapshot(Some(snapshot_idx), false)
                .await
                .expect("snapshot should succeed");
        }
        cluster.tick_and_route(100).await;

        // Verify compacted index on the leader.
        let leader_node = cluster.nodes.get(&leader).unwrap();
        assert!(
            leader_node.get_compacted_idx() >= snapshot_idx,
            "leader compacted_idx should be >= snapshot_idx"
        );

        // Reading a snapshotted entry should return a Snapshotted variant.
        let entry = leader_node.read(1).await.unwrap();
        match entry {
            Some(LogEntry::Snapshotted(s)) => {
                // The snapshot should contain the IDs of the snapshotted entries.
                assert!(
                    !s.snapshot.map.is_empty(),
                    "snapshot map should not be empty"
                );
            }
            other => panic!(
                "expected Snapshotted entry at index 1, got {:?}",
                other
            ),
        }
    });
}

// ---------------------------------------------------------------------------
// Single-node cluster tests
// ---------------------------------------------------------------------------

#[test]
fn test_single_node_leader_election() {
    smol::block_on(async {
        let mut cluster = TestCluster::new(vec![1]).await;
        // A single node should elect itself as leader within a few ticks.
        let leader = elect_leader(&mut cluster).await;
        assert_eq!(leader, 1, "the only node must be the leader");
    });
}

#[test]
fn test_single_node_replication() {
    smol::block_on(async {
        let mut cluster = TestCluster::new(vec![1]).await;
        let leader = elect_leader(&mut cluster).await;
        assert_eq!(leader, 1);

        // Propose entries and verify they are decided immediately.
        let num_entries: u64 = 5;
        for i in 1..=num_entries {
            cluster.propose(leader, Value::with_id(i)).await;
        }
        cluster.tick_and_route(10).await;

        let node = cluster.nodes.get(&1).unwrap();
        assert_eq!(
            node.get_decided_idx(),
            num_entries as usize,
            "single node should decide all entries"
        );

        let entries = node
            .read_decided_suffix(0)
            .await
            .unwrap()
            .expect("should have decided entries");
        let ids: Vec<u64> = entries
            .iter()
            .filter_map(|e| match e {
                LogEntry::Decided(v) => Some(v.id),
                _ => None,
            })
            .collect();
        assert_eq!(ids, vec![1, 2, 3, 4, 5]);
    });
}

#[test]
fn test_single_node_reconfigure_to_three() {
    smol::block_on(async {
        let mut cluster = TestCluster::new(vec![1]).await;
        let leader = elect_leader(&mut cluster).await;
        assert_eq!(leader, 1);

        // Propose some entries first.
        for i in 1..=3 {
            cluster.propose(leader, Value::with_id(i)).await;
        }
        cluster.tick_and_route(10).await;

        // Reconfigure from 1 node to 3 nodes.
        let new_config = ClusterConfig {
            configuration_id: 2,
            nodes: vec![1, 2, 3],
            ..Default::default()
        };
        let node = cluster.nodes.get_mut(&leader).unwrap();
        node.reconfigure(new_config, None).await.unwrap();

        // Tick to let the stopsign be decided.
        cluster.tick_and_route(10).await;

        let node = cluster.nodes.get(&leader).unwrap();
        let ss = node.is_reconfigured();
        assert!(
            ss.is_some(),
            "single node should have decided the stopsign for reconfiguration"
        );
        let ss = ss.unwrap();
        assert_eq!(ss.next_config.nodes, vec![1, 2, 3]);
    });
}

#[test]
fn test_single_node_recovery() {
    smol::block_on(async {
        // Simulate recovery: create a MemoryStorage with a stored promise
        // (as if the node had been a leader before crashing).
        use omnipaxos::ballot_leader_election::Ballot;

        let mut storage: MemoryStorage<Value> = MemoryStorage::default();
        let ballot = Ballot {
            config_id: 1,
            n: 1,
            priority: 0,
            pid: 1,
        };
        storage.set_promise(ballot).await.unwrap();
        storage.set_accepted_round(ballot).await.unwrap();
        // Write some entries that were accepted before "crash".
        storage
            .append_entries(vec![
                Value::with_id(1),
                Value::with_id(2),
                Value::with_id(3),
            ])
            .await
            .unwrap();
        storage.set_decided_idx(3).await.unwrap();

        let cluster_config = ClusterConfig {
            configuration_id: 1,
            nodes: vec![1],
            ..Default::default()
        };
        let server_config = ServerConfig {
            pid: 1,
            ..Default::default()
        };
        let config = OmniPaxosConfig {
            cluster_config,
            server_config,
        };
        let mut recovered = config.build(storage).await.unwrap();

        // The recovered node should not be stuck in Recover phase.
        // Tick enough for BLE to elect it leader again.
        for _ in 0..50 {
            recovered.tick().await.unwrap();
        }

        // Verify it can accept new entries.
        recovered.append(Value::with_id(10)).await.unwrap();
        for _ in 0..10 {
            recovered.tick().await.unwrap();
        }

        assert_eq!(
            recovered.get_decided_idx(),
            4,
            "recovered single node should decide new entry"
        );
    });
}
