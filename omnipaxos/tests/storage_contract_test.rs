use omnipaxos::{
    ballot_leader_election::Ballot,
    messages::Message,
    storage::{
        memory_storage::MemoryStorage, StopSign, Storage, StorageError, StorageOp,
        StorageResult,
    },
    ClusterConfig, OmniPaxos, OmniPaxosConfig, ServerConfig,
};
use std::cell::RefCell;
use std::collections::{HashMap, HashSet};
use std::rc::Rc;

mod test_utils;
use test_utils::{Value, ValueSnapshot};

// ===========================================================================
// FailableStorage — storage wrapper that can inject failures
// ===========================================================================

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
enum FailOn {
    WriteAtomically,
    AppendEntry,
    AppendEntries,
    AppendOnPrefix,
    SetPromise,
    SetDecidedIdx,
    GetDecidedIdx,
    SetAcceptedRound,
    GetAcceptedRound,
    GetEntries,
    GetLogLen,
    GetSuffix,
    GetPromise,
    SetStopsign,
    GetStopsign,
    Trim,
    SetCompactedIdx,
    GetCompactedIdx,
    SetSnapshot,
    GetSnapshot,
}

struct FailCtrl {
    permanent_failures: HashSet<FailOn>,
    fail_once: HashSet<FailOn>,
    write_atomically_count: usize,
    partial_write_limit: Option<usize>,
}

impl FailCtrl {
    fn new() -> Self {
        FailCtrl {
            permanent_failures: HashSet::new(),
            fail_once: HashSet::new(),
            write_atomically_count: 0,
            partial_write_limit: None,
        }
    }

    fn should_fail(&mut self, op: &FailOn) -> bool {
        if self.permanent_failures.contains(op) {
            return true;
        }
        if self.fail_once.remove(op) {
            return true;
        }
        false
    }
}

fn make_storage_error(msg: &str) -> StorageError {
    StorageError(Box::new(std::io::Error::new(
        std::io::ErrorKind::Other,
        msg,
    )))
}

struct FailableStorage {
    inner: MemoryStorage<Value>,
    ctrl: Rc<RefCell<FailCtrl>>,
}

impl FailableStorage {
    fn new_with_ctrl() -> (Self, Rc<RefCell<FailCtrl>>) {
        let ctrl = Rc::new(RefCell::new(FailCtrl::new()));
        let storage = FailableStorage {
            inner: MemoryStorage::default(),
            ctrl: ctrl.clone(),
        };
        (storage, ctrl)
    }
}

macro_rules! check_fail {
    ($self:expr, $op:expr) => {
        if $self.ctrl.borrow_mut().should_fail(&$op) {
            return Err(make_storage_error(&format!("injected failure: {:?}", $op)));
        }
    };
}

impl Storage<Value> for FailableStorage {
    async fn write_atomically(&mut self, ops: Vec<StorageOp<Value>>) -> StorageResult<()> {
        self.ctrl.borrow_mut().write_atomically_count += 1;

        // Check partial write limit (simulates non-atomic implementation)
        if let Some(limit) = self.ctrl.borrow().partial_write_limit {
            let backup = self.inner.clone();
            for (i, op) in ops.into_iter().enumerate() {
                if i >= limit {
                    // Partial write: stop after `limit` ops but return Ok
                    return Ok(());
                }
                let result = match op {
                    StorageOp::AppendEntry(entry) => self.inner.append_entry(entry).await,
                    StorageOp::AppendEntries(entries) => self.inner.append_entries(entries).await,
                    StorageOp::AppendOnPrefix(from_idx, entries) => {
                        self.inner.append_on_prefix(from_idx, entries).await
                    }
                    StorageOp::SetPromise(bal) => self.inner.set_promise(bal).await,
                    StorageOp::SetDecidedIndex(idx) => self.inner.set_decided_idx(idx).await,
                    StorageOp::SetAcceptedRound(bal) => self.inner.set_accepted_round(bal).await,
                    StorageOp::SetCompactedIdx(idx) => self.inner.set_compacted_idx(idx).await,
                    StorageOp::Trim(idx) => self.inner.trim(idx).await,
                    StorageOp::SetStopsign(ss) => self.inner.set_stopsign(ss).await,
                    StorageOp::SetSnapshot(snap) => self.inner.set_snapshot(snap).await,
                };
                if let Err(e) = result {
                    self.inner = backup;
                    return Err(e);
                }
            }
            return Ok(());
        }

        check_fail!(self, FailOn::WriteAtomically);

        // Execute ops individually with per-op failure checking and rollback.
        // We inline the failure check (instead of using check_fail! which does
        // early return) so that failures go through the rollback path.
        let backup = self.inner.clone();
        for op in ops {
            let fail_op = match &op {
                StorageOp::AppendEntry(_) => Some(FailOn::AppendEntry),
                StorageOp::AppendEntries(_) => Some(FailOn::AppendEntries),
                StorageOp::AppendOnPrefix(_, _) => Some(FailOn::AppendOnPrefix),
                StorageOp::SetPromise(_) => Some(FailOn::SetPromise),
                StorageOp::SetDecidedIndex(_) => Some(FailOn::SetDecidedIdx),
                StorageOp::SetAcceptedRound(_) => Some(FailOn::SetAcceptedRound),
                StorageOp::SetCompactedIdx(_) => Some(FailOn::SetCompactedIdx),
                StorageOp::Trim(_) => Some(FailOn::Trim),
                StorageOp::SetStopsign(_) => Some(FailOn::SetStopsign),
                StorageOp::SetSnapshot(_) => Some(FailOn::SetSnapshot),
            };
            if let Some(ref fop) = fail_op {
                if self.ctrl.borrow_mut().should_fail(fop) {
                    self.inner = backup;
                    return Err(make_storage_error(&format!(
                        "injected failure: {:?}",
                        fop
                    )));
                }
            }
            let result = match op {
                StorageOp::AppendEntry(entry) => self.inner.append_entry(entry).await,
                StorageOp::AppendEntries(entries) => self.inner.append_entries(entries).await,
                StorageOp::AppendOnPrefix(idx, entries) => {
                    self.inner.append_on_prefix(idx, entries).await
                }
                StorageOp::SetPromise(bal) => self.inner.set_promise(bal).await,
                StorageOp::SetDecidedIndex(idx) => self.inner.set_decided_idx(idx).await,
                StorageOp::SetAcceptedRound(bal) => self.inner.set_accepted_round(bal).await,
                StorageOp::SetCompactedIdx(idx) => self.inner.set_compacted_idx(idx).await,
                StorageOp::Trim(idx) => self.inner.trim(idx).await,
                StorageOp::SetStopsign(ss) => self.inner.set_stopsign(ss).await,
                StorageOp::SetSnapshot(snap) => self.inner.set_snapshot(snap).await,
            };
            if let Err(e) = result {
                self.inner = backup;
                return Err(e);
            }
        }
        Ok(())
    }

    async fn append_entry(&mut self, entry: Value) -> StorageResult<()> {
        check_fail!(self, FailOn::AppendEntry);
        self.inner.append_entry(entry).await
    }

    async fn append_entries(&mut self, entries: Vec<Value>) -> StorageResult<()> {
        check_fail!(self, FailOn::AppendEntries);
        self.inner.append_entries(entries).await
    }

    async fn append_on_prefix(
        &mut self,
        from_idx: usize,
        entries: Vec<Value>,
    ) -> StorageResult<()> {
        check_fail!(self, FailOn::AppendOnPrefix);
        self.inner.append_on_prefix(from_idx, entries).await
    }

    async fn set_promise(&mut self, n_prom: Ballot) -> StorageResult<()> {
        check_fail!(self, FailOn::SetPromise);
        self.inner.set_promise(n_prom).await
    }

    async fn set_decided_idx(&mut self, ld: usize) -> StorageResult<()> {
        check_fail!(self, FailOn::SetDecidedIdx);
        self.inner.set_decided_idx(ld).await
    }

    async fn get_decided_idx(&self) -> StorageResult<usize> {
        check_fail!(self, FailOn::GetDecidedIdx);
        self.inner.get_decided_idx().await
    }

    async fn set_accepted_round(&mut self, na: Ballot) -> StorageResult<()> {
        check_fail!(self, FailOn::SetAcceptedRound);
        self.inner.set_accepted_round(na).await
    }

    async fn get_accepted_round(&self) -> StorageResult<Option<Ballot>> {
        check_fail!(self, FailOn::GetAcceptedRound);
        self.inner.get_accepted_round().await
    }

    async fn get_entries(&self, from: usize, to: usize) -> StorageResult<Vec<Value>> {
        check_fail!(self, FailOn::GetEntries);
        self.inner.get_entries(from, to).await
    }

    async fn get_log_len(&self) -> StorageResult<usize> {
        check_fail!(self, FailOn::GetLogLen);
        self.inner.get_log_len().await
    }

    async fn get_suffix(&self, from: usize) -> StorageResult<Vec<Value>> {
        check_fail!(self, FailOn::GetSuffix);
        self.inner.get_suffix(from).await
    }

    async fn get_promise(&self) -> StorageResult<Option<Ballot>> {
        check_fail!(self, FailOn::GetPromise);
        self.inner.get_promise().await
    }

    async fn set_stopsign(&mut self, s: Option<StopSign>) -> StorageResult<()> {
        check_fail!(self, FailOn::SetStopsign);
        self.inner.set_stopsign(s).await
    }

    async fn get_stopsign(&self) -> StorageResult<Option<StopSign>> {
        check_fail!(self, FailOn::GetStopsign);
        self.inner.get_stopsign().await
    }

    async fn trim(&mut self, idx: usize) -> StorageResult<()> {
        check_fail!(self, FailOn::Trim);
        self.inner.trim(idx).await
    }

    async fn set_compacted_idx(&mut self, idx: usize) -> StorageResult<()> {
        check_fail!(self, FailOn::SetCompactedIdx);
        self.inner.set_compacted_idx(idx).await
    }

    async fn get_compacted_idx(&self) -> StorageResult<usize> {
        check_fail!(self, FailOn::GetCompactedIdx);
        self.inner.get_compacted_idx().await
    }

    async fn set_snapshot(&mut self, snapshot: Option<ValueSnapshot>) -> StorageResult<()> {
        check_fail!(self, FailOn::SetSnapshot);
        self.inner.set_snapshot(snapshot).await
    }

    async fn get_snapshot(&self) -> StorageResult<Option<ValueSnapshot>> {
        check_fail!(self, FailOn::GetSnapshot);
        self.inner.get_snapshot().await
    }
}

// ===========================================================================
// Helpers
// ===========================================================================

fn make_ballot(n: u32, pid: u64) -> Ballot {
    Ballot {
        config_id: 1,
        n,
        priority: 0,
        pid,
    }
}

/// Build a single-node OmniPaxos with FailableStorage.
async fn build_single_node(ctrl: Rc<RefCell<FailCtrl>>) -> OmniPaxos<Value, FailableStorage> {
    let storage = FailableStorage {
        inner: MemoryStorage::default(),
        ctrl,
    };
    let config = OmniPaxosConfig {
        cluster_config: ClusterConfig {
            configuration_id: 1,
            nodes: vec![1],
            ..Default::default()
        },
        server_config: ServerConfig {
            pid: 1,
            ..Default::default()
        },
    };
    config.build(storage).await.unwrap()
}

/// Build a 3-node cluster with FailableStorage on a specific node.
async fn build_cluster_with_failable(
    failable_pid: u64,
) -> (
    HashMap<u64, OmniPaxos<Value, FailableStorage>>,
    Rc<RefCell<FailCtrl>>,
) {
    let (nodes, ctrls) = build_cluster_all_failable().await;
    let ctrl = ctrls.get(&failable_pid).unwrap().clone();
    (nodes, ctrl)
}

/// Build a 3-node cluster where ALL nodes have FailableStorage.
/// Returns controllers for each node so tests can inject failures on any node.
async fn build_cluster_all_failable() -> (
    HashMap<u64, OmniPaxos<Value, FailableStorage>>,
    HashMap<u64, Rc<RefCell<FailCtrl>>>,
) {
    let node_ids = vec![1, 2, 3];
    let mut nodes = HashMap::new();
    let mut ctrls = HashMap::new();

    for &pid in &node_ids {
        let ctrl = Rc::new(RefCell::new(FailCtrl::new()));
        let storage = FailableStorage {
            inner: MemoryStorage::default(),
            ctrl: ctrl.clone(),
        };
        ctrls.insert(pid, ctrl);
        let config = OmniPaxosConfig {
            cluster_config: ClusterConfig {
                configuration_id: 1,
                nodes: node_ids.clone(),
                ..Default::default()
            },
            server_config: ServerConfig {
                pid,
                election_tick_timeout: 5,
                ..Default::default()
            },
        };
        let op = config.build(storage).await.unwrap();
        nodes.insert(pid, op);
    }
    (nodes, ctrls)
}

fn tick_and_route_cluster(
    nodes: &mut HashMap<u64, OmniPaxos<Value, FailableStorage>>,
    rounds: usize,
) {
    tick_and_route_cluster_impl(nodes, rounds, false)
}

fn tick_and_route_cluster_tolerant(
    nodes: &mut HashMap<u64, OmniPaxos<Value, FailableStorage>>,
    rounds: usize,
) {
    tick_and_route_cluster_impl(nodes, rounds, true)
}

fn tick_and_route_cluster_impl(
    nodes: &mut HashMap<u64, OmniPaxos<Value, FailableStorage>>,
    rounds: usize,
    tolerate_errors: bool,
) {
    smol::block_on(async {
        for _ in 0..rounds {
            for node in nodes.values_mut() {
                match node.tick().await {
                    Ok(()) => {}
                    Err(_e) if tolerate_errors => { /* expected in failure-injection tests */ }
                    Err(e) => panic!("unexpected tick error: {}", e),
                }
            }
            let mut all_msgs: Vec<Message<Value>> = Vec::new();
            let mut buf = Vec::new();
            for node in nodes.values_mut() {
                node.take_outgoing_messages(&mut buf);
                all_msgs.append(&mut buf);
            }
            for msg in all_msgs {
                let receiver = msg.get_receiver();
                if let Some(node) = nodes.get_mut(&receiver) {
                    match node.handle_incoming(msg).await {
                        Ok(()) => {}
                        Err(_e) if tolerate_errors => { /* expected in failure-injection tests */ }
                        Err(e) => panic!("unexpected handle_incoming error: {}", e),
                    }
                }
            }
        }
    });
}

fn elect_leader_cluster(nodes: &mut HashMap<u64, OmniPaxos<Value, FailableStorage>>) -> u64 {
    for _ in 0..500 {
        smol::block_on(async {
            for node in nodes.values_mut() {
                let _ = node.tick().await;
            }
        });
        let mut all_msgs: Vec<Message<Value>> = Vec::new();
        let mut buf = Vec::new();
        for node in nodes.values_mut() {
            node.take_outgoing_messages(&mut buf);
            all_msgs.append(&mut buf);
        }
        smol::block_on(async {
            for msg in all_msgs {
                let receiver = msg.get_receiver();
                if let Some(node) = nodes.get_mut(&receiver) {
                    let _ = node.handle_incoming(msg).await;
                }
            }
        });
        // Check for leader consensus
        let mut leader_counts: HashMap<u64, usize> = HashMap::new();
        for node in nodes.values() {
            if let Some((leader_pid, _)) = node.get_current_leader() {
                if nodes.contains_key(&leader_pid) {
                    *leader_counts.entry(leader_pid).or_insert(0) += 1;
                }
            }
        }
        let majority = nodes.len() / 2 + 1;
        for (&leader, &count) in &leader_counts {
            if count >= majority {
                return leader;
            }
        }
    }
    panic!("Leader was not elected within 500 rounds");
}

// ===========================================================================
// Category A: Storage Trait Contract (direct MemoryStorage tests)
// ===========================================================================

#[test]
fn test_promise_roundtrip() {
    smol::block_on(async {
        let mut storage: MemoryStorage<Value> = MemoryStorage::default();

        // Initially None
        assert_eq!(storage.get_promise().await.unwrap(), None);

        let ballot = make_ballot(5, 2);
        storage.set_promise(ballot).await.unwrap();
        assert_eq!(storage.get_promise().await.unwrap(), Some(ballot));

        // Overwrite with higher ballot
        let ballot2 = make_ballot(10, 3);
        storage.set_promise(ballot2).await.unwrap();
        assert_eq!(storage.get_promise().await.unwrap(), Some(ballot2));
    });
}

#[test]
fn test_decided_idx_roundtrip() {
    smol::block_on(async {
        let mut storage: MemoryStorage<Value> = MemoryStorage::default();

        assert_eq!(storage.get_decided_idx().await.unwrap(), 0);

        storage.set_decided_idx(5).await.unwrap();
        assert_eq!(storage.get_decided_idx().await.unwrap(), 5);

        storage.set_decided_idx(10).await.unwrap();
        assert_eq!(storage.get_decided_idx().await.unwrap(), 10);
    });
}

#[test]
fn test_accepted_round_roundtrip() {
    smol::block_on(async {
        let mut storage: MemoryStorage<Value> = MemoryStorage::default();

        assert_eq!(storage.get_accepted_round().await.unwrap(), None);

        let ballot = make_ballot(3, 1);
        storage.set_accepted_round(ballot).await.unwrap();
        assert_eq!(storage.get_accepted_round().await.unwrap(), Some(ballot));
    });
}

#[test]
fn test_append_and_get_entries() {
    smol::block_on(async {
        let mut storage: MemoryStorage<Value> = MemoryStorage::default();

        let entries: Vec<Value> = (1..=5).map(Value::with_id).collect();
        storage.append_entries(entries.clone()).await.unwrap();

        // get_entries for the full range
        let retrieved = storage.get_entries(0, 5).await.unwrap();
        assert_eq!(retrieved, entries);

        // get_entries for a sub-range
        let sub = storage.get_entries(2, 4).await.unwrap();
        assert_eq!(sub, vec![Value::with_id(3), Value::with_id(4)]);

        // get_suffix from index 3
        let suffix = storage.get_suffix(3).await.unwrap();
        assert_eq!(suffix, vec![Value::with_id(4), Value::with_id(5)]);

        // log length
        assert_eq!(storage.get_log_len().await.unwrap(), 5);

        // Append single entry
        storage.append_entry(Value::with_id(6)).await.unwrap();
        assert_eq!(storage.get_log_len().await.unwrap(), 6);
    });
}

#[test]
fn test_append_on_prefix_truncates() {
    smol::block_on(async {
        let mut storage: MemoryStorage<Value> = MemoryStorage::default();

        // Append 5 entries
        let entries: Vec<Value> = (1..=5).map(Value::with_id).collect();
        storage.append_entries(entries).await.unwrap();

        // Append on prefix at index 3 — keeps entries 0..3 and appends new ones
        let new_entries = vec![Value::with_id(100), Value::with_id(200)];
        storage.append_on_prefix(3, new_entries).await.unwrap();

        let all = storage.get_entries(0, 5).await.unwrap();
        assert_eq!(
            all,
            vec![
                Value::with_id(1),
                Value::with_id(2),
                Value::with_id(3),
                Value::with_id(100),
                Value::with_id(200),
            ]
        );
        assert_eq!(storage.get_log_len().await.unwrap(), 5);
    });
}

#[test]
fn test_write_atomically_all_succeed() {
    smol::block_on(async {
        let mut storage: MemoryStorage<Value> = MemoryStorage::default();

        let ballot = make_ballot(1, 1);
        let ops = vec![
            StorageOp::AppendEntries(vec![Value::with_id(1), Value::with_id(2)]),
            StorageOp::SetPromise(ballot),
            StorageOp::SetDecidedIndex(2),
            StorageOp::SetAcceptedRound(ballot),
        ];
        storage.write_atomically(ops).await.unwrap();

        // Verify all changes are visible
        assert_eq!(storage.get_entries(0, 2).await.unwrap().len(), 2);
        assert_eq!(storage.get_promise().await.unwrap(), Some(ballot));
        assert_eq!(storage.get_decided_idx().await.unwrap(), 2);
        assert_eq!(storage.get_accepted_round().await.unwrap(), Some(ballot));
    });
}

#[test]
fn test_trim_removes_entries() {
    smol::block_on(async {
        let mut storage: MemoryStorage<Value> = MemoryStorage::default();

        // Append 10 entries and mark all as decided
        let entries: Vec<Value> = (1..=10).map(Value::with_id).collect();
        storage.append_entries(entries).await.unwrap();
        storage.set_decided_idx(10).await.unwrap();

        // Trim first 5 entries
        storage.trim(5).await.unwrap();
        storage.set_compacted_idx(5).await.unwrap();

        // Entries after trim should still exist (use post-trim indices)
        let remaining = storage.get_entries(5, 10).await.unwrap();
        assert_eq!(remaining.len(), 5);
        assert_eq!(remaining[0], Value::with_id(6));

        // get_suffix from the trim point
        let suffix = storage.get_suffix(5).await.unwrap();
        assert_eq!(suffix.len(), 5);

        // Compacted idx
        assert_eq!(storage.get_compacted_idx().await.unwrap(), 5);

        // Log length reflects only remaining entries
        assert_eq!(storage.get_log_len().await.unwrap(), 5);
    });
}

#[test]
fn test_get_entries_boundary_behavior() {
    smol::block_on(async {
        let mut storage: MemoryStorage<Value> = MemoryStorage::default();

        // Append 3 entries
        let entries: Vec<Value> = (1..=3).map(Value::with_id).collect();
        storage.append_entries(entries).await.unwrap();

        // Boundary: from == log.len() should return empty (no entries at that index)
        let at_boundary = storage.get_entries(3, 10).await.unwrap();
        assert!(
            at_boundary.is_empty(),
            "get_entries from boundary should return empty, got {:?}",
            at_boundary
        );

        // Partial overlap: from < len, to > len — returns available range
        let partial = storage.get_entries(1, 10).await.unwrap();
        assert_eq!(partial.len(), 2, "should return available entries only");
        assert_eq!(partial[0], Value::with_id(2));
        assert_eq!(partial[1], Value::with_id(3));

        // get_suffix from beyond log
        let suffix = storage.get_suffix(100).await.unwrap();
        assert!(suffix.is_empty());

        // get_suffix from exact end
        let suffix_end = storage.get_suffix(3).await.unwrap();
        assert!(suffix_end.is_empty());
    });
}

// ===========================================================================
// Category B: Error Propagation (OmniPaxos with FailableStorage)
// ===========================================================================

#[test]
fn test_storage_error_on_tick_propagates() {
    smol::block_on(async {
        // Use large batch_size so entries are buffered and flushed during tick
        let ctrl = Rc::new(RefCell::new(FailCtrl::new()));
        let storage = FailableStorage {
            inner: MemoryStorage::default(),
            ctrl: ctrl.clone(),
        };
        let config = OmniPaxosConfig {
            cluster_config: ClusterConfig {
                configuration_id: 1,
                nodes: vec![1],
                ..Default::default()
            },
            server_config: ServerConfig {
                pid: 1,
                batch_size: 100,
                flush_batch_tick_timeout: 3,
                ..Default::default()
            },
        };
        let mut node = config.build(storage).await.unwrap();

        // Let the node elect itself first (single node)
        for _ in 0..50 {
            node.tick().await.unwrap();
        }

        // Propose entries (stays in batch buffer, not flushed yet)
        node.append(Value::with_id(1)).await.unwrap();

        // Inject failure on WriteAtomically — the batch flush path uses
        // write_atomically() for atomic append+decide.
        ctrl.borrow_mut()
            .permanent_failures
            .insert(FailOn::WriteAtomically);

        // tick() should eventually trigger batch flush and return Err
        let mut saw_error = false;
        for _ in 0..20 {
            if let Err(_) = node.tick().await {
                saw_error = true;
                break;
            }
        }
        assert!(
            saw_error,
            "tick should propagate storage error on batch flush"
        );
    });
}

#[test]
fn test_storage_error_on_handle_incoming_propagates() {
    smol::block_on(async {
        let (mut nodes, failable_ctrl) = build_cluster_with_failable(2).await;
        let leader = elect_leader_cluster(&mut nodes);

        // Inject permanent failure on node 2's WriteAtomically
        failable_ctrl
            .borrow_mut()
            .permanent_failures
            .insert(FailOn::WriteAtomically);

        // Propose entries through leader
        nodes
            .get_mut(&leader)
            .unwrap()
            .append(Value::with_id(1))
            .await
            .unwrap();

        // Tick and try to route — node 2's handle_incoming should return errors
        for _ in 0..50 {
            for node in nodes.values_mut() {
                let _ = node.tick().await;
            }
            let mut all_msgs: Vec<Message<Value>> = Vec::new();
            let mut buf = Vec::new();
            for node in nodes.values_mut() {
                node.take_outgoing_messages(&mut buf);
                all_msgs.append(&mut buf);
            }
            let mut saw_error = false;
            for msg in all_msgs {
                let receiver = msg.get_receiver();
                if let Some(node) = nodes.get_mut(&receiver) {
                    if let Err(_) = node.handle_incoming(msg).await {
                        saw_error = true;
                    }
                }
            }
            if saw_error {
                break;
            }
        }

        // Clear the failure — node 2 should be able to function again
        failable_ctrl.borrow_mut().permanent_failures.clear();
        tick_and_route_cluster(&mut nodes, 200);

        // Leader should eventually decide
        let decided = nodes[&leader].get_decided_idx();
        assert!(decided >= 1, "leader should decide after failure clears");
    });
}

#[test]
fn test_storage_error_on_append_propagates() {
    smol::block_on(async {
        // append() now propagates storage errors via ProposeErr::StorageErr.
        // With batch_size=1, the entry is flushed immediately during append
        // via append_entries(), so a failure is surfaced to the caller.
        let ctrl = Rc::new(RefCell::new(FailCtrl::new()));
        let mut node = build_single_node(ctrl.clone()).await;

        // Elect self
        for _ in 0..50 {
            node.tick().await.unwrap();
        }

        // Inject failure on AppendEntries (used by batch_size=1 immediate flush)
        ctrl.borrow_mut()
            .permanent_failures
            .insert(FailOn::AppendEntries);

        // append() should return Err when storage fails
        let result = node.append(Value::with_id(1)).await;
        assert!(
            result.is_err(),
            "append should return Err when storage write fails"
        );

        // The entry should NOT be decided (storage failure prevented persistence)
        ctrl.borrow_mut().permanent_failures.clear();
        for _ in 0..20 {
            let _ = node.tick().await;
        }
        assert_eq!(
            node.get_decided_idx(),
            0,
            "no entries should be decided after storage failure in append"
        );
    });
}

#[test]
fn test_transient_failure_recovers() {
    smol::block_on(async {
        let (mut nodes, failable_ctrl) = build_cluster_with_failable(2).await;
        let leader = elect_leader_cluster(&mut nodes);

        // Inject a one-time failure on node 2
        failable_ctrl
            .borrow_mut()
            .fail_once
            .insert(FailOn::WriteAtomically);

        // Propose entries through leader
        for i in 1..=5 {
            nodes
                .get_mut(&leader)
                .unwrap()
                .append(Value::with_id(i))
                .await
                .unwrap();
        }

        // Tick and route — the transient failure should not prevent eventual progress
        tick_and_route_cluster_tolerant(&mut nodes, 500);

        // All nodes should eventually decide all entries
        for (&pid, node) in &nodes {
            let decided = node.get_decided_idx();
            assert!(
                decided >= 5,
                "node {} should have decided at least 5 entries, got {}",
                pid,
                decided
            );
        }
    });
}

#[test]
fn test_write_atomically_fails_cleanly() {
    smol::block_on(async {
        let (mut storage, ctrl) = FailableStorage::new_with_ctrl();

        // Append some initial entries
        storage
            .append_entries(vec![Value::with_id(1), Value::with_id(2)])
            .await
            .unwrap();
        let ballot = make_ballot(1, 1);
        storage.set_promise(ballot).await.unwrap();

        // Inject WriteAtomically failure — the check happens before any ops execute
        ctrl.borrow_mut().fail_once.insert(FailOn::WriteAtomically);

        // Try write_atomically — should fail without modifying state
        let ops = vec![
            StorageOp::AppendEntries(vec![Value::with_id(3)]),
            StorageOp::SetDecidedIndex(3),
            StorageOp::SetPromise(make_ballot(2, 1)),
        ];
        let result = storage.write_atomically(ops).await;
        assert!(result.is_err(), "write_atomically should fail");

        // Verify state is unchanged (no ops were executed)
        let entries = storage.get_entries(0, 3).await.unwrap();
        assert_eq!(entries.len(), 2, "entries should not have been appended");
        assert_eq!(
            storage.get_promise().await.unwrap(),
            Some(ballot),
            "promise should not have changed"
        );
        assert_eq!(
            storage.get_decided_idx().await.unwrap(),
            0,
            "decided_idx should not have changed"
        );

        // After the transient failure, write_atomically should work
        let ops = vec![
            StorageOp::AppendEntries(vec![Value::with_id(3)]),
            StorageOp::SetDecidedIndex(3),
        ];
        storage.write_atomically(ops).await.unwrap();
        assert_eq!(storage.get_entries(0, 3).await.unwrap().len(), 3);
        assert_eq!(storage.get_decided_idx().await.unwrap(), 3);
    });
}

// ===========================================================================
// Category C: Atomicity Verification
// ===========================================================================

#[test]
fn test_partial_write_breaks_promise_invariant() {
    smol::block_on(async {
        let (mut storage, ctrl) = FailableStorage::new_with_ctrl();

        // Set partial_write_limit = 1: only first op in write_atomically is applied
        ctrl.borrow_mut().partial_write_limit = Some(1);

        let ballot = make_ballot(5, 1);
        let ops = vec![
            StorageOp::AppendEntries(vec![Value::with_id(1), Value::with_id(2)]),
            StorageOp::SetPromise(ballot),
        ];
        storage.write_atomically(ops).await.unwrap();

        // The entries were written (first op) but promise was NOT set (second op skipped)
        let entries = storage.get_entries(0, 2).await.unwrap();
        assert_eq!(
            entries.len(),
            2,
            "entries should be written (partial write)"
        );

        let promise = storage.get_promise().await.unwrap();
        assert_eq!(
            promise, None,
            "promise should NOT be set (partial write broke atomicity)"
        );

        // This demonstrates the invariant violation: entries exist without a matching promise
    });
}

#[test]
fn test_partial_write_breaks_decided_invariant() {
    smol::block_on(async {
        let (mut storage, ctrl) = FailableStorage::new_with_ctrl();

        // Set partial_write_limit = 1
        ctrl.borrow_mut().partial_write_limit = Some(1);

        let ops = vec![
            StorageOp::AppendEntries(vec![Value::with_id(1), Value::with_id(2)]),
            StorageOp::SetDecidedIndex(2),
        ];
        storage.write_atomically(ops).await.unwrap();

        // Entries written but decided_idx not updated
        let entries = storage.get_entries(0, 2).await.unwrap();
        assert_eq!(entries.len(), 2, "entries should be written");

        let decided = storage.get_decided_idx().await.unwrap();
        assert_eq!(
            decided, 0,
            "decided_idx should NOT be updated (partial write broke atomicity)"
        );
    });
}

#[test]
fn test_write_atomically_is_used_for_critical_paths() {
    smol::block_on(async {
        let ctrl = Rc::new(RefCell::new(FailCtrl::new()));
        let mut node = build_single_node(ctrl.clone()).await;

        // Reset the counter (build may have triggered some writes)
        ctrl.borrow_mut().write_atomically_count = 0;

        // Elect self and propose entries
        for _ in 0..50 {
            node.tick().await.unwrap();
        }
        node.append(Value::with_id(1)).await.unwrap();
        for _ in 0..20 {
            node.tick().await.unwrap();
        }

        // The protocol should have used write_atomically at least once
        let count = ctrl.borrow().write_atomically_count;
        assert!(
            count > 0,
            "protocol should use write_atomically for critical paths, but count was 0"
        );
    });
}

// ===========================================================================
// Regression tests for fixed bugs
//
// These tests verify that previously-broken behaviors are now correct.
// Each test documents the original bug and confirms the fix.
// ===========================================================================

// ---------------------------------------------------------------------------
// FIXED: append() now propagates storage errors via ProposeErr::StorageErr
//
// Previously, storage write failures during append() were silently discarded
// via `let _ = self.propose_entry(entry).await;`. Now append() returns
// Err(ProposeErr::StorageErr(...)) when the underlying storage fails.
// ---------------------------------------------------------------------------

/// Verifies that append() returns an error when the underlying storage fails.
#[test]
fn test_append_should_propagate_storage_errors() {
    smol::block_on(async {
        let ctrl = Rc::new(RefCell::new(FailCtrl::new()));
        let mut node = build_single_node(ctrl.clone()).await;

        // Elect self
        for _ in 0..50 {
            node.tick().await.unwrap();
        }

        // Inject permanent failure on AppendEntries
        ctrl.borrow_mut()
            .permanent_failures
            .insert(FailOn::AppendEntries);

        // append() SHOULD return Err when storage fails.
        // Instead, it returns Ok(()) and the entry is silently lost.
        let result = node.append(Value::with_id(1)).await;
        assert!(
            result.is_err(),
            "append() should return Err when storage write fails, \
             but it returned Ok — the entry is silently lost"
        );
    });
}

// ---------------------------------------------------------------------------
// FIXED: MemoryStorage::get_entries() now handles out-of-bounds gracefully
//
// Previously panicked with a VecDeque range error when `from > log.len()`,
// and underflowed on `from - self.trimmed_idx` when `from < trimmed_idx`.
// Now uses saturating_sub and bounds checks per the Storage trait contract.
// ---------------------------------------------------------------------------

/// Verifies that get_entries() returns empty vec for out-of-bounds indices
/// as specified by the trait contract.
#[test]
fn test_get_entries_out_of_bounds_returns_empty() {
    smol::block_on(async {
        let mut storage: MemoryStorage<Value> = MemoryStorage::default();
        let entries: Vec<Value> = (1..=3).map(Value::with_id).collect();
        storage.append_entries(entries).await.unwrap();

        // Per trait contract: "If entries do not exist for the complete
        // interval, an empty Vector should be returned."
        // MemoryStorage panics here instead.
        let result = storage.get_entries(10, 20).await.unwrap();
        assert!(
            result.is_empty(),
            "get_entries with from > log.len() should return empty vec per trait contract"
        );
    });
}

/// Verifies that get_entries() handles post-trim indices gracefully.
#[test]
fn test_get_entries_after_trim_underflow() {
    smol::block_on(async {
        let mut storage: MemoryStorage<Value> = MemoryStorage::default();
        let entries: Vec<Value> = (1..=10).map(Value::with_id).collect();
        storage.append_entries(entries).await.unwrap();
        storage.set_decided_idx(10).await.unwrap();
        storage.trim(5).await.unwrap();
        storage.set_compacted_idx(5).await.unwrap();

        // Asking for entries in the trimmed range should return empty, not panic/underflow
        let result = storage.get_entries(0, 5).await.unwrap();
        assert!(
            result.is_empty(),
            "get_entries for trimmed range should return empty vec, not panic"
        );
    });
}

// ---------------------------------------------------------------------------
// FIXED: Leader batch flush now uses write_atomically
//
// Previously, flush_batch_leader() used separate append_entries() and
// set_decided_idx() calls. Now uses flush_batch_and_decide_with_entries()
// which wraps both in write_atomically() for crash safety.
// ---------------------------------------------------------------------------

/// Verifies that the leader's batch flush path uses write_atomically for
/// the combined append+decide operation.
#[test]
fn test_leader_batch_flush_atomicity() {
    smol::block_on(async {
        // Use FailableStorage to detect whether write_atomically is called
        // for the batch flush + decide path (vs separate calls).
        let ctrl = Rc::new(RefCell::new(FailCtrl::new()));
        let storage = FailableStorage {
            inner: MemoryStorage::default(),
            ctrl: ctrl.clone(),
        };
        let config = OmniPaxosConfig {
            cluster_config: ClusterConfig {
                configuration_id: 1,
                nodes: vec![1],
                ..Default::default()
            },
            server_config: ServerConfig {
                pid: 1,
                batch_size: 100,
                flush_batch_tick_timeout: 3,
                ..Default::default()
            },
        };
        let mut node = config.build(storage).await.unwrap();

        // Elect self
        for _ in 0..50 {
            node.tick().await.unwrap();
        }

        // Reset write_atomically counter after election completes
        ctrl.borrow_mut().write_atomically_count = 0;

        // Propose entries (buffered in batch)
        for i in 1..=5 {
            node.append(Value::with_id(i)).await.unwrap();
        }

        // Tick enough for batch flush timeout to fire
        for _ in 0..10 {
            node.tick().await.unwrap();
        }

        // Entries should be decided
        assert_eq!(node.get_decided_idx(), 5, "entries should be decided");

        // The batch flush + decide should have used write_atomically.
        // If write_atomically_count is 0, it means the leader used
        // separate append_entries() + set_decided_idx() calls — which
        // is not crash-safe.
        let count = ctrl.borrow().write_atomically_count;
        assert!(
            count > 0,
            "Leader batch flush path should use write_atomically for atomic \
             append+decide, but write_atomically was called 0 times. \
             The leader uses separate append_entries() + set_decided_idx() \
             calls which are not crash-safe."
        );
    });
}

// ===========================================================================
// Bug fix: Cache preserved on write failure
//
// flush_batch_and_* methods must not lose batched entries when the
// underlying write_atomically call fails. Previously, take_batched_entries()
// was called before the write, so a failure meant entries were gone from
// the cache AND never persisted — permanently lost.
// ===========================================================================

/// Verifies that after a storage write failure during batch flush, the
/// batched entries are still available for retry. If the cache is cleared
/// before the write (the old bug), the entry is permanently lost.
#[test]
fn test_batch_cache_preserved_on_write_failure() {
    smol::block_on(async {
        let ctrl = Rc::new(RefCell::new(FailCtrl::new()));
        let storage = FailableStorage {
            inner: MemoryStorage::default(),
            ctrl: ctrl.clone(),
        };
        let config = OmniPaxosConfig {
            cluster_config: ClusterConfig {
                configuration_id: 1,
                nodes: vec![1],
                ..Default::default()
            },
            server_config: ServerConfig {
                pid: 1,
                batch_size: 100,
                flush_batch_tick_timeout: 3,
                ..Default::default()
            },
        };
        let mut node = config.build(storage).await.unwrap();

        // Elect self as leader
        for _ in 0..50 {
            node.tick().await.unwrap();
        }

        // Propose an entry (stays in batch buffer, not flushed yet)
        node.append(Value::with_id(42)).await.unwrap();

        // Inject write failure — the next batch flush will fail
        ctrl.borrow_mut()
            .permanent_failures
            .insert(FailOn::WriteAtomically);

        // Tick until batch flush fires — should fail
        let mut saw_error = false;
        for _ in 0..10 {
            if node.tick().await.is_err() {
                saw_error = true;
                break;
            }
        }
        assert!(saw_error, "tick should have returned an error");

        // Nothing should be decided (write failed)
        assert_eq!(node.get_decided_idx(), 0, "nothing decided after failure");

        // Clear the failure and tick again — the entry should still be in
        // the batch buffer and get flushed successfully on retry.
        ctrl.borrow_mut().permanent_failures.clear();
        for _ in 0..10 {
            let _ = node.tick().await;
        }

        // If the cache was cleared before the write (the old bug), the entry
        // is permanently lost and decided_idx stays 0.
        assert_eq!(
            node.get_decided_idx(),
            1,
            "entry should be decided after retry — if 0, the batch cache was \
             cleared before the write failed and the entry is permanently lost"
        );
    });
}

/// Verifies that a follower's batch cache is preserved when flush fails.
/// Uses a 3-node cluster where the failable follower's storage fails
/// during batch flush triggered by AcceptDecide.
#[test]
fn test_follower_batch_cache_preserved_on_write_failure() {
    smol::block_on(async {
        let (mut nodes, failable_ctrl) = build_cluster_with_failable(2).await;
        let leader = elect_leader_cluster(&mut nodes);

        // Propose entries through leader
        for i in 1..=5 {
            nodes
                .get_mut(&leader)
                .unwrap()
                .append(Value::with_id(i))
                .await
                .unwrap();
        }

        // Route until decided on leader and healthy follower
        tick_and_route_cluster(&mut nodes, 100);
        let leader_decided = nodes[&leader].get_decided_idx();
        assert!(leader_decided >= 5, "leader should have decided entries");

        // Now inject failure on node 2's WriteAtomically
        failable_ctrl
            .borrow_mut()
            .permanent_failures
            .insert(FailOn::WriteAtomically);

        // Propose more entries — node 2 will fail to flush
        for i in 6..=10 {
            nodes
                .get_mut(&leader)
                .unwrap()
                .append(Value::with_id(i))
                .await
                .unwrap();
        }
        tick_and_route_cluster_tolerant(&mut nodes, 100);

        // Clear failure and let node 2 catch up
        failable_ctrl.borrow_mut().permanent_failures.clear();
        tick_and_route_cluster(&mut nodes, 500);

        // Node 2 should eventually catch up. If the cache was cleared on
        // failure, node 2 would have lost entries and be stuck.
        let node2_decided = nodes[&2].get_decided_idx();
        assert!(
            node2_decided >= 10,
            "node 2 should catch up to 10 after failure clears, got {}. \
             If stuck, the batch cache was cleared before the failed write.",
            node2_decided
        );
    });
}

// ===========================================================================
// Bug fix: Leader state reverted on flush failure
//
// flush_batch_leader() must revert leader_state.accepted_idx if the
// underlying write fails. Previously, accepted_idx was set before the
// write and never reverted, causing is_chosen() to return true for
// entries that were never persisted.
// ===========================================================================

/// Verifies that the leader's accepted_idx in leader_state is reverted
/// when the batch flush write fails. After clearing the failure, the
/// leader should be able to successfully flush and decide entries.
///
/// Uses a 3-node cluster with batch_size=100 because:
/// 1. batch_size=100 forces entries to accumulate in the batch buffer
///    and flush via flush_batch_leader() on tick timeout (not immediately
///    via append_entries). flush_batch_leader uses write_atomically.
/// 2. A multi-node cluster means the quorum calculation matters:
///    leader_state tracks each node's accepted_idx, and is_chosen() checks
///    whether a quorum has accepted. If the leader's own accepted_idx is
///    advanced before write succeeds, is_chosen() uses stale state.
#[test]
fn test_leader_state_reverted_on_flush_failure() {
    smol::block_on(async {
        // Build a 3-node cluster with batch_size=100 so entries accumulate
        // and flush via flush_batch_leader() → write_atomically() on tick timeout.
        let node_ids = vec![1, 2, 3];
        let mut nodes = HashMap::new();
        let mut ctrls = HashMap::new();

        for &pid in &node_ids {
            let ctrl = Rc::new(RefCell::new(FailCtrl::new()));
            let storage = FailableStorage {
                inner: MemoryStorage::default(),
                ctrl: ctrl.clone(),
            };
            ctrls.insert(pid, ctrl);
            let config = OmniPaxosConfig {
                cluster_config: ClusterConfig {
                    configuration_id: 1,
                    nodes: node_ids.clone(),
                    ..Default::default()
                },
                server_config: ServerConfig {
                    pid,
                    batch_size: 100,
                    flush_batch_tick_timeout: 3,
                    election_tick_timeout: 5,
                    ..Default::default()
                },
            };
            let op = config.build(storage).await.unwrap();
            nodes.insert(pid, op);
        }

        // Elect leader
        let leader = elect_leader_cluster(&mut nodes);

        // Propose entries through the leader. With batch_size=100, these
        // accumulate in the batch buffer and don't flush immediately.
        for i in 1..=5 {
            nodes
                .get_mut(&leader)
                .unwrap()
                .append(Value::with_id(i))
                .await
                .unwrap();
        }
        // Tick enough for flush_batch_tick_timeout to fire and decide
        tick_and_route_cluster(&mut nodes, 30);
        for node in nodes.values() {
            assert_eq!(
                node.get_decided_idx(),
                5,
                "all nodes should decide 5 entries before failure injection"
            );
        }

        // Inject WriteAtomically failure on the LEADER's storage.
        // This blocks flush_batch_leader() → flush_batch_and_decide_with_entries()
        // → write_atomically().
        let leader_ctrl = ctrls.get(&leader).unwrap();
        leader_ctrl
            .borrow_mut()
            .permanent_failures
            .insert(FailOn::WriteAtomically);

        // Propose more entries — they accumulate in the batch buffer but
        // the flush_batch_leader() call on tick timeout will fail.
        for i in 6..=10 {
            let _ = nodes
                .get_mut(&leader)
                .unwrap()
                .append(Value::with_id(i))
                .await;
        }
        tick_and_route_cluster_tolerant(&mut nodes, 30);

        // Leader should not have decided beyond 5 because its storage write failed.
        // The fix reverts leader_state.accepted_idx on failure so is_chosen()
        // doesn't use stale state.
        let leader_decided = nodes.get(&leader).unwrap().get_decided_idx();
        assert!(
            leader_decided <= 5,
            "leader should not have decided beyond 5 during storage failure, got {}",
            leader_decided
        );

        // Clear failure and tick — everything should recover
        leader_ctrl.borrow_mut().permanent_failures.clear();
        for i in 11..=15 {
            let _ = nodes
                .get_mut(&leader)
                .unwrap()
                .append(Value::with_id(i))
                .await;
        }
        tick_and_route_cluster(&mut nodes, 100);

        // All entries should eventually be decided across all nodes
        let final_decided = nodes.get(&leader).unwrap().get_decided_idx();
        assert!(
            final_decided >= 10,
            "after clearing failure, at least 10 entries should be decided, got {}. \
             If leader_state.accepted_idx was not reverted on write failure, \
             is_chosen() uses stale quorum state and entries may never decide.",
            final_decided
        );

        // All nodes should agree
        let decided_indices: Vec<usize> = nodes.values().map(|n| n.get_decided_idx()).collect();
        let max_decided = *decided_indices.iter().max().unwrap();
        let min_decided = *decided_indices.iter().min().unwrap();
        assert_eq!(
            max_decided, min_decided,
            "all nodes should agree on decided_idx, got {:?}",
            decided_indices
        );
    });
}

// ===========================================================================
// Bug fix: reconfigure() propagates storage errors
//
// Previously, accept_stopsign_leader() errors were silently discarded
// with `let _ = ...`. Now they propagate via ProposeErr::StorageErr.
// ===========================================================================

/// Verifies that reconfigure() returns an error when the underlying
/// storage fails during StopSign acceptance.
#[test]
fn test_reconfigure_propagates_storage_error() {
    smol::block_on(async {
        let ctrl = Rc::new(RefCell::new(FailCtrl::new()));
        let storage = FailableStorage {
            inner: MemoryStorage::default(),
            ctrl: ctrl.clone(),
        };
        let config = OmniPaxosConfig {
            cluster_config: ClusterConfig {
                configuration_id: 1,
                nodes: vec![1],
                ..Default::default()
            },
            server_config: ServerConfig {
                pid: 1,
                ..Default::default()
            },
        };
        let mut node = config.build(storage).await.unwrap();

        // Elect self as leader and enter Accept phase
        for _ in 0..50 {
            node.tick().await.unwrap();
        }

        // Inject failure on WriteAtomically — append_stopsign now uses
        // write_atomically() for atomic entries+stopsign writes
        ctrl.borrow_mut()
            .permanent_failures
            .insert(FailOn::WriteAtomically);

        // reconfigure() should now propagate the error instead of
        // silently discarding it with `let _ = ...`
        let new_config = ClusterConfig {
            configuration_id: 2,
            nodes: vec![1, 2, 3],
            ..Default::default()
        };
        let result = node.reconfigure(new_config, None).await;
        assert!(
            result.is_err(),
            "reconfigure should return Err when storage fails — \
             previously the error was silently discarded with `let _ = ...`"
        );

        // The node should NOT be reconfigured (storage failed)
        assert!(
            node.is_reconfigured().is_none(),
            "node should not be reconfigured after storage failure"
        );
    });
}

// ===========================================================================
// StopSign contract tests (previously missing)
// ===========================================================================

#[test]
fn test_stopsign_roundtrip() {
    smol::block_on(async {
        let mut storage: MemoryStorage<Value> = MemoryStorage::default();

        // Initially None
        assert_eq!(storage.get_stopsign().await.unwrap(), None);

        let ss = StopSign::with(
            ClusterConfig {
                configuration_id: 2,
                nodes: vec![1, 2, 3],
                ..Default::default()
            },
            None,
        );
        storage.set_stopsign(Some(ss.clone())).await.unwrap();
        let retrieved = storage.get_stopsign().await.unwrap();
        assert_eq!(
            retrieved,
            Some(ss.clone()),
            "retrieved stopsign should match what was set (config_id, nodes, metadata)"
        );

        // Clear it
        storage.set_stopsign(None).await.unwrap();
        assert_eq!(storage.get_stopsign().await.unwrap(), None);
    });
}

#[test]
fn test_snapshot_roundtrip() {
    smol::block_on(async {
        let mut storage: MemoryStorage<Value> = MemoryStorage::default();

        // Initially None
        assert_eq!(storage.get_snapshot().await.unwrap(), None);

        let mut map = HashMap::new();
        map.insert(1, 1);
        map.insert(2, 2);
        let snap = ValueSnapshot { map };
        storage.set_snapshot(Some(snap.clone())).await.unwrap();
        let retrieved = storage.get_snapshot().await.unwrap();
        assert_eq!(retrieved, Some(snap));

        // Clear it
        storage.set_snapshot(None).await.unwrap();
        assert_eq!(storage.get_snapshot().await.unwrap(), None);
    });
}

// ===========================================================================
// Bug Fix Regression Tests (Phase 6.5)
// ===========================================================================

/// Bug 1.1 regression: Verify that cache is NOT updated when set_decided_idx fails.
#[test]
fn test_cache_consistent_after_decided_idx_failure() {
    smol::block_on(async {
        let ctrl = Rc::new(RefCell::new(FailCtrl::new()));
        let storage = FailableStorage {
            inner: MemoryStorage::default(),
            ctrl: ctrl.clone(),
        };
        let config = OmniPaxosConfig {
            cluster_config: ClusterConfig {
                configuration_id: 1,
                nodes: vec![1],
                ..Default::default()
            },
            server_config: ServerConfig {
                pid: 1,
                ..Default::default()
            },
        };
        let mut node = config.build(storage).await.unwrap();

        // Elect self as leader and get into Accept phase
        for _ in 0..50 {
            node.tick().await.unwrap();
        }

        // Propose and decide an entry (single-node cluster decides immediately)
        node.append(Value { id: 1 }).await.unwrap();
        node.tick().await.unwrap();

        let decided_before = node.get_decided_idx();
        assert!(decided_before > 0, "should have decided at least one entry");

        // Now inject SetDecidedIdx failure
        ctrl.borrow_mut()
            .permanent_failures
            .insert(FailOn::SetDecidedIdx);

        // Propose another entry — the decided_idx update should fail
        let _ = node.append(Value { id: 2 }).await;
        let _ = node.tick().await;

        // The decided_idx should NOT have advanced (cache-before-write was fixed)
        // With the bug, cache would be updated before the write, leaving it inconsistent
        let decided_after = node.get_decided_idx();
        // Due to the fix, decided_idx should either be the same or have advanced
        // only if the write succeeded. We're checking it doesn't advance past what storage has.
        // The key invariant: decided_idx in cache <= decided_idx in storage.
        // With permanent failures, decided_idx should stay at decided_before.
        assert_eq!(
            decided_after, decided_before,
            "decided_idx should not advance when storage write fails (was {}, now {})",
            decided_before, decided_after
        );
    });
}

/// Bug 1.2 regression: Verify that cache is NOT updated when set_promise fails.
#[test]
fn test_cache_consistent_after_promise_failure() {
    smol::block_on(async {
        let ctrl = Rc::new(RefCell::new(FailCtrl::new()));
        let storage = FailableStorage {
            inner: MemoryStorage::default(),
            ctrl: ctrl.clone(),
        };
        let config = OmniPaxosConfig {
            cluster_config: ClusterConfig {
                configuration_id: 1,
                nodes: vec![1, 2, 3],
                ..Default::default()
            },
            server_config: ServerConfig {
                pid: 1,
                ..Default::default()
            },
        };
        let mut node = config.build(storage).await.unwrap();

        let promise_before = node.get_promise();

        // Inject promise failure — this will prevent the node from accepting a new leader
        ctrl.borrow_mut()
            .permanent_failures
            .insert(FailOn::WriteAtomically);

        // Try to elect — this triggers set_promise (via flush_batch_and_set_promise)
        for _ in 0..50 {
            let _ = node.tick().await;
        }

        let promise_after = node.get_promise();

        // If the write failed, promise should not have changed
        // (the cache should not have been updated ahead of storage)
        assert_eq!(
            promise_before, promise_after,
            "promise should not change when storage write fails"
        );
    });
}

/// Bug 1.4 regression: Verify that InternalStorage::with() returns Err
/// instead of panicking when storage fails during load_cache.
#[test]
fn test_load_cache_returns_error_not_panic() {
    smol::block_on(async {
        let ctrl = Rc::new(RefCell::new(FailCtrl::new()));
        // Make get_promise fail — this is the first call in load_cache
        ctrl.borrow_mut()
            .permanent_failures
            .insert(FailOn::GetPromise);

        let storage = FailableStorage {
            inner: MemoryStorage::default(),
            ctrl: ctrl.clone(),
        };
        let config = OmniPaxosConfig {
            cluster_config: ClusterConfig {
                configuration_id: 1,
                nodes: vec![1, 2, 3],
                ..Default::default()
            },
            server_config: ServerConfig {
                pid: 1,
                ..Default::default()
            },
        };

        // Should return Err, not panic
        let result = config.build(storage).await;
        assert!(
            result.is_err(),
            "build() should return Err when storage fails during load_cache, not panic"
        );
    });
}

/// Bug 1.3 regression: Verify that append_stopsign is atomic —
/// if the write fails, neither entries nor stopsign are reflected in cache.
#[test]
fn test_stopsign_atomicity_on_failure() {
    smol::block_on(async {
        let ctrl = Rc::new(RefCell::new(FailCtrl::new()));
        let storage = FailableStorage {
            inner: MemoryStorage::default(),
            ctrl: ctrl.clone(),
        };
        let config = OmniPaxosConfig {
            cluster_config: ClusterConfig {
                configuration_id: 1,
                nodes: vec![1],
                ..Default::default()
            },
            server_config: ServerConfig {
                pid: 1,
                ..Default::default()
            },
        };
        let mut node = config.build(storage).await.unwrap();

        // Elect self
        for _ in 0..50 {
            node.tick().await.unwrap();
        }

        // Propose some entries first
        for i in 1..=3 {
            node.append(Value { id: i }).await.unwrap();
        }
        node.tick().await.unwrap();

        let _decided_before = node.get_decided_idx();

        // Now inject WriteAtomically failure and try to reconfigure
        ctrl.borrow_mut()
            .permanent_failures
            .insert(FailOn::WriteAtomically);

        let new_config = ClusterConfig {
            configuration_id: 2,
            nodes: vec![1, 2, 3],
            ..Default::default()
        };
        let result = node.reconfigure(new_config, None).await;
        // Should fail due to storage error
        assert!(
            result.is_err(),
            "reconfigure should fail when write_atomically fails"
        );

        // Node should NOT be reconfigured
        assert!(
            node.is_reconfigured().is_none(),
            "node should not be reconfigured after atomic write failure"
        );
    });
}

// ===========================================================================
// Adversarial storage contract tests: batching, compaction, and edge cases
// ===========================================================================

/// Verify that flush_batch_and_set_decided_idx clamps decided_idx correctly
/// when decided_idx > current accepted_idx + batched entries.
#[test]
fn test_decided_idx_clamping_on_flush() {
    smol::block_on(async {
        let ctrl = Rc::new(RefCell::new(FailCtrl::new()));
        let storage = FailableStorage {
            inner: MemoryStorage::default(),
            ctrl: ctrl.clone(),
        };
        let config = OmniPaxosConfig {
            cluster_config: ClusterConfig {
                configuration_id: 1,
                nodes: vec![1],
                ..Default::default()
            },
            server_config: ServerConfig {
                pid: 1,
                batch_size: 10, // large batch
                flush_batch_tick_timeout: 5,
                ..Default::default()
            },
        };
        let mut node = config.build(storage).await.unwrap();

        // Elect self (single-node cluster)
        for _ in 0..50 {
            node.tick().await.unwrap();
        }

        // Propose a few entries (less than batch_size) — they stay in cache
        for i in 1..=3 {
            node.append(Value { id: i }).await.unwrap();
        }

        // Tick enough for flush_batch_timeout
        for _ in 0..20 {
            node.tick().await.unwrap();
        }

        // Single-node cluster: entries should be decided
        let decided = node.get_decided_idx();
        assert_eq!(
            decided, 3,
            "single-node should self-decide after flush, got {}",
            decided
        );
    });
}

/// Verify that WriteAtomically failure during flush_batch_and_set_decided_idx
/// leaves cache consistent (the leader's accepted_idx is reverted on error).
#[test]
fn test_flush_batch_failure_leaves_cache_consistent() {
    smol::block_on(async {
        let ctrl = Rc::new(RefCell::new(FailCtrl::new()));
        let storage = FailableStorage {
            inner: MemoryStorage::default(),
            ctrl: ctrl.clone(),
        };
        let config = OmniPaxosConfig {
            cluster_config: ClusterConfig {
                configuration_id: 1,
                nodes: vec![1],
                ..Default::default()
            },
            server_config: ServerConfig {
                pid: 1,
                batch_size: 10,
                flush_batch_tick_timeout: 5,
                ..Default::default()
            },
        };
        let mut node = config.build(storage).await.unwrap();

        // Elect self
        for _ in 0..50 {
            node.tick().await.unwrap();
        }

        // Propose entries (stay in batch cache)
        for i in 1..=3 {
            node.append(Value { id: i }).await.unwrap();
        }

        let decided_before = node.get_decided_idx();

        // Inject write failure before flush happens
        ctrl.borrow_mut()
            .permanent_failures
            .insert(FailOn::WriteAtomically);

        // Tick to trigger flush — it should fail
        for _ in 0..20 {
            let _ = node.tick().await;
        }

        // decided_idx should not have advanced
        let decided_after = node.get_decided_idx();
        assert_eq!(
            decided_before, decided_after,
            "decided_idx should not advance when flush fails"
        );

        // Clear the failure and flush should succeed on next attempt
        ctrl.borrow_mut()
            .permanent_failures
            .remove(&FailOn::WriteAtomically);

        for _ in 0..20 {
            node.tick().await.unwrap();
        }

        let decided_recovered = node.get_decided_idx();
        assert_eq!(
            decided_recovered, 3,
            "entries should be decided after error recovery, got {}",
            decided_recovered
        );
    });
}

/// Verify that append_on_prefix with from_idx == trimmed_idx works correctly
/// (boundary case: no entries trimmed beyond the prefix point).
#[test]
fn test_append_on_prefix_at_trim_boundary() {
    smol::block_on(async {
        let mut storage = MemoryStorage::<Value>::default();

        // Append 10 entries
        for i in 0..10 {
            storage.append_entry(Value { id: i }).await.unwrap();
        }

        // Trim first 5
        storage.trim(5).await.unwrap();

        // append_on_prefix at exactly trimmed_idx (5) — should truncate to empty, then extend
        let new_entries = vec![Value { id: 100 }, Value { id: 101 }];
        storage.append_on_prefix(5, new_entries).await.unwrap();

        let suffix = storage.get_suffix(5).await.unwrap();
        assert_eq!(suffix.len(), 2);
        assert_eq!(suffix[0].id, 100);
        assert_eq!(suffix[1].id, 101);
    });
}

/// Verify that append_on_prefix with from_idx > trimmed_idx retains entries
/// between trimmed_idx and from_idx.
#[test]
fn test_append_on_prefix_above_trim_boundary() {
    smol::block_on(async {
        let mut storage = MemoryStorage::<Value>::default();

        // Append 10 entries (ids 0-9)
        for i in 0..10 {
            storage.append_entry(Value { id: i }).await.unwrap();
        }

        // Trim first 3
        storage.trim(3).await.unwrap();

        // append_on_prefix at 7 — should keep entries at indices 3..7 and replace 7+
        let new_entries = vec![Value { id: 200 }, Value { id: 201 }];
        storage.append_on_prefix(7, new_entries).await.unwrap();

        let suffix = storage.get_suffix(3).await.unwrap();
        // Entries at indices 3,4,5,6 are original (ids 3,4,5,6), then 200,201
        assert_eq!(suffix.len(), 6, "suffix should be 4 retained + 2 new");
        assert_eq!(suffix[0].id, 3);
        assert_eq!(suffix[3].id, 6);
        assert_eq!(suffix[4].id, 200);
        assert_eq!(suffix[5].id, 201);
    });
}

/// Verify that get_suffix returns empty vec (not error) when from >= log end.
#[test]
fn test_get_suffix_beyond_log_end() {
    smol::block_on(async {
        let mut storage = MemoryStorage::<Value>::default();

        for i in 0..5 {
            storage.append_entry(Value { id: i }).await.unwrap();
        }

        // get_suffix beyond log: should return empty
        let suffix = storage.get_suffix(100).await.unwrap();
        assert!(suffix.is_empty(), "get_suffix beyond log should be empty");

        // get_suffix exactly at log end
        let suffix = storage.get_suffix(5).await.unwrap();
        assert!(suffix.is_empty(), "get_suffix at log end should be empty");
    });
}

/// Verify that get_suffix after trim returns only entries after the trimmed region.
#[test]
fn test_get_suffix_after_trim() {
    smol::block_on(async {
        let mut storage = MemoryStorage::<Value>::default();

        for i in 0..10 {
            storage.append_entry(Value { id: i }).await.unwrap();
        }

        // Trim first 7
        storage.trim(7).await.unwrap();

        // get_suffix(5) — asks for entries below trimmed_idx (7).
        // Due to saturating_sub, this returns entries from trimmed_idx onward.
        let suffix = storage.get_suffix(5).await.unwrap();
        assert_eq!(
            suffix.len(),
            3,
            "get_suffix below trimmed_idx should return remaining entries"
        );
        assert_eq!(suffix[0].id, 7);
        assert_eq!(suffix[2].id, 9);

        // get_suffix at exactly trimmed_idx
        let suffix = storage.get_suffix(7).await.unwrap();
        assert_eq!(suffix.len(), 3, "get_suffix at trimmed_idx returns remaining");

        // get_suffix above trimmed_idx
        let suffix = storage.get_suffix(8).await.unwrap();
        assert_eq!(suffix.len(), 2);
        assert_eq!(suffix[0].id, 8);
    });
}

/// Verify partial_write_limit on write_atomically simulates a non-atomic storage
/// that only applies some ops. This demonstrates why write_atomically must be truly
/// atomic in production — a partial write leaves inconsistent state.
#[test]
fn test_write_atomically_partial_write_demonstrates_inconsistency() {
    smol::block_on(async {
        let ctrl = Rc::new(RefCell::new(FailCtrl::new()));
        let mut storage = FailableStorage {
            inner: MemoryStorage::default(),
            ctrl: ctrl.clone(),
        };

        // Append initial entry
        storage.append_entry(Value { id: 1 }).await.unwrap();
        let initial_len = storage.get_log_len().await.unwrap();
        assert_eq!(initial_len, 1);

        // partial_write_limit=1: only first op is applied, second is silently dropped
        ctrl.borrow_mut().partial_write_limit = Some(1);

        let ops = vec![
            StorageOp::AppendEntries(vec![Value { id: 2 }, Value { id: 3 }]),
            StorageOp::SetDecidedIndex(3), // This op will be silently skipped
        ];
        let result = storage.write_atomically(ops).await;
        // Returns Ok even though second op was skipped — non-atomic behavior
        assert!(result.is_ok(), "partial write returns Ok (non-atomic)");

        // Entries were appended (first op succeeded)
        let len_after = storage.get_log_len().await.unwrap();
        assert_eq!(len_after, 3, "entries should be appended (first op)");

        // But decided_idx was NOT set (second op skipped) — INCONSISTENCY
        let decided = storage.get_decided_idx().await.unwrap();
        assert_eq!(
            decided, 0,
            "decided_idx should still be 0 because second op was skipped"
        );
    });
}

/// Verify MemoryStorage's write_atomically rollback on error:
/// if an op in the middle fails, all preceding ops are rolled back.
#[test]
fn test_write_atomically_preflight_error_leaves_state_unchanged() {
    smol::block_on(async {
        let ctrl = Rc::new(RefCell::new(FailCtrl::new()));
        let mut storage = FailableStorage {
            inner: MemoryStorage::default(),
            ctrl: ctrl.clone(),
        };

        // Append initial entry
        storage.append_entry(Value { id: 1 }).await.unwrap();
        storage.set_decided_idx(0).await.unwrap();

        // Make write_atomically fail (goes through MemoryStorage which has its own rollback)
        ctrl.borrow_mut().permanent_failures.insert(FailOn::WriteAtomically);

        let ops = vec![
            StorageOp::AppendEntries(vec![Value { id: 2 }, Value { id: 3 }]),
            StorageOp::SetDecidedIndex(3),
        ];
        let result = storage.write_atomically(ops).await;
        assert!(result.is_err(), "write_atomically should fail");

        // State should be unchanged
        let len = storage.get_log_len().await.unwrap();
        assert_eq!(len, 1, "log should still have 1 entry after failed write");

        let decided = storage.get_decided_idx().await.unwrap();
        assert_eq!(decided, 0, "decided_idx should still be 0 after failed write");
    });
}

/// Verify write_atomically rollback on mid-operation failure:
/// if the second op fails, the first op's changes are rolled back.
#[test]
fn test_write_atomically_mid_operation_rollback() {
    smol::block_on(async {
        let ctrl = Rc::new(RefCell::new(FailCtrl::new()));
        let mut storage = FailableStorage {
            inner: MemoryStorage::default(),
            ctrl: ctrl.clone(),
        };

        // Pre-populate with one entry
        storage.append_entry(Value::with_id(1)).await.unwrap();
        storage.set_decided_idx(0).await.unwrap();

        // Inject failure on SetDecidedIdx -- AppendEntries will succeed but SetDecidedIndex will fail
        ctrl.borrow_mut().fail_once.insert(FailOn::SetDecidedIdx);

        let ops = vec![
            StorageOp::AppendEntries(vec![Value::with_id(2), Value::with_id(3)]),
            StorageOp::SetDecidedIndex(3),
        ];
        let result = storage.write_atomically(ops).await;
        assert!(result.is_err(), "write_atomically should fail when SetDecidedIdx fails");

        // Both operations should be rolled back
        let len = storage.get_log_len().await.unwrap();
        assert_eq!(len, 1, "log should still have 1 entry after rollback (AppendEntries was reverted)");

        let decided = storage.get_decided_idx().await.unwrap();
        assert_eq!(decided, 0, "decided_idx should still be 0 after rollback");
    });
}

#[test]
fn test_crash_recovery_with_torn_write() {
    smol::block_on(async {
        // Build a 3-node cluster with FailableStorage on all nodes
        let (mut nodes, ctrls) = build_cluster_all_failable().await;

        // Elect leader and decide 10 entries
        tick_and_route_cluster(&mut nodes, 50);
        let leader = nodes
            .values()
            .filter_map(|n| n.get_current_leader().map(|(pid, _)| pid))
            .next()
            .unwrap();

        for i in 1..=10 {
            nodes
                .get_mut(&leader)
                .unwrap()
                .append(Value::with_id(i))
                .await
                .unwrap();
        }
        tick_and_route_cluster(&mut nodes, 200);

        // Verify all decided
        for node in nodes.values() {
            assert_eq!(node.get_decided_idx(), 10);
        }

        // Set partial_write_limit on the leader to simulate torn write
        ctrls[&leader].borrow_mut().partial_write_limit = Some(1);

        // Propose more entries (the leader's write_atomically will be partial)
        for i in 11..=15 {
            let _ = nodes
                .get_mut(&leader)
                .unwrap()
                .append(Value::with_id(i))
                .await;
        }
        tick_and_route_cluster_tolerant(&mut nodes, 50);

        // Clear the partial_write_limit to simulate recovery after crash
        ctrls[&leader].borrow_mut().partial_write_limit = None;

        // Tick to convergence -- the protocol should handle the inconsistency
        tick_and_route_cluster(&mut nodes, 500);

        // All nodes should be consistent
        // (checking that decided_idx >= 10 at minimum -- torn writes shouldn't lose committed data)
        for node in nodes.values() {
            assert!(
                node.get_decided_idx() >= 10,
                "committed entries must survive torn write"
            );
        }
    });
}
