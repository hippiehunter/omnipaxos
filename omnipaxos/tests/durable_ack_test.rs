//! Durable-before-ack: a node votes for an entry (a follower's `Accepted`, the
//! leader's self-vote) only once the entry is durable in its storage under the
//! voted round, so every decided entry is durable on a write quorum.
//!
//! The cluster runs on `ReferenceAsyncStorage`, whose log writes become durable
//! only when the harness flushes them, and a crash rebuilds a node from exactly
//! what recovery would find. Every `Accepted` a node sends and every vote it
//! counts for itself is checked against that crash image.

use omnipaxos::{
    ballot_leader_election::Ballot,
    messages::{
        sequence_paxos::{Accepted, PaxosMessage, PaxosMsg},
        Message,
    },
    metrics::Phase,
    storage::{PersistedState, StopSign, Storage, StorageOp, StorageResult},
    util::LogEntry,
    ClusterConfig, OmniPaxos, OmniPaxosConfig, ServerConfig,
};
use std::{
    cell::RefCell,
    collections::{btree_map, BTreeMap, VecDeque},
    rc::Rc,
};

mod test_utils;
use test_utils::{Value, ValueSnapshot};

// ===========================================================================
// ReferenceAsyncStorage
// ===========================================================================

/// One storage state: what reads see, what is durable, or what a crash
/// recovers.
#[derive(Clone, Default)]
struct Image {
    log: VecDeque<Value>,
    trimmed_idx: u64,
    promise: Option<Ballot>,
    accepted_round: Option<Ballot>,
    decided_idx: u64,
    compacted_idx: u64,
    stopsign: Option<StopSign>,
    snapshot: Option<ValueSnapshot>,
}

impl Image {
    fn apply(&mut self, op: &StorageOp<Value>) {
        match op {
            StorageOp::AppendEntry(entry, _) => self.log.push_back(entry.clone()),
            StorageOp::AppendEntries(entries, _) => self.log.extend(entries.iter().cloned()),
            StorageOp::AppendOnPrefix(from_idx, entries, _) => {
                self.log
                    .truncate(from_idx.saturating_sub(self.trimmed_idx) as usize);
                self.log.extend(entries.iter().cloned());
            }
            StorageOp::SetPromise(ballot) => self.promise = Some(*ballot),
            StorageOp::SetDecidedIndex(idx) => self.decided_idx = *idx,
            StorageOp::SetAcceptedRound(ballot) => self.accepted_round = Some(*ballot),
            StorageOp::SetCompactedIdx(idx) => self.compacted_idx = *idx,
            StorageOp::Trim(idx) => {
                let to_trim = ((idx - self.trimmed_idx) as usize).min(self.log.len());
                self.log.drain(..to_trim);
                self.trimmed_idx = *idx;
            }
            StorageOp::SetStopsign(ss) => self.stopsign = ss.clone(),
            StorageOp::SetSnapshot(snapshot) => self.snapshot = snapshot.clone(),
            StorageOp::BeginSync { .. } => {}
        }
    }

    /// End of the real log, counting trimmed entries.
    fn real_len(&self) -> u64 {
        self.trimmed_idx + self.log.len() as u64
    }

    /// The accepted index a node rebuilt from this image starts with.
    fn accepted_idx(&self) -> u64 {
        self.real_len() + u64::from(self.stopsign.is_some())
    }

    fn entry(&self, idx: u64) -> Option<&Value> {
        idx.checked_sub(self.trimmed_idx)
            .and_then(|i| self.log.get(i as usize))
    }

    fn ids(&self) -> Vec<u64> {
        self.log.iter().map(|v| v.id).collect()
    }
}

/// Models a write-ahead log with group commit:
/// - log ops (appends, rewrites, decided index, trims, snapshots) and
///   `BeginSync` queue up in write order and become durable only when the
///   harness flushes them;
/// - `SetPromise` is durable on return;
/// - `SetStopsign` and `SetAcceptedRound` are durable on return and ordered
///   after everything written before them (a WAL-ordered seal or commit);
/// - from a `BeginSync` until the `SetAcceptedRound` that commits it, a crash
///   recovers the durable state the sync started from.
///
/// With `durable_on_return` every write is durable when it returns, the
/// contract `MemoryStorage` follows; it runs the same harness against the
/// engine's synchronous-durability mode.
struct RefState {
    durable_on_return: bool,
    visible: Image,
    durable: Image,
    queue: VecDeque<StorageOp<Value>>,
    /// The durable image as of the open sync's `BeginSync`, once it is flushed.
    pre_sync: Option<Image>,
    sync_open: bool,
    /// The real-log head the open sync produced.
    sync_head: u64,
}

impl RefState {
    fn new(image: Image, durable_on_return: bool) -> Self {
        RefState {
            durable_on_return,
            visible: image.clone(),
            durable: image,
            queue: VecDeque::new(),
            pre_sync: None,
            sync_open: false,
            sync_head: 0,
        }
    }

    fn write(&mut self, ops: Vec<StorageOp<Value>>) {
        if self.durable_on_return {
            for op in ops {
                self.visible.apply(&op);
                self.durable.apply(&op);
            }
            return;
        }
        let opens_sync = matches!(ops.first(), Some(StorageOp::BeginSync { .. }));
        let commits = ops
            .iter()
            .any(|op| matches!(op, StorageOp::SetAcceptedRound(_)));
        if self.sync_open && commits {
            assert!(
                self.durable_log_len() >= self.sync_head,
                "AcceptSync committed before its log was durable: durable {} < sync head {}",
                self.durable_log_len(),
                self.sync_head
            );
        }
        for op in ops {
            self.visible.apply(&op);
            match op {
                StorageOp::SetPromise(_) => {
                    self.durable.apply(&op);
                    if let Some(pre_sync) = &mut self.pre_sync {
                        pre_sync.apply(&op);
                    }
                }
                StorageOp::SetStopsign(_) => {
                    assert!(
                        !self.sync_open || commits,
                        "SetStopsign written inside an uncommitted AcceptSync"
                    );
                    self.flush_all();
                    self.durable.apply(&op);
                }
                StorageOp::SetAcceptedRound(_) => {
                    self.flush_all();
                    self.durable.apply(&op);
                    self.sync_open = false;
                    self.pre_sync = None;
                }
                StorageOp::BeginSync { .. } => {
                    self.sync_open = true;
                    self.queue.push_back(op);
                }
                _ => self.queue.push_back(op),
            }
        }
        if opens_sync {
            self.sync_head = self.visible.real_len();
        }
    }

    /// Make the oldest `n` queued writes durable.
    fn flush(&mut self, n: usize) {
        for _ in 0..n {
            let Some(op) = self.queue.pop_front() else {
                return;
            };
            if matches!(op, StorageOp::BeginSync { .. })
                && self.sync_open
                && self.pre_sync.is_none()
            {
                self.pre_sync = Some(self.durable.clone());
            }
            self.durable.apply(&op);
        }
    }

    fn flush_all(&mut self) {
        self.flush(self.queue.len());
    }

    /// The durable real-log prefix as a host would report it: the durable log,
    /// cut at any queued rewrite (its old suffix is about to be replaced).
    fn durable_log_len(&self) -> u64 {
        self.queue
            .iter()
            .filter_map(|op| match op {
                StorageOp::AppendOnPrefix(from_idx, ..) => Some(*from_idx),
                _ => None,
            })
            .fold(self.durable.real_len(), u64::min)
    }

    /// What recovery finds if the node crashes now.
    fn crash_image(&self) -> &Image {
        match (&self.pre_sync, self.sync_open) {
            (Some(pre_sync), true) => pre_sync,
            _ => &self.durable,
        }
    }
}

struct ReferenceAsyncStorage {
    state: Rc<RefCell<RefState>>,
}

impl Storage<Value> for ReferenceAsyncStorage {
    async fn write_atomically(&mut self, ops: Vec<StorageOp<Value>>) -> StorageResult<()> {
        self.state.borrow_mut().write(ops);
        Ok(())
    }

    async fn load_state(&self) -> StorageResult<PersistedState<Value>> {
        let state = self.state.borrow();
        let image = &state.visible;
        Ok(PersistedState {
            promise: image.promise,
            accepted_round: image.accepted_round,
            decided_idx: image.decided_idx,
            compacted_idx: image.compacted_idx,
            stopsign: image.stopsign.clone(),
            log_len: image.log.len() as u64,
            snapshot: image.snapshot.clone(),
        })
    }

    async fn get_entries(&self, from: u64, to: u64) -> StorageResult<Vec<Value>> {
        let state = self.state.borrow();
        let image = &state.visible;
        let from = from.saturating_sub(image.trimmed_idx) as usize;
        let to = to.saturating_sub(image.trimmed_idx) as usize;
        if from >= image.log.len() {
            return Ok(vec![]);
        }
        Ok(image
            .log
            .range(from..to.min(image.log.len()))
            .cloned()
            .collect())
    }

    async fn get_suffix(&self, from: u64) -> StorageResult<Vec<Value>> {
        let state = self.state.borrow();
        let image = &state.visible;
        let start = from.saturating_sub(image.trimmed_idx) as usize;
        if start >= image.log.len() {
            Ok(vec![])
        } else {
            Ok(image.log.range(start..).cloned().collect())
        }
    }

    async fn get_snapshot(&self) -> StorageResult<Option<ValueSnapshot>> {
        Ok(self.state.borrow().visible.snapshot.clone())
    }

    async fn get_log_len(&self) -> StorageResult<u64> {
        Ok(self.state.borrow().visible.log.len() as u64)
    }

    fn log_writes_durable_on_return(&self) -> bool {
        self.state.borrow().durable_on_return
    }
}

// ===========================================================================
// Cluster harness
// ===========================================================================

type Node = OmniPaxos<Value, ReferenceAsyncStorage>;
type MessageFilter = Box<dyn Fn(&Message<Value>) -> bool>;

/// How much of a node's queued writes the harness flushes each round.
#[derive(Clone, Copy, PartialEq)]
enum Flush {
    All,
    Random,
    Hold,
}

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

fn test_seed(default: u64) -> u64 {
    std::env::var("OMNIPAXOS_TEST_SEED")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(default)
}

struct Cluster {
    nodes: BTreeMap<u64, Node>,
    states: BTreeMap<u64, Rc<RefCell<RefState>>>,
    crashed: BTreeMap<u64, Image>,
    outboxes: BTreeMap<u64, Vec<Message<Value>>>,
    cluster_config: ClusterConfig,
    server_configs: BTreeMap<u64, ServerConfig>,
    flush: BTreeMap<u64, Flush>,
    filters: Vec<Option<MessageFilter>>,
    rng: SimpleRng,
    /// Every decided entry any node has reported, by log index.
    observed_decided: BTreeMap<u64, u64>,
    /// Every `Accepted` sent, with its sender.
    sent_accepted: Vec<(u64, Accepted)>,
    durable_on_return: bool,
    round: u64,
    /// Recent deliveries and node states, printed when an invariant fails.
    trace: VecDeque<String>,
}

const TRACE_LINES: usize = 20_000;

impl Cluster {
    async fn new(node_ids: Vec<u64>, seed: u64) -> Self {
        Self::with_durability(node_ids, seed, false).await
    }

    async fn with_durability(node_ids: Vec<u64>, seed: u64, durable_on_return: bool) -> Self {
        let cluster_config = ClusterConfig {
            configuration_id: 1,
            nodes: node_ids.clone(),
            ..Default::default()
        };
        let server_configs = node_ids
            .iter()
            .map(|&pid| {
                (
                    pid,
                    ServerConfig {
                        pid,
                        election_tick_timeout: 5,
                        ..Default::default()
                    },
                )
            })
            .collect();
        Self::with_configs(cluster_config, server_configs, seed, durable_on_return).await
    }

    async fn with_configs(
        cluster_config: ClusterConfig,
        server_configs: BTreeMap<u64, ServerConfig>,
        seed: u64,
        durable_on_return: bool,
    ) -> Self {
        let mut cluster = Cluster {
            nodes: BTreeMap::new(),
            states: BTreeMap::new(),
            crashed: BTreeMap::new(),
            outboxes: BTreeMap::new(),
            cluster_config,
            server_configs,
            flush: BTreeMap::new(),
            filters: Vec::new(),
            rng: SimpleRng::new(seed),
            observed_decided: BTreeMap::new(),
            sent_accepted: Vec::new(),
            durable_on_return,
            round: 0,
            trace: VecDeque::new(),
        };
        let pids: Vec<u64> = cluster.server_configs.keys().copied().collect();
        for pid in pids {
            cluster.flush.insert(pid, Flush::All);
            cluster.start_node(pid, Image::default()).await;
        }
        cluster
    }

    async fn start_node(&mut self, pid: u64, image: Image) {
        let state = Rc::new(RefCell::new(RefState::new(image, self.durable_on_return)));
        let storage = ReferenceAsyncStorage {
            state: state.clone(),
        };
        let config = OmniPaxosConfig {
            cluster_config: self.cluster_config.clone(),
            server_config: self.server_configs[&pid].clone(),
        };
        let node = config.build(storage).await.unwrap();
        self.nodes.insert(pid, node);
        self.states.insert(pid, state);
        self.outboxes.insert(pid, Vec::new());
        self.drain(pid);
    }

    /// Collect a node's outgoing messages after a call on it, checking every
    /// `Accepted` it sends and its own vote against what a crash would leave.
    fn drain(&mut self, pid: u64) {
        let mut msgs = Vec::new();
        self.nodes
            .get_mut(&pid)
            .unwrap()
            .take_outgoing_messages(&mut msgs);
        let state = self.states[&pid].borrow();
        let crash = state.crash_image();
        for msg in &msgs {
            if let Message::SequencePaxos(PaxosMessage {
                msg: PaxosMsg::Accepted(acc),
                ..
            }) = msg
            {
                assert_eq!(
                    crash.accepted_round,
                    Some(acc.n),
                    "node {pid} sent Accepted({}) in round {:?}, but a crash recovers round {:?}",
                    acc.accepted_idx,
                    acc.n,
                    crash.accepted_round
                );
                assert!(
                    crash.accepted_idx() >= acc.accepted_idx,
                    "node {pid} sent Accepted({}), but a crash recovers only {}",
                    acc.accepted_idx,
                    crash.accepted_idx()
                );
                let from = crash.trimmed_idx.max(state.visible.trimmed_idx);
                for idx in from..acc.accepted_idx.min(crash.real_len()) {
                    assert_eq!(
                        crash.entry(idx).map(|v| v.id),
                        state.visible.entry(idx).map(|v| v.id),
                        "node {pid} sent Accepted({}), but a crash recovers different content at {idx}",
                        acc.accepted_idx
                    );
                }
                self.sent_accepted.push((pid, *acc));
            }
        }
        let durable_accepted_idx = self.nodes[&pid].get_durable_accepted_idx();
        assert!(
            crash.accepted_idx() >= durable_accepted_idx,
            "node {pid} votes up to {durable_accepted_idx}, but a crash recovers only {}",
            crash.accepted_idx()
        );
        drop(state);
        self.outboxes.get_mut(&pid).unwrap().append(&mut msgs);
    }

    fn live_nodes(&self) -> Vec<u64> {
        self.nodes.keys().copied().collect()
    }

    fn is_crashed(&self, pid: u64) -> bool {
        !self.nodes.contains_key(&pid)
    }

    fn set_flush(&mut self, pid: u64, flush: Flush) {
        self.flush.insert(pid, flush);
    }

    // -- Driving ------------------------------------------------------------

    async fn tick_all(&mut self) {
        for pid in self.live_nodes() {
            self.nodes.get_mut(&pid).unwrap().tick().await.unwrap();
            self.drain(pid);
        }
    }

    /// Deliver every queued message once, dropping what the filters block.
    async fn route_once(&mut self) {
        let mut msgs = Vec::new();
        for outbox in self.outboxes.values_mut() {
            msgs.append(outbox);
        }
        for msg in msgs {
            if self.filters.iter().flatten().any(|filter| !filter(&msg)) {
                if let Message::SequencePaxos(p) = &msg {
                    self.note(format!("dropped {}->{} {:?}", p.from, p.to, p.msg));
                }
                continue;
            }
            self.deliver(msg).await;
        }
    }

    async fn deliver(&mut self, msg: Message<Value>) {
        let to = msg.get_receiver();
        if let Message::SequencePaxos(p) = &msg {
            self.note(format!("deliver {}->{} {:?}", p.from, p.to, p.msg));
        }
        let traced = matches!(msg, Message::SequencePaxos(_));
        if let Some(node) = self.nodes.get_mut(&to) {
            node.handle_incoming(msg).await.unwrap();
            self.drain(to);
            if traced {
                let state = self.state_line(to);
                self.note(state);
            }
        }
    }

    fn note(&mut self, line: String) {
        if self.trace.len() == TRACE_LINES {
            self.trace.pop_front();
        }
        let round = self.round;
        self.trace.push_back(format!("r{round} {line}"));
    }

    fn state_line(&self, pid: u64) -> String {
        let node = &self.nodes[&pid];
        let m = node.metrics();
        let state = self.states[&pid].borrow();
        format!(
            "  node {pid}: {:?}/{:?} promise {}.{} accepted {} durable {} decided {} compacted {} \
             committed round {:?} trimmed {} log {:?} queued {}",
            m.role,
            m.phase,
            m.promise.n,
            m.promise.pid,
            m.accepted_idx,
            node.get_durable_accepted_idx(),
            m.decided_idx,
            m.compacted_idx,
            state.durable.accepted_round.map(|b| (b.n, b.pid)),
            state.visible.trimmed_idx,
            state.visible.ids(),
            state.queue.len()
        )
    }

    /// Print the trace and every node's state, then fail.
    fn fail(&self, message: String) -> ! {
        for line in &self.trace {
            eprintln!("{line}");
        }
        for &pid in self.nodes.keys() {
            eprintln!("{}", self.state_line(pid));
        }
        panic!("{message}");
    }

    /// Flush `n` of a node's queued writes, without reporting them.
    fn flush_ops(&mut self, pid: u64, n: usize) {
        self.states[&pid].borrow_mut().flush(n);
    }

    /// Report a durable log prefix to a node.
    async fn report(&mut self, pid: u64, durable_log_len: u64) {
        let before = self.nodes[&pid].get_durable_accepted_idx();
        self.nodes
            .get_mut(&pid)
            .unwrap()
            .on_log_durable(durable_log_len)
            .await
            .unwrap();
        self.drain(pid);
        if self.nodes[&pid].get_durable_accepted_idx() != before {
            let state = self.state_line(pid);
            self.note(format!("report {pid} durable {durable_log_len}\n{state}"));
        }
    }

    /// Flush each node's writes per its policy and report what is durable.
    async fn report_durability(&mut self) {
        for pid in self.live_nodes() {
            let queued = self.states[&pid].borrow().queue.len();
            let n = match self.flush[&pid] {
                Flush::All => queued,
                Flush::Random => self.rng.next_range(queued as u64 + 1) as usize,
                Flush::Hold => 0,
            };
            self.flush_ops(pid, n);
            let durable_log_len = self.states[&pid].borrow().durable_log_len();
            self.report(pid, durable_log_len).await;
        }
    }

    async fn step(&mut self) {
        self.round += 1;
        self.tick_all().await;
        self.route_once().await;
        self.report_durability().await;
        self.observe_decided().await;
        self.check_round_prefixes();
    }

    async fn run(&mut self, rounds: usize) {
        for _ in 0..rounds {
            self.step().await;
        }
    }

    async fn run_until(&mut self, max_rounds: usize, done: impl Fn(&Cluster) -> bool) -> bool {
        for _ in 0..max_rounds {
            if done(self) {
                return true;
            }
            self.step().await;
        }
        done(self)
    }

    async fn propose(&mut self, pid: u64, id: u64) {
        self.note(format!("propose {id} at {pid}"));
        let _ = self
            .nodes
            .get_mut(&pid)
            .unwrap()
            .append(Value::with_id(id))
            .await;
        self.drain(pid);
    }

    async fn reconfigure(&mut self, pid: u64, nodes: Vec<u64>) {
        let config = ClusterConfig {
            configuration_id: self.cluster_config.configuration_id + 1,
            nodes,
            ..Default::default()
        };
        self.nodes
            .get_mut(&pid)
            .unwrap()
            .reconfigure(config, None)
            .await
            .unwrap();
        self.drain(pid);
    }

    async fn try_become_leader(&mut self, pid: u64) {
        self.note(format!("try_become_leader {pid}"));
        self.nodes
            .get_mut(&pid)
            .unwrap()
            .try_become_leader()
            .await
            .unwrap();
        self.drain(pid);
    }

    /// Remove and return the indexes of the `Accepted` messages a node has
    /// queued.
    fn take_accepted(&mut self, pid: u64) -> Vec<u64> {
        let outbox = self.outboxes.get_mut(&pid).unwrap();
        let mut accepted = Vec::new();
        outbox.retain(|msg| match msg {
            Message::SequencePaxos(PaxosMessage {
                msg: PaxosMsg::Accepted(acc),
                ..
            }) => {
                accepted.push(acc.accepted_idx);
                false
            }
            _ => true,
        });
        accepted
    }

    // -- Faults -------------------------------------------------------------

    /// Power loss: the node restarts from what its storage made durable.
    fn crash(&mut self, pid: u64) {
        self.note(format!("crash {pid}"));
        let image = self.states[&pid].borrow().crash_image().clone();
        self.crashed.insert(pid, image);
        self.nodes.remove(&pid);
        self.states.remove(&pid);
        self.outboxes.remove(&pid);
    }

    async fn recover(&mut self, pid: u64) {
        self.note(format!("recover {pid}"));
        let image = self.crashed.remove(&pid).unwrap();
        self.start_node(pid, image).await;
        for other in self.live_nodes() {
            if other != pid {
                self.nodes.get_mut(&other).unwrap().reconnected(pid);
                self.drain(other);
            }
        }
    }

    fn add_filter(&mut self, filter: MessageFilter) -> usize {
        self.filters.push(Some(filter));
        self.filters.len() - 1
    }

    fn remove_filter(&mut self, handle: usize) {
        self.note(format!("remove filter {handle}"));
        self.filters[handle] = None;
    }

    fn isolate(&mut self, pid: u64) -> usize {
        self.add_filter(Box::new(move |msg| {
            msg.get_sender() != pid && msg.get_receiver() != pid
        }))
    }

    fn partition(&mut self, group_a: Vec<u64>, group_b: Vec<u64>) -> usize {
        self.note(format!("partition {group_a:?} | {group_b:?}"));
        self.add_filter(Box::new(move |msg| {
            let (s, r) = (msg.get_sender(), msg.get_receiver());
            !((group_a.contains(&s) && group_b.contains(&r))
                || (group_b.contains(&s) && group_a.contains(&r)))
        }))
    }

    fn reconnect_all(&mut self) {
        self.note("reconnect all".to_string());
        let live = self.live_nodes();
        for &pid in &live {
            for &other in &live {
                if other != pid {
                    self.nodes.get_mut(&pid).unwrap().reconnected(other);
                }
            }
            self.drain(pid);
        }
    }

    /// Heal every fault, flush every write each round, and run.
    async fn heal_and_converge(&mut self, rounds: usize) {
        self.filters.clear();
        let crashed: Vec<u64> = self.crashed.keys().copied().collect();
        for pid in crashed {
            self.recover(pid).await;
        }
        self.reconnect_all();
        let pids: Vec<u64> = self.flush.keys().copied().collect();
        for pid in pids {
            self.flush.insert(pid, Flush::All);
        }
        self.run(rounds).await;
    }

    // -- Inspection ---------------------------------------------------------

    fn elected_leader(&self) -> Option<u64> {
        let mut counts: BTreeMap<u64, usize> = BTreeMap::new();
        for node in self.nodes.values() {
            if let Some((leader, _)) = node.get_current_leader() {
                *counts.entry(leader).or_insert(0) += 1;
            }
        }
        let majority = self.cluster_config.nodes.len() / 2 + 1;
        counts
            .into_iter()
            .find(|&(leader, count)| {
                count >= majority
                    && self
                        .nodes
                        .get(&leader)
                        .is_some_and(|node| node.get_current_leader() == Some((leader, true)))
            })
            .map(|(leader, _)| leader)
    }

    /// Run until an acceptable leader is in its Accept phase, then long enough
    /// for its followers to apply its AcceptSync.
    async fn elect_leader_where(&mut self, accept: impl Fn(u64) -> bool) -> u64 {
        for _ in 0..500 {
            self.step().await;
            if let Some(leader) = self.elected_leader().filter(|&l| accept(l)) {
                self.run(10).await;
                return leader;
            }
        }
        panic!("no leader elected within 500 rounds");
    }

    async fn elect_leader(&mut self) -> u64 {
        self.elect_leader_where(|_| true).await
    }

    fn others(&self, pid: u64) -> (u64, u64) {
        let others: Vec<u64> = self
            .cluster_config
            .nodes
            .iter()
            .copied()
            .filter(|&p| p != pid)
            .collect();
        (others[0], others[1])
    }

    /// Record every node's decided entries and fail if a decided entry ever
    /// changes, on any node, across any crash.
    async fn observe_decided(&mut self) {
        let mut decided = Vec::new();
        for (&pid, node) in &self.nodes {
            let from = node.get_compacted_idx();
            let to = node.get_decided_idx_without_stopsign();
            if to <= from {
                continue;
            }
            let entries = node.read_entries(from..to).await.unwrap().unwrap();
            for (i, entry) in entries.into_iter().enumerate() {
                let idx = from + i as u64;
                match entry {
                    LogEntry::Decided(value) => decided.push((pid, idx, value.id)),
                    other => self.fail(format!(
                        "node {pid}: entry {idx} below decided index {to} is {other:?}"
                    )),
                }
            }
        }
        for (pid, idx, id) in decided {
            match self.observed_decided.entry(idx) {
                btree_map::Entry::Vacant(slot) => {
                    slot.insert(id);
                }
                btree_map::Entry::Occupied(slot) => {
                    let observed = *slot.get();
                    if observed != id {
                        self.fail(format!(
                            "node {pid}: decided entry {idx} changed from {observed} to {id}"
                        ));
                    }
                }
            }
        }
    }

    /// Within one round every acceptor's log is a prefix of the round
    /// leader's: nodes whose committed accepted round is the same agree on
    /// every index both still hold.
    fn check_round_prefixes(&self) {
        let pids: Vec<u64> = self.live_nodes();
        for (i, &a) in pids.iter().enumerate() {
            for &b in &pids[i + 1..] {
                let state_a = self.states[&a].borrow();
                let state_b = self.states[&b].borrow();
                let (image_a, image_b) = (state_a.crash_image(), state_b.crash_image());
                if image_a.accepted_round.is_none()
                    || image_a.accepted_round != image_b.accepted_round
                {
                    continue;
                }
                let from = image_a.trimmed_idx.max(image_b.trimmed_idx);
                let to = image_a.real_len().min(image_b.real_len());
                for idx in from..to {
                    let (x, y) = (
                        image_a.entry(idx).unwrap().id,
                        image_b.entry(idx).unwrap().id,
                    );
                    if x != y {
                        self.fail(format!(
                            "nodes {a} and {b} committed the same round but hold {x} and {y} at {idx}"
                        ));
                    }
                }
            }
        }
    }

    fn decided_log(&self) -> Vec<u64> {
        self.observed_decided.values().copied().collect()
    }

    fn assert_converged(&self) {
        let decided: BTreeMap<u64, u64> = self
            .nodes
            .iter()
            .map(|(&pid, node)| (pid, node.get_decided_idx()))
            .collect();
        let first = *decided.values().next().unwrap();
        assert!(
            decided.values().all(|&d| d == first),
            "live nodes have not converged: {decided:?}"
        );
    }
}

/// `log` keeps the order of the elements of `ids` it contains.
fn assert_in_order(log: &[u64], ids: &[u64]) {
    let present: Vec<u64> = log.iter().copied().filter(|id| ids.contains(id)).collect();
    let expected: Vec<u64> = ids
        .iter()
        .copied()
        .filter(|id| present.contains(id))
        .collect();
    assert_eq!(
        present, expected,
        "entries reordered in the decided log {log:?}"
    );
}

// ===========================================================================
// F1-F8: durable acknowledgement
// ===========================================================================

/// Seeded crashes, partitions, proposals and partial WAL flushes: no node ever
/// acknowledges or self-votes past what a crash would leave it, and no decided
/// entry ever changes.
#[test]
fn accepted_never_exceeds_committed_durable_prefix() {
    smol::block_on(async {
        let nodes = vec![1, 2, 3, 4, 5];
        let mut cluster = Cluster::new(nodes.clone(), test_seed(7)).await;
        for &pid in &nodes {
            cluster.set_flush(pid, Flush::Random);
        }
        let mut partition = None;
        for next_id in (1u64..).take(500) {
            match cluster.rng.next_range(10) {
                0 => {
                    let target = nodes[cluster.rng.next_range(nodes.len() as u64) as usize];
                    if cluster.is_crashed(target) {
                        cluster.recover(target).await;
                    } else if cluster.live_nodes().len() > 3 {
                        cluster.crash(target);
                    }
                }
                1 => match partition.take() {
                    Some(filter) => {
                        cluster.remove_filter(filter);
                        cluster.reconnect_all();
                    }
                    None => {
                        let split = 1 + cluster.rng.next_range(nodes.len() as u64 - 1) as usize;
                        partition = Some(
                            cluster.partition(nodes[..split].to_vec(), nodes[split..].to_vec()),
                        );
                    }
                },
                _ => {}
            }
            let live = cluster.live_nodes();
            let proposer = live[cluster.rng.next_range(live.len() as u64) as usize];
            cluster.propose(proposer, next_id).await;
            cluster.run(3).await;
        }
        cluster.heal_and_converge(500).await;
        cluster.assert_converged();
        assert!(
            !cluster.observed_decided.is_empty(),
            "the run should decide entries"
        );
    });
}

/// A durable leader and a follower whose WAL never flushes are not a write
/// quorum: the follower appends but does not acknowledge, and nothing is
/// decided until its log is durable.
#[test]
fn leader_ignores_volatile_follower_accept() {
    smol::block_on(async {
        let mut cluster = Cluster::new(vec![1, 2, 3], test_seed(11)).await;
        let leader = cluster.elect_leader().await;
        let (volatile, cut_off) = cluster.others(leader);
        cluster.isolate(cut_off);
        cluster.set_flush(volatile, Flush::Hold);
        let decided = cluster.nodes[&leader].get_decided_idx();

        for id in 1..=3 {
            cluster.propose(leader, id).await;
        }
        cluster.run(50).await;

        assert_eq!(
            cluster.nodes[&volatile].get_accepted_idx(),
            decided + 3,
            "the follower appends the entries"
        );
        assert_eq!(cluster.nodes[&volatile].get_durable_accepted_idx(), decided);
        assert_eq!(
            cluster.nodes[&leader].get_decided_idx(),
            decided,
            "a volatile follower's accept must not complete a write quorum"
        );

        cluster.set_flush(volatile, Flush::All);
        cluster.run(10).await;
        assert_eq!(cluster.nodes[&leader].get_decided_idx(), decided + 3);
    });
}

/// The divergence a volatile acknowledgement allowed: {durable leader,
/// volatile follower} decide, the follower loses power, the other majority
/// moves on and rewrites the slots. With durable acknowledgements those
/// entries were never decided, nothing decided is ever lost or changed, and a
/// whole-cluster power loss keeps every decided entry.
#[test]
fn decided_entry_survives_volatile_state_loss() {
    smol::block_on(async {
        let mut cluster = Cluster::new(vec![1, 2, 3], test_seed(13)).await;
        let leader = cluster.elect_leader().await;
        let (volatile, cut_off) = cluster.others(leader);
        for id in 1..=3 {
            cluster.propose(leader, id).await;
        }
        cluster.run(30).await;
        assert_eq!(cluster.decided_log(), vec![1, 2, 3]);

        let cut_off_filter = cluster.isolate(cut_off);
        cluster.set_flush(volatile, Flush::Hold);
        for id in 101..=103 {
            cluster.propose(leader, id).await;
        }
        cluster.run(30).await;

        // The follower loses power with its accepts unflushed, the old leader
        // is cut off, and the other majority takes over.
        cluster.crash(volatile);
        cluster.remove_filter(cut_off_filter);
        let leader_filter = cluster.isolate(leader);
        cluster.recover(volatile).await;
        cluster.set_flush(volatile, Flush::All);
        let new_leader = cluster.elect_leader_where(|l| l != leader).await;
        for id in 201..=203 {
            cluster.propose(new_leader, id).await;
        }
        cluster.run(50).await;
        assert!(cluster.nodes[&new_leader].get_decided_idx() >= 6);

        // The old leader rejoins; then every node loses power with whatever
        // its WAL had flushed.
        cluster.remove_filter(leader_filter);
        cluster.reconnect_all();
        cluster.run(50).await;
        for pid in [1, 2, 3] {
            cluster.set_flush(pid, Flush::Random);
        }
        for id in 301..=303 {
            cluster.propose(new_leader, id).await;
        }
        cluster.run(3).await;
        for pid in [1, 2, 3] {
            cluster.crash(pid);
        }
        cluster.heal_and_converge(500).await;
        cluster.assert_converged();

        let log = cluster.decided_log();
        assert_eq!(&log[..3], &[1, 2, 3]);
        for proposer_ids in [&[101, 102, 103], &[201, 202, 203], &[301, 302, 303]] {
            assert_in_order(&log, proposer_ids);
        }
        for id in [201, 202, 203] {
            assert!(log.contains(&id), "decided entry {id} lost: {log:?}");
        }
    });
}

/// The leader's own vote lags while its WAL holds its writes, yet the
/// followers' durable acknowledgements decide the entries; losing the
/// leader's unflushed log loses nothing decided.
#[test]
fn followers_decide_while_leader_wal_lags() {
    smol::block_on(async {
        let mut cluster = Cluster::new(vec![1, 2, 3], test_seed(17)).await;
        let leader = cluster.elect_leader().await;
        cluster.set_flush(leader, Flush::Hold);
        let decided = cluster.nodes[&leader].get_decided_idx();

        for id in 1..=5 {
            cluster.propose(leader, id).await;
        }
        cluster.run(30).await;

        for (pid, node) in &cluster.nodes {
            assert_eq!(node.get_decided_idx(), decided + 5, "node {pid}");
        }
        assert_eq!(
            cluster.nodes[&leader].get_durable_accepted_idx(),
            decided,
            "the leader's vote must wait for its own WAL"
        );

        cluster.crash(leader);
        cluster.elect_leader_where(|l| l != leader).await;
        cluster.heal_and_converge(200).await;
        cluster.assert_converged();
        assert_eq!(cluster.decided_log(), vec![1, 2, 3, 4, 5]);
    });
}

/// Several appends made durable by one WAL flush are acknowledged by one
/// `Accepted` covering all of them.
#[test]
fn one_ack_covers_group_commit() {
    smol::block_on(async {
        let mut cluster = Cluster::new(vec![1, 2, 3], test_seed(19)).await;
        let leader = cluster.elect_leader().await;
        let (follower, _) = cluster.others(leader);
        cluster.set_flush(follower, Flush::Hold);
        let durable = cluster.nodes[&follower].get_durable_accepted_idx();
        cluster.take_accepted(follower);

        for id in 1..=5 {
            cluster.propose(leader, id).await;
        }
        cluster.route_once().await;
        assert_eq!(cluster.nodes[&follower].get_accepted_idx(), durable + 5);
        assert!(
            cluster.take_accepted(follower).is_empty(),
            "appends must not be acknowledged before they are durable"
        );

        cluster.flush_ops(follower, usize::MAX);
        let durable_log_len = cluster.states[&follower].borrow().durable_log_len();
        cluster.report(follower, durable_log_len).await;
        assert_eq!(cluster.take_accepted(follower), vec![durable + 5]);
    });
}

/// A crash between an AcceptSync's log rewrite and its round commit recovers
/// the previous round and log, and the follower never acknowledges in the new
/// round before the commit.
#[test]
fn accept_sync_crash_before_commit_restores_prior_round() {
    smol::block_on(async {
        let mut cluster = Cluster::new(vec![1, 2, 3], test_seed(23)).await;
        let leader = cluster.elect_leader().await;
        let (follower, successor) = cluster.others(leader);
        for id in 1..=3 {
            cluster.propose(leader, id).await;
        }
        cluster.run(30).await;
        let prior_round = cluster.states[&follower].borrow().durable.accepted_round;
        let prior_log = cluster.states[&follower].borrow().durable.ids();
        assert_eq!(prior_log, vec![1, 2, 3]);

        // The follower misses three entries, and the successor takes over in
        // a new round while it is cut off.
        let follower_filter = cluster.isolate(follower);
        for id in 4..=6 {
            cluster.propose(leader, id).await;
        }
        cluster.run(30).await;
        cluster.try_become_leader(successor).await;
        assert!(
            cluster
                .run_until(100, |c| c.nodes[&successor].get_current_leader()
                    == Some((successor, true)))
                .await
        );
        let new_round = cluster.nodes[&successor].get_promise();

        // The follower rejoins with its WAL held: it applies the AcceptSync's
        // rewrite but cannot commit the round.
        cluster.set_flush(follower, Flush::Hold);
        cluster.remove_filter(follower_filter);
        cluster.reconnect_all();
        assert!(
            cluster
                .run_until(300, |c| c.states[&follower].borrow().sync_open)
                .await,
            "the follower should receive the AcceptSync"
        );
        cluster.run(10).await;
        assert!(cluster.states[&follower].borrow().sync_open);
        assert_eq!(
            cluster.states[&follower].borrow().durable.accepted_round,
            prior_round
        );
        assert!(
            !cluster
                .sent_accepted
                .iter()
                .any(|(pid, acc)| *pid == follower && acc.n == new_round),
            "the follower acknowledged the new round before committing it"
        );

        // Part of the sync reaches the WAL, then the follower loses power.
        cluster.flush_ops(follower, 3);
        cluster.crash(follower);
        let image = cluster.crashed[&follower].clone();
        assert_eq!(image.accepted_round, prior_round);
        assert_eq!(image.ids(), prior_log);

        cluster.heal_and_converge(300).await;
        cluster.assert_converged();
        assert_eq!(cluster.nodes[&follower].get_decided_idx(), 6);
        assert_eq!(cluster.decided_log(), vec![1, 2, 3, 4, 5, 6]);
    });
}

/// An AcceptSync built from a Promise the follower made before a crash that
/// lost part of its log is refused; the follower asks to be prepared again
/// and is synced from what it recovered.
#[test]
fn stale_accept_sync_after_restart_is_fenced() {
    smol::block_on(async {
        let mut cluster = Cluster::new(vec![1, 2, 3], test_seed(29)).await;
        let leader = cluster.elect_leader().await;
        let (follower, successor) = cluster.others(leader);
        for id in 1..=3 {
            cluster.propose(leader, id).await;
        }
        cluster.run(30).await;

        // The follower appends three more entries it never makes durable.
        cluster.set_flush(follower, Flush::Hold);
        for id in 4..=6 {
            cluster.propose(leader, id).await;
        }
        cluster.run(30).await;
        assert_eq!(cluster.nodes[&follower].get_accepted_idx(), 6);

        // The successor's AcceptSync answers the follower's Promise of six
        // entries; hold it back.
        let captured: Rc<RefCell<Vec<Message<Value>>>> = Rc::new(RefCell::new(Vec::new()));
        let sink = captured.clone();
        let capture = cluster.add_filter(Box::new(move |msg| {
            let is_sync = matches!(
                msg,
                Message::SequencePaxos(PaxosMessage {
                    to,
                    msg: PaxosMsg::AcceptSync(_),
                    ..
                }) if *to == follower
            );
            if is_sync {
                sink.borrow_mut().push(msg.clone());
            }
            !is_sync
        }));
        cluster.try_become_leader(successor).await;
        assert!(
            cluster
                .run_until(100, |_| !captured.borrow().is_empty())
                .await
        );
        cluster.remove_filter(capture);
        let stale = captured.borrow_mut().remove(0);

        // Power loss drops the unflushed entries; the follower restarts and
        // promises again with the three it recovered.
        cluster.crash(follower);
        assert_eq!(cluster.crashed[&follower].accepted_idx(), 3);
        cluster.recover(follower).await;
        cluster.set_flush(follower, Flush::All);
        assert!(
            cluster.nodes[&follower].metrics().phase == Phase::Recover,
            "a restarted follower recovers before following"
        );
        cluster.route_once().await;
        cluster.route_once().await;
        assert!(
            cluster.nodes[&follower].metrics().phase == Phase::Prepare,
            "the follower should have promised again"
        );

        cluster.deliver(stale).await;
        assert_eq!(
            cluster.nodes[&follower].get_accepted_idx(),
            3,
            "a stale AcceptSync must not be applied"
        );
        assert!(
            cluster.nodes[&follower].metrics().phase == Phase::Recover,
            "a refused AcceptSync sends the follower back to recovery"
        );
        assert!(
            cluster.outboxes[&follower].iter().any(|msg| matches!(
                msg,
                Message::SequencePaxos(PaxosMessage {
                    to,
                    msg: PaxosMsg::PrepareReq(_),
                    ..
                }) if *to == successor
            )),
            "the follower should ask the leader to prepare it again"
        );

        cluster.heal_and_converge(300).await;
        cluster.assert_converged();
        assert_eq!(cluster.nodes[&follower].get_decided_idx(), 6);
        assert_eq!(cluster.decided_log(), vec![1, 2, 3, 4, 5, 6]);
    });
}

/// The StopSign's slot is acknowledged only once the whole log before it is
/// reported durable.
#[test]
fn stopsign_slot_acked_only_after_prefix_durable() {
    smol::block_on(async {
        let mut cluster = Cluster::new(vec![1, 2, 3], test_seed(31)).await;
        let leader = cluster.elect_leader().await;
        let (follower, _) = cluster.others(leader);
        let durable = cluster.nodes[&follower].get_durable_accepted_idx();
        cluster.set_flush(follower, Flush::Hold);
        cluster.take_accepted(follower);

        for id in 1..=3 {
            cluster.propose(leader, id).await;
        }
        cluster.route_once().await;
        cluster.reconfigure(leader, vec![1, 2, 3]).await;
        cluster.route_once().await;
        assert_eq!(cluster.nodes[&follower].get_accepted_idx(), durable + 4);
        assert!(
            cluster.take_accepted(follower).is_empty(),
            "nothing is acknowledged before the follower reports durability"
        );

        cluster.report(follower, durable + 2).await;
        assert_eq!(
            cluster.take_accepted(follower),
            vec![durable + 2],
            "a partial durable prefix must not cover the StopSign slot"
        );
        cluster.report(follower, durable + 3).await;
        assert_eq!(cluster.take_accepted(follower), vec![durable + 4]);
    });
}

// ===========================================================================
// Snapshot carried by a Promise
// ===========================================================================

/// The snapshot a node holds below its compacted index, as sorted entry ids.
async fn snapshot_ids(node: &Node) -> Vec<u64> {
    match node.read(0).await.unwrap() {
        Some(LogEntry::Snapshotted(entry)) => {
            let mut ids: Vec<u64> = entry.snapshot.map.keys().copied().collect();
            ids.sort_unstable();
            ids
        }
        other => panic!("expected a snapshot at index 0, got {other:?}"),
    }
}

/// A new leader whose log lags adopts a more up-to-date promiser's log
/// through a delta snapshot (entry 4 was decided as 15 in a later round than
/// the leader's own stale 7). It must keep that snapshot, merged with its own
/// decided entries below the delta: when it then syncs a follower that still
/// holds the stale 7 at index 4, the sync has to carry the snapshot. Before
/// the fix the leader trimmed to the delta's end without storing a snapshot,
/// sent the follower a sync with no snapshot at the compacted index, and the
/// follower decided its own stale 7 at index 4.
async fn delta_snapshot_adopted_by_leader_is_kept_for_later_syncs(durable_on_return: bool) {
    let mut cluster =
        Cluster::with_durability(vec![1, 2, 3], test_seed(47), durable_on_return).await;
    let ballot = |n, pid| Ballot {
        config_id: 1,
        n,
        priority: 0,
        pid,
    };
    let (stale_round, later_round) = (ballot(1, 3), ballot(2, 1));
    let image = |round: Ballot, ids: &[u64], decided_idx: u64| Image {
        log: ids.iter().map(|&id| Value::with_id(id)).collect(),
        promise: Some(later_round),
        accepted_round: Some(round),
        decided_idx,
        ..Default::default()
    };
    let images = [
        (1, image(later_round, &[1, 2, 3, 4, 15], 5)),
        (2, image(stale_round, &[1, 2, 3, 4, 7], 4)),
        (3, image(stale_round, &[1, 2, 3, 4, 7], 4)),
    ];
    for (pid, image) in images {
        cluster.crash(pid);
        cluster.crashed.insert(pid, image);
    }
    for pid in [1, 2, 3] {
        cluster.recover(pid).await;
    }
    cluster.observe_decided().await;
    assert_eq!(cluster.decided_log(), vec![1, 2, 3, 4, 15]);

    // Node 2 prepares: node 1's Promise carries the delta {15} for [4, 5)
    // and completes the majority; node 3's Promise arrives after, and its
    // AcceptSync is built from the leader's compacted state.
    cluster.try_become_leader(2).await;
    for _ in 0..6 {
        cluster.route_once().await;
        cluster.report_durability().await;
        cluster.observe_decided().await;
    }
    assert_eq!(cluster.nodes[&2].get_current_leader(), Some((2, true)));
    assert_eq!(cluster.nodes[&2].get_compacted_idx(), 5);
    assert_eq!(snapshot_ids(&cluster.nodes[&2]).await, vec![1, 2, 3, 4, 15]);
    assert_eq!(cluster.nodes[&3].get_decided_idx(), 5);
    assert_eq!(cluster.nodes[&3].get_compacted_idx(), 5);
    assert_eq!(snapshot_ids(&cluster.nodes[&3]).await, vec![1, 2, 3, 4, 15]);

    cluster.propose(2, 16).await;
    cluster.run(50).await;
    cluster.assert_converged();
    assert_eq!(cluster.decided_log(), vec![1, 2, 3, 4, 15, 16]);
}

#[test]
fn delta_snapshot_adopted_by_leader_is_kept_for_later_syncs_async() {
    smol::block_on(delta_snapshot_adopted_by_leader_is_kept_for_later_syncs(
        false,
    ));
}

#[test]
fn delta_snapshot_adopted_by_leader_is_kept_for_later_syncs_durable_on_return() {
    smol::block_on(delta_snapshot_adopted_by_leader_is_kept_for_later_syncs(
        true,
    ));
}

// ===========================================================================
// Duplicate Promise: a second AcceptSync with a newer session
// ===========================================================================

impl Cluster {
    /// Whether `pid` has queued a PrepareReq, i.e. asked to be prepared again.
    fn asks_for_prepare(&self, pid: u64) -> bool {
        self.outboxes[&pid].iter().any(|msg| {
            matches!(
                msg,
                Message::SequencePaxos(PaxosMessage {
                    msg: PaxosMsg::PrepareReq(_),
                    ..
                })
            )
        })
    }

    /// Make `follower` ask `leader` to prepare it twice, so the leader sends
    /// two Prepares and the follower answers both with identical Promises.
    async fn queue_duplicate_promises(&mut self, leader: u64, follower: u64) {
        for _ in 0..2 {
            self.nodes.get_mut(&follower).unwrap().reconnected(leader);
        }
        self.drain(follower);
        // The leader answers both PrepareReqs, then the follower both Prepares.
        self.route_once().await;
        self.route_once().await;
        let promises = self.outboxes[&follower]
            .iter()
            .filter(|msg| {
                matches!(
                    msg,
                    Message::SequencePaxos(PaxosMessage {
                        msg: PaxosMsg::Promise(_),
                        ..
                    })
                )
            })
            .count();
        assert_eq!(promises, 2, "the follower should answer both Prepares");
    }
}

/// Both Promises reach the leader, which answers each with an AcceptSync of
/// its own session. The follower applies the first and adopts the second's
/// session, so the leader's next AcceptDecide is in sequence: the follower
/// never asks to be prepared again and the proposal is decided.
async fn duplicate_promise_adopts_newer_accept_sync_session(durable_on_return: bool) {
    let mut cluster =
        Cluster::with_durability(vec![1, 2, 3], test_seed(53), durable_on_return).await;
    let leader = cluster.elect_leader().await;
    let (follower, _) = cluster.others(leader);
    for id in 1..=2 {
        cluster.propose(leader, id).await;
    }
    cluster.run(20).await;

    cluster.queue_duplicate_promises(leader, follower).await;
    // The leader sends AcceptSync sessions s and s+1; the follower applies
    // the first and adopts the second.
    cluster.route_once().await;
    cluster.route_once().await;
    assert!(cluster.nodes[&follower].metrics().phase == Phase::Accept);
    assert!(
        !cluster.asks_for_prepare(follower),
        "a second AcceptSync matching the follower's log must not restart the prepare"
    );

    cluster.propose(leader, 3).await;
    for _ in 0..6 {
        cluster.route_once().await;
        cluster.report_durability().await;
        cluster.observe_decided().await;
        assert!(
            !cluster.asks_for_prepare(follower),
            "the follower should accept AcceptDecides in the adopted session"
        );
    }
    assert_eq!(cluster.nodes[&follower].get_accepted_idx(), 3);
    for (pid, node) in &cluster.nodes {
        assert_eq!(node.get_decided_idx(), 3, "node {pid}");
    }
    assert_eq!(cluster.decided_log(), vec![1, 2, 3]);
}

#[test]
fn duplicate_promise_adopts_newer_accept_sync_session_async() {
    smol::block_on(duplicate_promise_adopts_newer_accept_sync_session(false));
}

#[test]
fn duplicate_promise_adopts_newer_accept_sync_session_durable_on_return() {
    smol::block_on(duplicate_promise_adopts_newer_accept_sync_session(true));
}

/// The second AcceptSync carries an entry whose AcceptDecide the follower
/// never received, so it ends past the follower's log: the follower must not
/// adopt its session but ask to be prepared again, and then catches up.
async fn newer_accept_sync_past_follower_log_restarts_prepare(durable_on_return: bool) {
    let mut cluster =
        Cluster::with_durability(vec![1, 2, 3], test_seed(59), durable_on_return).await;
    let leader = cluster.elect_leader().await;
    let (follower, _) = cluster.others(leader);
    for id in 1..=2 {
        cluster.propose(leader, id).await;
    }
    cluster.run(20).await;

    cluster.queue_duplicate_promises(leader, follower).await;
    // Hold back the second Promise so the leader appends an entry between
    // the two AcceptSyncs, and lose that entry's AcceptDecide to the follower.
    let held: Rc<RefCell<Vec<Message<Value>>>> = Rc::new(RefCell::new(Vec::new()));
    let sink = held.clone();
    let promises_seen = Rc::new(RefCell::new(0));
    let seen = promises_seen.clone();
    let hold = cluster.add_filter(Box::new(move |msg| {
        let is_promise = matches!(
            msg,
            Message::SequencePaxos(PaxosMessage {
                from,
                msg: PaxosMsg::Promise(_),
                ..
            }) if *from == follower
        );
        if !is_promise {
            return true;
        }
        *seen.borrow_mut() += 1;
        if *seen.borrow() == 2 {
            sink.borrow_mut().push(msg.clone());
            return false;
        }
        true
    }));
    cluster.route_once().await;
    cluster.remove_filter(hold);
    assert_eq!(held.borrow().len(), 1);
    let lose_accept_decide = cluster.add_filter(Box::new(move |msg| {
        !matches!(
            msg,
            Message::SequencePaxos(PaxosMessage {
                to,
                msg: PaxosMsg::AcceptDecide(_),
                ..
            }) if *to == follower
        )
    }));
    cluster.route_once().await;
    assert!(cluster.nodes[&follower].metrics().phase == Phase::Accept);
    cluster.propose(leader, 3).await;
    cluster.route_once().await;
    cluster.remove_filter(lose_accept_decide);
    assert_eq!(cluster.nodes[&follower].get_accepted_idx(), 2);

    let second_promise = held.borrow_mut().remove(0);
    cluster.deliver(second_promise).await;
    cluster.route_once().await;
    assert!(
        cluster.asks_for_prepare(follower),
        "an AcceptSync ending past the follower's log must restart the prepare"
    );
    assert!(cluster.nodes[&follower].metrics().phase == Phase::Recover);
    assert_eq!(cluster.nodes[&follower].get_accepted_idx(), 2);

    cluster.run(150).await;
    cluster.assert_converged();
    assert_eq!(cluster.nodes[&follower].get_decided_idx(), 3);
    assert_eq!(cluster.decided_log(), vec![1, 2, 3]);
}

#[test]
fn newer_accept_sync_past_follower_log_restarts_prepare_async() {
    smol::block_on(newer_accept_sync_past_follower_log_restarts_prepare(false));
}

#[test]
fn newer_accept_sync_past_follower_log_restarts_prepare_durable_on_return() {
    smol::block_on(newer_accept_sync_past_follower_log_restarts_prepare(true));
}

// ===========================================================================
// F9: existing stress and reconfiguration scenarios on reference storage,
// flushing every write each round
// ===========================================================================

#[test]
fn reference_storage_stress_random_crashes_500() {
    smol::block_on(async {
        let nodes = vec![1, 2, 3, 4, 5, 6, 7];
        let mut cluster = Cluster::new(nodes.clone(), test_seed(42)).await;
        for proposal_id in (1u64..).take(500) {
            let target = nodes[cluster.rng.next_range(nodes.len() as u64) as usize];
            if cluster.is_crashed(target) {
                cluster.recover(target).await;
            } else if cluster.live_nodes().len() > 4 {
                cluster.crash(target);
            }
            let live = cluster.live_nodes();
            let proposer = live[cluster.rng.next_range(live.len() as u64) as usize];
            cluster.propose(proposer, proposal_id).await;
            cluster.run(5).await;
        }
        cluster.heal_and_converge(500).await;
        cluster.assert_converged();
    });
}

async fn stress_random_partitions_500(durable_on_return: bool) {
    let nodes = vec![1, 2, 3, 4, 5];
    let mut cluster =
        Cluster::with_durability(nodes.clone(), test_seed(123), durable_on_return).await;
    let mut active_filter: Option<usize> = None;
    for proposal_id in (1u64..).take(500) {
        if cluster.rng.next_range(3) == 0 {
            match active_filter.take() {
                Some(filter) => {
                    cluster.remove_filter(filter);
                    cluster.reconnect_all();
                }
                None => {
                    let split = (cluster.rng.next_range(3) + 1) as usize;
                    active_filter =
                        Some(cluster.partition(nodes[..split].to_vec(), nodes[split..].to_vec()));
                }
            }
        }
        let live = cluster.live_nodes();
        let proposer = live[cluster.rng.next_range(live.len() as u64) as usize];
        cluster.propose(proposer, proposal_id).await;
        cluster.run(3).await;
    }
    cluster.heal_and_converge(500).await;
    cluster.assert_converged();
}

#[test]
fn reference_storage_stress_random_partitions_500() {
    smol::block_on(stress_random_partitions_500(false));
}

/// The same run with every write durable on return: the engine's synchronous
/// durability mode under this harness.
#[test]
fn durable_on_return_stress_random_partitions_500() {
    smol::block_on(stress_random_partitions_500(true));
}

#[test]
fn reference_storage_reconfigure_add_nodes() {
    smol::block_on(async {
        let mut cluster = Cluster::new(vec![1, 2, 3], test_seed(37)).await;
        let leader = cluster.elect_leader().await;
        for id in 1..=5 {
            cluster.propose(leader, id).await;
        }
        cluster.run(100).await;
        cluster.reconfigure(leader, vec![1, 2, 3, 4, 5]).await;
        cluster.run(100).await;

        for (pid, node) in &cluster.nodes {
            let ss = node.is_reconfigured();
            assert!(
                ss.is_some(),
                "node {pid} should see the reconfiguration decided"
            );
            assert_eq!(ss.unwrap().next_config.nodes, vec![1, 2, 3, 4, 5]);
            assert_eq!(node.get_decided_idx(), 6, "node {pid}");
        }
        assert_eq!(cluster.decided_log(), vec![1, 2, 3, 4, 5]);
    });
}

#[test]
fn reference_storage_leader_crash_during_reconfig() {
    smol::block_on(async {
        let mut cluster = Cluster::new(vec![1, 2, 3], test_seed(41)).await;
        let leader = cluster.elect_leader().await;
        for id in 1..=3 {
            cluster.propose(leader, id).await;
        }
        cluster.run(30).await;
        cluster.reconfigure(leader, vec![1, 2, 3, 4]).await;
        cluster.run(1).await;
        cluster.crash(leader);
        cluster.run(500).await;
        cluster.heal_and_converge(500).await;
        cluster.assert_converged();
        assert_eq!(&cluster.decided_log()[..3], &[1, 2, 3]);
    });
}

#[test]
fn reference_storage_reconfig_with_concurrent_crash_recovery() {
    smol::block_on(async {
        let mut cluster = Cluster::new(vec![1, 2, 3, 4, 5], test_seed(43)).await;
        let leader = cluster.elect_leader().await;
        let follower = if leader == 1 { 2 } else { 1 };
        cluster.crash(follower);
        cluster.reconfigure(leader, vec![1, 2, 3, 4, 5, 6]).await;
        cluster.run(200).await;
        cluster.recover(follower).await;
        cluster.run(500).await;
        cluster.assert_converged();
        for (pid, node) in &cluster.nodes {
            assert!(
                node.is_reconfigured().is_some(),
                "node {pid} should see the reconfiguration decided"
            );
        }
    });
}
