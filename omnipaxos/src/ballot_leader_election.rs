use std::cmp::Ordering;

/// Ballot Leader Election algorithm for electing new leaders
use crate::{
    engine::state::{Phase, Role},
    util::{defaults::*, ConfigurationId, FlexibleQuorum, Quorum},
};

use crate::{
    messages::ballot_leader_election::{
        BLEMessage, HeartbeatMsg, HeartbeatReply, HeartbeatRequest,
    },
    util::NodeId,
    OmniPaxosConfig,
};
#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};

/// Used to define a Sequence Paxos epoch
#[derive(Clone, Copy, Eq, Debug, Default, PartialEq)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub struct Ballot {
    /// The identifier for the configuration that the replica with this ballot is part of.
    pub config_id: ConfigurationId,
    /// Ballot number
    pub n: u32,
    /// Custom priority parameter
    pub priority: u32,
    /// The pid of the process
    pub pid: NodeId,
}

impl Ballot {
    /// Creates a new Ballot
    /// # Arguments
    /// * `config_id` - The identifier for the configuration that the replica with this ballot is part of.
    /// * `n` - Ballot number.
    /// * `pid` -  Used as tiebreaker for total ordering of ballots.
    pub fn with(config_id: ConfigurationId, n: u32, priority: u32, pid: NodeId) -> Ballot {
        Ballot {
            config_id,
            n,
            priority,
            pid,
        }
    }
}

impl Ord for Ballot {
    fn cmp(&self, other: &Self) -> Ordering {
        (self.config_id, self.n, self.priority, self.pid).cmp(&(
            other.config_id,
            other.n,
            other.priority,
            other.pid,
        ))
    }
}

impl PartialOrd for Ballot {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

const INITIAL_ROUND: u32 = 1;
const RECOVERY_ROUND: u32 = 0;

/// A Ballot Leader Election component. Used in conjunction with OmniPaxos to handle the election of a leader for a cluster of OmniPaxos servers,
/// incoming messages and produces outgoing messages that the user has to fetch periodically and send using a network implementation.
/// User also has to periodically fetch the decided entries that are guaranteed to be strongly consistent and linearizable, and therefore also safe to be used in the higher level application.
pub(crate) struct BallotLeaderElection {
    /// The identifier for the configuration that this instance is part of.
    configuration_id: ConfigurationId,
    /// Process identifier used to uniquely identify this instance.
    pid: NodeId,
    /// Vector that holds the pids of all the other servers.
    peers: Vec<NodeId>,
    /// The current round of the heartbeat cycle.
    hb_round: u32,
    /// The heartbeat replies this instance received during the current round.
    heartbeat_replies: Vec<HeartbeatReply>,
    /// Vector that holds all the received heartbeats from the previous heartbeat round, including the current node. Only used to display the connectivity of this node in the UI.
    /// Represents nodes that are currently alive from the view of the current node.
    prev_replies: Vec<HeartbeatReply>,
    /// Holds the current ballot of this instance.
    current_ballot: Ballot,
    /// The current leader of this instance.
    leader: Ballot,
    /// A happy node either sees that it is, is connected to, or sees evidence of a potential leader
    /// for the cluster. If a node is unhappy then it is seeking a new leader.
    happy: bool,
    /// Number of consecutive rounds this node has been unhappy. Takeover requires
    /// sustained unhappiness to avoid false positives from stale heartbeat data.
    consecutive_unhappy_rounds: u32,
    /// The number of replicas inside the cluster whose heartbeats are needed to become and remain the leader.
    quorum: Quorum,
    /// Maximum size of the outgoing message buffer.
    buffer_size: usize,
    /// Vector which holds all the outgoing messages of the BLE instance.
    outgoing: Vec<BLEMessage>,
}

impl BallotLeaderElection {
    /// Construct a new BallotLeaderElection node
    pub(crate) fn with(config: BLEConfig, recovered_leader: Option<Ballot>) -> Self {
        let config_id = config.configuration_id;
        let pid = config.pid;
        let peers = config.peers;
        let num_nodes = &peers.len() + 1;
        let quorum = Quorum::with(config.flexible_quorum, num_nodes);
        let mut initial_ballot = Ballot::with(config_id, INITIAL_ROUND, config.priority, pid);
        let initial_leader = match recovered_leader {
            Some(b) if b != Ballot::default() => {
                if peers.is_empty() {
                    // Single-node: no peers to recover from, skip recovery round.
                    // Set ballot higher than the recovered leader so we self-elect
                    // immediately on the next hb_timeout.
                    initial_ballot.n = b.n + 1;
                } else {
                    // Prevents a recovered server from retaining BLE leadership
                    // with the same ballot.
                    initial_ballot.n = RECOVERY_ROUND;
                }
                b
            }
            _ => initial_ballot,
        };
        let mut ble = BallotLeaderElection {
            configuration_id: config_id,
            pid,
            peers,
            hb_round: 0,
            heartbeat_replies: Vec::with_capacity(num_nodes),
            prev_replies: Vec::with_capacity(num_nodes),
            current_ballot: initial_ballot,
            leader: initial_leader,
            happy: true,
            consecutive_unhappy_rounds: 0,
            quorum,
            buffer_size: config.buffer_size,
            outgoing: Vec::with_capacity(config.buffer_size),
        };
        tracing::info!(pid, "Ballot Leader Election component created");
        ble.new_hb_round();
        ble
    }

    /// Try to push a BLE message to the outgoing buffer. Returns false if the
    /// buffer is full (message dropped).
    fn try_push(&mut self, msg: BLEMessage) -> bool {
        if self.outgoing.len() >= self.buffer_size {
            false
        } else {
            self.outgoing.push(msg);
            true
        }
    }

    /// Update the custom priority used in the Ballot for this server. Note that changing the
    /// priority triggers a leader re-election.
    pub(crate) fn set_priority(&mut self, p: u32) {
        self.current_ballot.priority = p;
    }

    /// Returns reference to outgoing messages
    pub(crate) fn outgoing_mut(&mut self) -> &mut Vec<BLEMessage> {
        &mut self.outgoing
    }

    /// Handle an incoming message.
    /// # Arguments
    /// * `m` - the message to be handled.
    pub(crate) fn handle(&mut self, m: BLEMessage) {
        match m.msg {
            HeartbeatMsg::Request(req) => self.handle_request(m.from, req),
            HeartbeatMsg::Reply(rep) => self.handle_reply(rep),
        }
    }

    /// Initiates a new heartbeat round.
    pub(crate) fn new_hb_round(&mut self) {
        self.prev_replies = std::mem::take(&mut self.heartbeat_replies);
        self.hb_round += 1;
        tracing::trace!(pid = self.pid, round = self.hb_round, "new_heartbeat_round");
        for i in 0..self.peers.len() {
            let peer = self.peers[i];
            let hb_request = HeartbeatRequest {
                round: self.hb_round,
            };
            self.try_push(BLEMessage {
                from: self.pid,
                to: peer,
                msg: HeartbeatMsg::Request(hb_request),
            });
        }
    }

    /// End of a heartbeat round. Returns current leader and election status.
    pub(crate) fn hb_timeout(
        &mut self,
        seq_paxos_state: &(Role, Phase),
        seq_paxos_promise: Ballot,
    ) -> Option<Ballot> {
        self.update_leader();
        self.update_happiness(seq_paxos_state);
        self.check_takeover();
        self.new_hb_round();
        if seq_paxos_promise > self.leader {
            // Sync leader with Paxos promise in case ballot didn't make it to BLE followers
            // or become_leader() was called.
            self.leader = seq_paxos_promise;
            if seq_paxos_promise.pid == self.pid {
                self.current_ballot = seq_paxos_promise;
            }
            self.happy = true;
        }
        if self.leader == self.current_ballot {
            Some(self.current_ballot)
        } else {
            None
        }
    }

    fn update_leader(&mut self) {
        let max_reply_ballot = self.heartbeat_replies.iter().map(|r| r.ballot).max();
        if let Some(max) = max_reply_ballot {
            if max > self.leader {
                self.leader = max;
                // New leader discovered — reset unhappy counter to give it time
                // to stabilize before any takeover attempt.
                self.consecutive_unhappy_rounds = 0;
            }
        }
    }

    fn update_happiness(&mut self, seq_paxos_state: &(Role, Phase)) {
        self.happy = if self.leader == self.current_ballot {
            let potential_followers = self
                .heartbeat_replies
                .iter()
                .filter(|hb_reply| hb_reply.leader <= self.current_ballot)
                .count();
            let can_form_quorum = match seq_paxos_state {
                (Role::Leader, Phase::Accept) => {
                    self.quorum.is_accept_quorum(potential_followers + 1)
                }
                _ => self.quorum.is_prepare_quorum(potential_followers + 1),
            };
            if can_form_quorum {
                true
            } else {
                let see_larger_happy_leader = self
                    .heartbeat_replies
                    .iter()
                    .any(|r| r.leader > self.current_ballot && r.happy);
                see_larger_happy_leader
            }
        } else {
            self.heartbeat_replies
                .iter()
                .any(|r| r.ballot == self.leader && r.happy)
        };
        // Track consecutive unhappy rounds. Takeover requires sustained
        // unhappiness to filter out transient unhappiness from stale heartbeat
        // data (previous-round replies may carry outdated happy flags).
        if !self.happy {
            self.consecutive_unhappy_rounds = self.consecutive_unhappy_rounds.saturating_add(1);
        } else {
            self.consecutive_unhappy_rounds = 0;
        }
    }

    fn check_takeover(&mut self) {
        // Require sustained unhappiness before attempting takeover. Previous-round
        // heartbeat replies may carry stale happy flags, so we need multiple rounds
        // to confirm the situation is genuine.
        if !self.happy && self.consecutive_unhappy_rounds >= 2 {
            // Check if the leader is directly reachable: look for a heartbeat reply
            // with the leader's ballot. If the leader is alive, it will respond to
            // our heartbeat requests. If it has crashed, we won't see its ballot.
            let leader_is_reachable = self.leader == self.current_ballot
                || self
                    .heartbeat_replies
                    .iter()
                    .any(|r| r.ballot == self.leader);
            // Check if any neighbor is happy with the current leader. If so, the
            // leader is likely alive and we are asymmetrically partitioned from it.
            // Attempting takeover in this case would cause a spurious election.
            let neighbor_happy_with_leader = self
                .heartbeat_replies
                .iter()
                .any(|r| r.leader == self.leader && r.happy);
            let im_quorum_connected = self
                .quorum
                .is_prepare_quorum(self.heartbeat_replies.len() + 1);
            if !leader_is_reachable && !neighbor_happy_with_leader && im_quorum_connected {
                // Randomized backoff: stagger takeover attempts across nodes to prevent
                // simultaneous leader elections. Use pid and hb_round as a deterministic
                // seed so that higher-priority / lower-pid nodes attempt takeover sooner.
                // The delay is in units of heartbeat rounds.
                let backoff_hash = (self.pid as u32)
                    .wrapping_mul(7)
                    .wrapping_add(self.hb_round);
                let delay_rounds = backoff_hash % (self.peers.len() as u32 + 1);
                if delay_rounds == 0 {
                    tracing::info!(
                        pid = self.pid,
                        old_leader_pid = self.leader.pid,
                        new_ballot_n = self.leader.n + 1,
                        consecutive_unhappy = self.consecutive_unhappy_rounds,
                        "leader_takeover"
                    );
                    self.current_ballot.n = self.leader.n + 1;
                    self.leader = self.current_ballot;
                    self.happy = true;
                    self.consecutive_unhappy_rounds = 0;
                }
                // else: wait for a future round (delay_rounds != 0 means skip this round)
            }
        }
    }

    fn handle_request(&mut self, from: NodeId, req: HeartbeatRequest) {
        let hb_reply = HeartbeatReply {
            round: req.round,
            ballot: self.current_ballot,
            leader: self.leader,
            happy: self.happy,
        };
        self.try_push(BLEMessage {
            from: self.pid,
            to: from,
            msg: HeartbeatMsg::Reply(hb_reply),
        });
    }

    fn handle_reply(&mut self, rep: HeartbeatReply) {
        // Accept replies from the current round OR the previous round. Due to
        // the tick-route-tick-route pattern, replies to round N's requests
        // typically arrive after round N+1 has started. Without accepting
        // previous-round replies, BLE heartbeats are non-functional at
        // election_tick_timeout=1.
        let is_current = rep.round == self.hb_round;
        let is_previous = self.hb_round > 0 && rep.round == self.hb_round - 1;
        if (is_current || is_previous) && rep.ballot.config_id == self.configuration_id {
            // Previous-round replies have stale `happy` flags: the sender's
            // happiness may have changed since then. Override to false so that
            // stale happiness doesn't mask a leader failure. Ballot/leader
            // fields remain valid for leader discovery.
            let adjusted = if is_previous {
                HeartbeatReply {
                    happy: false,
                    ..rep
                }
            } else {
                rep
            };
            self.heartbeat_replies.push(adjusted);
        }
    }

    pub(crate) fn get_current_ballot(&self) -> Ballot {
        self.current_ballot
    }

    pub(crate) fn set_current_ballot(&mut self, ballot: Ballot) {
        self.current_ballot = ballot;
    }

    pub(crate) fn set_leader(&mut self, ballot: Ballot) {
        self.leader = ballot;
    }

    /// Returns the current heartbeat round number.
    pub(crate) fn get_hb_round(&self) -> u32 {
        self.hb_round
    }

}

/// Configuration for `BallotLeaderElection`.
/// # Fields
/// * `configuration_id`: The identifier for the configuration that this node is part of.
/// * `pid`: The unique identifier of this node. Must not be 0.
/// * `peers`: The peers of this node i.e. the `pid`s of the other servers in the configuration.
/// * `priority`: Set custom priority for this node to be elected as the leader.
/// * `flexible_quorum` : Defines read and write quorum sizes. Can be used for different latency vs fault tolerance tradeoffs.
/// * `buffer_size`: The buffer size for outgoing messages.
#[derive(Clone, Debug)]
pub(crate) struct BLEConfig {
    configuration_id: ConfigurationId,
    pid: NodeId,
    peers: Vec<NodeId>,
    priority: u32,
    flexible_quorum: Option<FlexibleQuorum>,
    buffer_size: usize,
}

impl From<OmniPaxosConfig> for BLEConfig {
    fn from(config: OmniPaxosConfig) -> Self {
        let pid = config.server_config.pid;
        let peers = config
            .cluster_config
            .nodes
            .into_iter()
            .filter(|x| *x != pid)
            .collect();

        Self {
            configuration_id: config.cluster_config.configuration_id,
            pid,
            peers,
            priority: config.server_config.leader_priority,
            flexible_quorum: config.cluster_config.flexible_quorum,
            buffer_size: BLE_BUFFER_SIZE,
        }
    }
}
