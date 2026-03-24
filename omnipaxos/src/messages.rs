use crate::{
    messages::{ballot_leader_election::BLEMessage, sequence_paxos::PaxosMessage},
    storage::Entry,
    util::NodeId,
};
#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};

/// Opaque reference to pre-staged payload bytes.
///
/// Used by the RDMA data plane to disseminate bulk entry data before the
/// consensus control message arrives. The transport layer maps this to
/// physical RDMA addresses internally — the consensus engine only sees
/// identity and validation fields.
#[derive(Clone, Debug, PartialEq)]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub struct PayloadRef {
    /// Leader's ballot epoch — invalid after leader change.
    pub leader_epoch: u64,
    /// Connection incarnation — invalid after reconnect/QP reset.
    pub conn_epoch: u64,
    /// Monotonic within (leader_epoch, conn_epoch).
    pub batch_id: u64,
    /// Payload byte length.
    pub len: u32,
    /// Number of entries (for AcceptDecide) or 0 for opaque blobs.
    pub entry_count: u32,
    /// CRC32C of staged bytes.
    pub crc: u32,
}

/// Entry data is either inline or referenced via a [`PayloadRef`].
///
/// The RDMA data plane stages bulk entry bytes on the remote follower before
/// the consensus control message arrives. The pump layer resolves `Ref`
/// variants to `Inline` before the engine processes them. On the send side,
/// the pump converts `Inline` to `Ref` after staging via the data transport.
#[derive(Clone, Debug)]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub enum InlineOrRef<T> {
    /// Payload data is carried inline in the message.
    Inline(T),
    /// Payload data was pre-staged via the RDMA data plane.
    Ref(PayloadRef),
}

impl<T> InlineOrRef<T> {
    /// Returns the inline value, panicking if this is a `Ref`.
    ///
    /// The pump layer must resolve all `Ref` variants before the engine
    /// processes messages. If this panics, the pump failed to resolve.
    pub fn into_inner(self) -> T {
        match self {
            InlineOrRef::Inline(v) => v,
            InlineOrRef::Ref(_) => panic!("unresolved PayloadRef reached engine"),
        }
    }

    /// Returns a reference to the inline value, or `None` if `Ref`.
    pub fn as_inline(&self) -> Option<&T> {
        match self {
            InlineOrRef::Inline(v) => Some(v),
            InlineOrRef::Ref(_) => None,
        }
    }

    /// Returns true if this is an inline value.
    pub fn is_inline(&self) -> bool {
        matches!(self, InlineOrRef::Inline(_))
    }
}

/// Internal component for log replication
pub mod sequence_paxos {
    use crate::{
        ballot_leader_election::Ballot,
        messages::InlineOrRef,
        storage::{Entry, StopSign},
        util::{LogSync, NodeId, SequenceNumber},
    };
    #[cfg(feature = "serde")]
    use serde::{Deserialize, Serialize};
    use std::fmt::Debug;
    use std::sync::Arc;

    /// Message sent by a follower on crash-recovery or dropped messages to request its leader to re-prepare them.
    #[derive(Copy, Clone, Debug)]
    #[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
    #[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
    pub struct PrepareReq {
        /// The current round.
        pub n: Ballot,
    }

    /// Prepare message sent by a newly-elected leader to initiate the Prepare phase.
    #[derive(Copy, Clone, Debug)]
    #[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
    #[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
    pub struct Prepare {
        /// The current round.
        pub n: Ballot,
        /// The decided index of this leader.
        pub decided_idx: u64,
        /// The latest round in which an entry was accepted.
        pub n_accepted: Ballot,
        /// The log length of this leader.
        pub accepted_idx: u64,
    }

    /// Promise message sent by a follower in response to a [`Prepare`] sent by the leader.
    #[derive(Clone, Debug)]
    #[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
    #[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
    pub struct Promise<T>
    where
        T: Entry,
    {
        /// The current round.
        pub n: Ballot,
        /// The latest round in which an entry was accepted.
        pub n_accepted: Ballot,
        /// The decided index of this follower.
        pub decided_idx: u64,
        /// The log length of this follower.
        pub accepted_idx: u64,
        /// The log update which the leader applies to its log in order to sync
        /// with this follower (if the follower is more up-to-date).
        /// May be a `Ref` if the payload was staged via RDMA data plane.
        pub log_sync: Option<InlineOrRef<LogSync<T>>>,
    }

    /// AcceptSync message sent by the leader to synchronize the logs of all replicas in the prepare phase.
    #[derive(Clone, Debug)]
    #[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
    #[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
    pub struct AcceptSync<T>
    where
        T: Entry,
    {
        /// The current round.
        pub n: Ballot,
        /// The sequence number of this message in the leader-to-follower accept sequence
        pub seq_num: SequenceNumber,
        /// The decided index
        pub decided_idx: u64,
        /// The log update which the follower applies to its log in order to sync
        /// with the leader. May be a `Ref` if staged via RDMA data plane.
        pub log_sync: InlineOrRef<LogSync<T>>,
    }

    /// Message with entries to be replicated and the latest decided index sent by the leader in the accept phase.
    #[derive(Clone, Debug)]
    #[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
    #[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
    pub struct AcceptDecide<T>
    where
        T: Entry,
    {
        /// The current round.
        pub n: Ballot,
        /// The sequence number of this message in the leader-to-follower accept sequence
        pub seq_num: SequenceNumber,
        /// The decided index.
        pub decided_idx: u64,
        /// Entries to be replicated. May be a `Ref` if staged via RDMA data plane.
        pub entries: InlineOrRef<Arc<Vec<T>>>,
    }

    /// Message sent by follower to leader when entries has been accepted.
    #[derive(Copy, Clone, Debug)]
    #[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
    #[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
    pub struct Accepted {
        /// The current round.
        pub n: Ballot,
        /// The accepted index.
        pub accepted_idx: u64,
    }

    /// Message sent by leader to followers to decide up to a certain index in the log.
    #[derive(Copy, Clone, Debug)]
    #[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
    #[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
    pub struct Decide {
        /// The current round.
        pub n: Ballot,
        /// The sequence number of this message in the leader-to-follower accept sequence
        pub seq_num: SequenceNumber,
        /// The decided index.
        pub decided_idx: u64,
    }

    /// Message sent by leader to followers to accept a StopSign
    #[derive(Clone, Debug)]
    #[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
    #[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
    pub struct AcceptStopSign {
        /// The current round.
        pub n: Ballot,
        /// The sequence number of this message in the leader-to-follower accept sequence
        pub seq_num: SequenceNumber,
        /// The decided index.
        pub ss: StopSign,
    }

    /// Message sent by follower to leader when accepting an entry is rejected.
    /// This happens when the follower is promised to a greater leader.
    #[derive(Clone, Debug)]
    #[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
    #[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
    pub struct NotAccepted {
        /// The follower's current ballot
        pub n: Ballot,
    }

    /// Compaction Request
    #[allow(missing_docs)]
    #[derive(Clone, Debug)]
    #[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
    #[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
    pub enum Compaction {
        Trim(u64),
        Snapshot(Option<u64>),
    }

    /// An enum for all the different message types.
    #[allow(missing_docs)]
    #[derive(Clone, Debug)]
    #[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
    #[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
    pub enum PaxosMsg<T>
    where
        T: Entry,
    {
        /// Request a [`Prepare`] to be sent from the leader. Used for fail-recovery.
        PrepareReq(PrepareReq),
        #[allow(missing_docs)]
        Prepare(Prepare),
        Promise(Promise<T>),
        AcceptSync(AcceptSync<T>),
        AcceptDecide(AcceptDecide<T>),
        Accepted(Accepted),
        NotAccepted(NotAccepted),
        Decide(Decide),
        /// Forward client proposals to the leader.
        ProposalForward(Vec<T>),
        Compaction(Compaction),
        AcceptStopSign(AcceptStopSign),
        ForwardStopSign(StopSign),
        /// Leader transfer: current leader asks target to take over.
        TransferLeader(TransferLeader),
        /// Leader asks followers to confirm its authority for linearizable reads.
        ReadIndexReq(ReadIndexReq),
        /// Follower confirms (or denies) leader authority for linearizable reads.
        ReadIndexResp(ReadIndexResp),
    }

    /// Leader sends this to confirm its authority for linearizable reads.
    #[derive(Copy, Clone, Debug)]
    #[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
    #[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
    pub struct ReadIndexReq {
        /// The leader's current ballot.
        pub n: Ballot,
        /// Unique ID for this read-index request.
        pub read_id: u64,
    }

    /// Follower response confirming or denying leader authority.
    #[derive(Copy, Clone, Debug)]
    #[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
    #[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
    pub struct ReadIndexResp {
        /// The leader's ballot from the request.
        pub n: Ballot,
        /// The read-index request ID.
        pub read_id: u64,
        /// True if the follower's promise matches the leader's ballot.
        pub ok: bool,
    }

    /// Message sent by a leader to a target node to initiate leadership transfer.
    #[derive(Clone, Debug)]
    #[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
    #[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
    pub struct TransferLeader {
        /// The current leader's ballot.
        pub n: Ballot,
        /// Suggested ballot number for the target to use (higher than leader's).
        pub suggested_n: u32,
    }

    /// A struct for a Paxos message that also includes sender and receiver.
    #[derive(Clone, Debug)]
    #[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
    #[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
    pub struct PaxosMessage<T>
    where
        T: Entry,
    {
        /// Sender of `msg`.
        pub from: NodeId,
        /// Receiver of `msg`.
        pub to: NodeId,
        /// The message content.
        pub msg: PaxosMsg<T>,
    }
}

/// The different messages BLE uses to communicate with other servers.
pub mod ballot_leader_election {

    use crate::{ballot_leader_election::Ballot, util::NodeId};
    #[cfg(feature = "serde")]
    use serde::{Deserialize, Serialize};

    /// An enum for all the different BLE message types.
    #[allow(missing_docs)]
    #[derive(Clone, Debug)]
    #[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
    #[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
    pub enum HeartbeatMsg {
        Request(HeartbeatRequest),
        Reply(HeartbeatReply),
    }

    /// Requests a reply from all the other servers.
    #[derive(Clone, Debug)]
    #[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
    #[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
    pub struct HeartbeatRequest {
        /// Number of the current round.
        pub round: u32,
    }

    /// Replies
    #[derive(Clone, Debug)]
    #[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
    #[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
    pub struct HeartbeatReply {
        /// Number of the current heartbeat round.
        pub round: u32,
        /// Ballot of replying server.
        pub ballot: Ballot,
        /// Leader this server is following
        pub leader: Ballot,
        /// Whether the replying server sees a need for a new leader
        pub happy: bool,
    }

    /// A struct for a Paxos message that also includes sender and receiver.
    #[derive(Clone, Debug)]
    #[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
    #[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
    pub struct BLEMessage {
        /// Sender of `msg`.
        pub from: NodeId,
        /// Receiver of `msg`.
        pub to: NodeId,
        /// The message content.
        pub msg: HeartbeatMsg,
    }
}

#[allow(missing_docs)]
/// Message in OmniPaxos. Can be either a `SequencePaxos` message (for log replication) or `BLE` message (for leader election)
#[derive(Clone, Debug)]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub enum Message<T>
where
    T: Entry,
{
    SequencePaxos(PaxosMessage<T>),
    BLE(BLEMessage),
}

impl<T> Message<T>
where
    T: Entry,
{
    /// Get the sender id of the message
    pub fn get_sender(&self) -> NodeId {
        match self {
            Message::SequencePaxos(p) => p.from,
            Message::BLE(b) => b.from,
        }
    }

    /// Get the receiver id of the message
    pub fn get_receiver(&self) -> NodeId {
        match self {
            Message::SequencePaxos(p) => p.to,
            Message::BLE(b) => b.to,
        }
    }
}
