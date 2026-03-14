/// An in-memory storage implementation for OmniPaxos.
pub mod memory_storage;

use super::ballot_leader_election::Ballot;
pub use crate::errors::{AnyError, StorageError, StorageOperation};
use crate::ClusterConfig;
#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};
use std::fmt::Debug;

/// Type of the entries stored in the log.
pub trait Entry: Clone + Debug {
    #[cfg(not(feature = "serde"))]
    /// The snapshot type for this entry type.
    type Snapshot: Snapshot<Self>;

    #[cfg(feature = "serde")]
    /// The snapshot type for this entry type.
    type Snapshot: Snapshot<Self> + Serialize + for<'a> Deserialize<'a>;
}

/// A StopSign entry that marks the end of a configuration. Used for reconfiguration.
#[derive(Clone, Debug, PartialEq)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub struct StopSign {
    /// The new `Omnipaxos` cluster configuration
    pub next_config: ClusterConfig,
    /// Metadata for the reconfiguration.
    pub metadata: Option<Vec<u8>>,
}

impl StopSign {
    /// Creates a [`StopSign`].
    pub fn with(next_config: ClusterConfig, metadata: Option<Vec<u8>>) -> Self {
        StopSign {
            next_config,
            metadata,
        }
    }
}

/// Snapshot type. A `Complete` snapshot contains all snapshotted data while `Delta` has snapshotted changes since an earlier snapshot.
#[allow(missing_docs)]
#[derive(Clone, Debug)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub enum SnapshotType<T>
where
    T: Entry,
{
    Complete(T::Snapshot),
    Delta(T::Snapshot),
}

/// Trait for implementing snapshot operations for log entries of type `T` in OmniPaxos.
pub trait Snapshot<T>: Clone + Debug
where
    T: Entry,
{
    /// Create a snapshot from the log `entries`.
    fn create(entries: &[T]) -> Self;

    /// Merge another snapshot `delta` into self.
    fn merge(&mut self, delta: Self);

    /// Whether `T` is snapshottable. If not, simply return `false` and leave the other functions `unimplemented!()`.
    fn use_snapshots() -> bool;
}

/// The Result type returned by the storage API.
///
/// Storage trait methods return `AnyError` — the Runtime wraps these into
/// `StorageError` (with operation context) as they propagate up.
pub type StorageResult<T> = Result<T, AnyError>;

/// The write operations of the storage implementation.
#[derive(Debug)]
pub enum StorageOp<T: Entry> {
    /// Appends an entry to the end of the log.
    AppendEntry(T),
    /// Appends entries to the end of the log.
    AppendEntries(Vec<T>),
    /// Appends entries to the log from the prefix specified by the given index.
    AppendOnPrefix(usize, Vec<T>),
    /// Sets the round that has been promised.
    SetPromise(Ballot),
    /// Sets the decided index in the log.
    SetDecidedIndex(usize),
    /// Sets the latest accepted round.
    SetAcceptedRound(Ballot),
    /// Sets the compacted (i.e. trimmed or snapshotted) index.
    SetCompactedIdx(usize),
    /// Removes elements up to the given idx from storage.
    Trim(usize),
    /// Sets the StopSign used for reconfiguration.
    SetStopsign(Option<StopSign>),
    /// Sets the snapshot.
    SetSnapshot(Option<T::Snapshot>),
}

impl<T: Entry> StorageOp<T> {
    /// Returns a short description of this operation for error diagnostics.
    pub fn describe(&self) -> &'static str {
        match self {
            StorageOp::AppendEntry(_) => "AppendEntry",
            StorageOp::AppendEntries(_) => "AppendEntries",
            StorageOp::AppendOnPrefix(..) => "AppendOnPrefix",
            StorageOp::SetPromise(_) => "SetPromise",
            StorageOp::SetDecidedIndex(_) => "SetDecidedIndex",
            StorageOp::SetAcceptedRound(_) => "SetAcceptedRound",
            StorageOp::SetCompactedIdx(_) => "SetCompactedIdx",
            StorageOp::Trim(_) => "Trim",
            StorageOp::SetStopsign(_) => "SetStopsign",
            StorageOp::SetSnapshot(_) => "SetSnapshot",
        }
    }
}

/// Describe all operations in a batch for error diagnostics.
pub(crate) fn describe_batch<T: Entry>(ops: &[StorageOp<T>]) -> String {
    ops.iter().map(|op| op.describe()).collect::<Vec<_>>().join(", ")
}

/// All persisted scalar state, loaded once during recovery via [`Storage::load_state`].
#[derive(Debug)]
pub struct PersistedState<T: Entry> {
    /// The promised ballot.
    pub promise: Option<Ballot>,
    /// The latest accepted round.
    pub accepted_round: Option<Ballot>,
    /// The decided log index.
    pub decided_idx: usize,
    /// The compacted (trimmed/snapshotted) index.
    pub compacted_idx: usize,
    /// The stopsign for reconfiguration.
    pub stopsign: Option<StopSign>,
    /// Log length (excluding compacted entries).
    pub log_len: usize,
    /// The stored snapshot.
    pub snapshot: Option<T::Snapshot>,
}

/// Trait for implementing the storage backend of OmniPaxos.
///
/// All mutations go through [`write_atomically`](Storage::write_atomically).
/// State is loaded once at startup via [`load_state`](Storage::load_state).
/// The remaining methods are read-only and used during normal operation.
///
/// All methods are async to support storage backends with async I/O primitives.
/// No `Send` bound is placed on the returned futures, so implementations may use
/// thread-affine runtimes (e.g. smol).
#[allow(async_fn_in_trait)]
pub trait Storage<T>
where
    T: Entry,
{
    /// **Atomically** perform all storage operations in order.
    /// For correctness, the operations must be atomic i.e., either all operations are performed
    /// successfully or all get rolled back. If the `StorageResult` returns as `Err`, the
    /// operations are assumed to have been rolled back to the previous state before this function
    /// call.
    async fn write_atomically(&mut self, ops: Vec<StorageOp<T>>) -> StorageResult<()>;

    /// Load all persisted scalar state on startup. Called once during recovery.
    async fn load_state(&self) -> StorageResult<PersistedState<T>>;

    /// Returns the entries in the log in the index interval of [from, to).
    /// If entries **do not exist for the complete interval**, an empty Vector should be returned.
    async fn get_entries(&self, from: usize, to: usize) -> StorageResult<Vec<T>>;

    /// Returns the suffix of entries in the log from index `from` (inclusive).
    /// If entries **do not exist for the complete interval**, an empty Vector should be returned.
    async fn get_suffix(&self, from: usize) -> StorageResult<Vec<T>>;

    /// Returns the stored snapshot.
    async fn get_snapshot(&self) -> StorageResult<Option<T::Snapshot>>;

    /// Returns the current length of the log (without the trimmed/snapshotted entries).
    async fn get_log_len(&self) -> StorageResult<usize>;

    /// Whether the storage backend is currently ready to create a snapshot.
    ///
    /// Called before routine compaction. If this returns `false`, the snapshot
    /// is deferred (returning [`CompactionErr::Deferred`](crate::CompactionErr::Deferred)).
    /// Urgent snapshots (e.g., for follower catch-up) bypass this check.
    ///
    /// The default implementation always returns `true`.
    async fn can_snapshot(&self) -> bool {
        true
    }
}

/// A place holder type for when not using snapshots. You should not use this type, it is only internally when deriving the Entry implementation.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub struct NoSnapshot;

impl<T: Entry> Snapshot<T> for NoSnapshot {
    fn create(_entries: &[T]) -> Self {
        panic!("NoSnapshot should not be created");
    }

    fn merge(&mut self, _delta: Self) {
        panic!("NoSnapshot should not be merged");
    }

    fn use_snapshots() -> bool {
        false
    }
}
