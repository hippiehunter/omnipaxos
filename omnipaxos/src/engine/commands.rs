use crate::storage::{Entry, StorageOp};

/// Commands emitted by the Engine for the Runtime to execute.
/// These represent storage mutations that need to be persisted.
#[derive(Debug)]
pub(crate) enum Command<T: Entry> {
    /// Execute storage operations atomically via write_atomically().
    WriteAtomic(Vec<StorageOp<T>>),
    /// Execute `ops` atomically after inserting, at `snapshot_at`, a
    /// `SetSnapshot` of the stored snapshot merged with this node's own
    /// decided entries `[own_from, own_to)` and then `delta`. The runtime
    /// reads both before the write, so the snapshot covers every decided
    /// entry the batch trims.
    WriteAtomicWithDelta {
        ops: Vec<StorageOp<T>>,
        snapshot_at: usize,
        own_from: u64,
        own_to: u64,
        delta: T::Snapshot,
    },
    /// Non-atomic append (for the batching path when batch flushes).
    AppendEntries(Vec<T>),
}
