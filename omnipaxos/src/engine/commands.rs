use crate::storage::{Entry, StorageOp};

/// Commands emitted by the Engine for the Runtime to execute.
/// These represent storage mutations that need to be persisted.
#[derive(Debug)]
pub(crate) enum Command<T: Entry> {
    /// Execute storage operations atomically via write_atomically().
    WriteAtomic(Vec<StorageOp<T>>),
    /// Non-atomic append (for the batching path when batch flushes).
    AppendEntries(Vec<T>),
}
