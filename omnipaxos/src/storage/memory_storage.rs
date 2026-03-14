use crate::{
    ballot_leader_election::Ballot,
    storage::{Entry, PersistedState, StopSign, Storage, StorageOp, StorageResult},
};
use std::collections::VecDeque;

/// An in-memory storage implementation for OmniPaxos.
#[derive(Clone)]
pub struct MemoryStorage<T>
where
    T: Entry,
{
    log: VecDeque<T>,
    n_prom: Option<Ballot>,
    acc_round: Option<Ballot>,
    ld: usize,
    trimmed_idx: usize,
    compacted_idx: usize,
    snapshot: Option<T::Snapshot>,
    stopsign: Option<StopSign>,
}

impl<T: Entry> Default for MemoryStorage<T> {
    fn default() -> Self {
        Self {
            log: VecDeque::new(),
            n_prom: None,
            acc_round: None,
            ld: 0,
            trimmed_idx: 0,
            compacted_idx: 0,
            snapshot: None,
            stopsign: None,
        }
    }
}

impl<T: Entry> MemoryStorage<T> {
    // Internal helpers used by write_atomically
    fn apply_op(&mut self, op: StorageOp<T>) {
        match op {
            StorageOp::AppendEntry(entry) => self.log.push_back(entry),
            StorageOp::AppendEntries(entries) => self.log.extend(entries),
            StorageOp::AppendOnPrefix(from_idx, entries) => {
                self.log.truncate(from_idx.saturating_sub(self.trimmed_idx));
                self.log.extend(entries);
            }
            StorageOp::SetPromise(bal) => self.n_prom = Some(bal),
            StorageOp::SetDecidedIndex(idx) => self.ld = idx,
            StorageOp::SetAcceptedRound(bal) => self.acc_round = Some(bal),
            StorageOp::SetCompactedIdx(idx) => self.compacted_idx = idx,
            StorageOp::Trim(trimmed_idx) => {
                let to_trim = (trimmed_idx - self.trimmed_idx).min(self.log.len());
                self.log.drain(0..to_trim);
                self.trimmed_idx = trimmed_idx;
            }
            StorageOp::SetStopsign(ss) => self.stopsign = ss,
            StorageOp::SetSnapshot(snap) => self.snapshot = snap,
        }
    }
}

impl<T> Storage<T> for MemoryStorage<T>
where
    T: Entry,
{
    async fn write_atomically(&mut self, ops: Vec<StorageOp<T>>) -> StorageResult<()> {
        let backup = self.clone();
        for op in ops {
            self.apply_op(op);
        }
        // MemoryStorage ops are infallible, but keep backup pattern for API contract
        let _ = backup;
        Ok(())
    }

    async fn load_state(&self) -> StorageResult<PersistedState<T>> {
        Ok(PersistedState {
            promise: self.n_prom,
            accepted_round: self.acc_round,
            decided_idx: self.ld,
            compacted_idx: self.compacted_idx,
            stopsign: self.stopsign.clone(),
            log_len: self.log.len(),
            snapshot: self.snapshot.clone(),
        })
    }

    async fn get_entries(&self, from: usize, to: usize) -> StorageResult<Vec<T>> {
        let from = from.saturating_sub(self.trimmed_idx);
        let to = to.saturating_sub(self.trimmed_idx);
        if from >= self.log.len() {
            return Ok(vec![]);
        }
        Ok(self
            .log
            .range(from..to.min(self.log.len()))
            .cloned()
            .collect())
    }

    async fn get_log_len(&self) -> StorageResult<usize> {
        Ok(self.log.len())
    }

    async fn get_suffix(&self, from: usize) -> StorageResult<Vec<T>> {
        let start = from.saturating_sub(self.trimmed_idx);
        if start >= self.log.len() {
            Ok(vec![])
        } else {
            Ok(self.log.range(start..).cloned().collect())
        }
    }

    async fn get_snapshot(&self) -> StorageResult<Option<T::Snapshot>> {
        Ok(self.snapshot.clone())
    }
}
