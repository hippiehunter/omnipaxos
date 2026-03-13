use crate::{
    ballot_leader_election::Ballot,
    storage::{Entry, StopSign, Storage, StorageOp, StorageResult},
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

impl<T> Storage<T> for MemoryStorage<T>
where
    T: Entry,
{
    async fn write_atomically(&mut self, ops: Vec<StorageOp<T>>) -> StorageResult<()> {
        // Save state for rollback on error
        let backup = self.clone();
        for op in ops {
            let result = match op {
                StorageOp::AppendEntry(entry) => self.append_entry(entry).await,
                StorageOp::AppendEntries(entries) => self.append_entries(entries).await,
                StorageOp::AppendOnPrefix(from_idx, entries) => {
                    self.append_on_prefix(from_idx, entries).await
                }
                StorageOp::SetPromise(bal) => self.set_promise(bal).await,
                StorageOp::SetDecidedIndex(idx) => self.set_decided_idx(idx).await,
                StorageOp::SetAcceptedRound(bal) => self.set_accepted_round(bal).await,
                StorageOp::SetCompactedIdx(idx) => self.set_compacted_idx(idx).await,
                StorageOp::Trim(idx) => self.trim(idx).await,
                StorageOp::SetStopsign(ss) => self.set_stopsign(ss).await,
                StorageOp::SetSnapshot(snap) => self.set_snapshot(snap).await,
            };
            if let Err(e) = result {
                *self = backup;
                return Err(e);
            }
        }
        Ok(())
    }

    async fn append_entry(&mut self, entry: T) -> StorageResult<()> {
        self.log.push_back(entry);
        Ok(())
    }

    async fn append_entries(&mut self, entries: Vec<T>) -> StorageResult<()> {
        self.log.extend(entries);
        Ok(())
    }

    async fn append_on_prefix(&mut self, from_idx: usize, entries: Vec<T>) -> StorageResult<()> {
        self.log.truncate(from_idx.saturating_sub(self.trimmed_idx));
        self.log.extend(entries);
        Ok(())
    }

    async fn set_promise(&mut self, n_prom: Ballot) -> StorageResult<()> {
        self.n_prom = Some(n_prom);
        Ok(())
    }

    async fn set_decided_idx(&mut self, ld: usize) -> StorageResult<()> {
        self.ld = ld;
        Ok(())
    }

    async fn get_decided_idx(&self) -> StorageResult<usize> {
        Ok(self.ld)
    }

    async fn set_accepted_round(&mut self, na: Ballot) -> StorageResult<()> {
        self.acc_round = Some(na);
        Ok(())
    }

    async fn get_accepted_round(&self) -> StorageResult<Option<Ballot>> {
        Ok(self.acc_round)
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

    async fn get_promise(&self) -> StorageResult<Option<Ballot>> {
        Ok(self.n_prom)
    }

    async fn set_stopsign(&mut self, s: Option<StopSign>) -> StorageResult<()> {
        self.stopsign = s;
        Ok(())
    }

    async fn get_stopsign(&self) -> StorageResult<Option<StopSign>> {
        Ok(self.stopsign.clone())
    }

    async fn trim(&mut self, trimmed_idx: usize) -> StorageResult<()> {
        let to_trim = (trimmed_idx - self.trimmed_idx).min(self.log.len());
        self.log.drain(0..to_trim);
        self.trimmed_idx = trimmed_idx;
        Ok(())
    }

    async fn set_compacted_idx(&mut self, compact_idx: usize) -> StorageResult<()> {
        self.compacted_idx = compact_idx;
        Ok(())
    }

    async fn get_compacted_idx(&self) -> StorageResult<usize> {
        Ok(self.compacted_idx)
    }

    async fn set_snapshot(&mut self, snapshot: Option<T::Snapshot>) -> StorageResult<()> {
        self.snapshot = snapshot;
        Ok(())
    }

    async fn get_snapshot(&self) -> StorageResult<Option<T::Snapshot>> {
        Ok(self.snapshot.clone())
    }
}
