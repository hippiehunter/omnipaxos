use super::state_cache::StateCache;
use crate::{
    ballot_leader_election::Ballot,
    storage::{Entry, Snapshot, SnapshotType, StopSign, Storage, StorageOp, StorageResult},
    util::{AcceptedMetaData, IndexEntry, LogEntry, LogSync, SnapshottedEntry},
    CompactionErr,
};
use std::{
    cmp::Ordering,
    marker::PhantomData,
    ops::{Bound, RangeBounds},
    sync::Arc,
};

pub(crate) struct InternalStorageConfig {
    pub(crate) batch_size: usize,
}

/// Internal representation of storage. Serves as the interface between Sequence Paxos and the
/// storage back-end.
pub(crate) struct InternalStorage<I, T>
where
    I: Storage<T>,
    T: Entry,
{
    storage: I,
    state_cache: StateCache<T>,
    _t: PhantomData<T>,
}

impl<I, T> InternalStorage<I, T>
where
    I: Storage<T>,
    T: Entry,
{
    pub(crate) async fn with(storage: I, config: InternalStorageConfig) -> StorageResult<Self> {
        let mut internal_store = InternalStorage {
            storage,
            state_cache: StateCache::new(config),
            _t: Default::default(),
        };
        internal_store.load_cache().await?;
        Ok(internal_store)
    }

    async fn load_cache(&mut self) -> StorageResult<()> {
        self.state_cache.promise = self.storage.get_promise().await?.unwrap_or_default();
        self.state_cache.decided_idx = self.storage.get_decided_idx().await?;
        self.state_cache.accepted_round =
            self.storage.get_accepted_round().await?.unwrap_or_default();
        self.state_cache.compacted_idx = self.storage.get_compacted_idx().await?;
        self.state_cache.stopsign = self.storage.get_stopsign().await?;
        self.state_cache.accepted_idx =
            self.storage.get_log_len().await? + self.state_cache.compacted_idx;
        if self.state_cache.stopsign.is_some() {
            self.state_cache.accepted_idx += 1;
        }
        Ok(())
    }

    /// Read all decided entries from `from_idx` in the log. Returns `None` if `from_idx` is out of bounds.
    pub(crate) async fn read_decided_suffix(
        &self,
        from_idx: usize,
    ) -> StorageResult<Option<Vec<LogEntry<T>>>> {
        let decided_idx = self.get_decided_idx();
        if from_idx < decided_idx {
            self.read(from_idx..decided_idx).await
        } else {
            Ok(None)
        }
    }

    /// Read entries in the range `r` in the log. Returns `None` if `r` is out of bounds.
    pub(crate) async fn read<R>(&self, r: R) -> StorageResult<Option<Vec<LogEntry<T>>>>
    where
        R: RangeBounds<usize>,
    {
        let from_idx = match r.start_bound() {
            Bound::Included(i) => *i,
            Bound::Excluded(e) => *e + 1,
            Bound::Unbounded => 0,
        };
        let to_idx = match r.end_bound() {
            Bound::Included(i) => *i + 1,
            Bound::Excluded(e) => *e,
            Bound::Unbounded => self.get_accepted_idx(),
        };
        if to_idx == 0 {
            return Ok(None);
        }
        let compacted_idx = self.get_compacted_idx();
        let accepted_idx = self.get_accepted_idx();
        // use to_idx-1 when getting the entry type as to_idx is exclusive
        let to_type = match self.get_entry_type(to_idx - 1, compacted_idx, accepted_idx)? {
            Some(IndexEntry::Compacted) => {
                return Ok(Some(vec![
                    self.create_compacted_entry(compacted_idx).await?,
                ]))
            }
            Some(from_type) => from_type,
            _ => return Ok(None),
        };
        let from_type = match self.get_entry_type(from_idx, compacted_idx, accepted_idx)? {
            Some(from_type) => from_type,
            _ => return Ok(None),
        };
        match (from_type, to_type) {
            (IndexEntry::Entry, IndexEntry::Entry) => {
                Ok(Some(self.create_read_log_entries(from_idx, to_idx).await?))
            }
            (IndexEntry::Entry, IndexEntry::StopSign(ss)) => {
                let mut entries = self.create_read_log_entries(from_idx, to_idx - 1).await?;
                entries.push(LogEntry::StopSign(ss, self.stopsign_is_decided()));
                Ok(Some(entries))
            }
            (IndexEntry::Compacted, IndexEntry::Entry) => {
                let mut entries = Vec::with_capacity(to_idx - compacted_idx + 1);
                let compacted = self.create_compacted_entry(compacted_idx).await?;
                entries.push(compacted);
                let mut e = self.create_read_log_entries(compacted_idx, to_idx).await?;
                entries.append(&mut e);
                Ok(Some(entries))
            }
            (IndexEntry::Compacted, IndexEntry::StopSign(ss)) => {
                let mut entries = Vec::with_capacity(to_idx - compacted_idx + 1);
                let compacted = self.create_compacted_entry(compacted_idx).await?;
                entries.push(compacted);
                let mut e = self
                    .create_read_log_entries(compacted_idx, to_idx - 1)
                    .await?;
                entries.append(&mut e);
                entries.push(LogEntry::StopSign(ss, self.stopsign_is_decided()));
                Ok(Some(entries))
            }
            (IndexEntry::StopSign(ss), IndexEntry::StopSign(_)) => {
                Ok(Some(vec![LogEntry::StopSign(
                    ss,
                    self.stopsign_is_decided(),
                )]))
            }
            _ => Ok(None),
        }
    }

    fn get_entry_type(
        &self,
        idx: usize,
        compacted_idx: usize,
        accepted_idx: usize,
    ) -> StorageResult<Option<IndexEntry>> {
        if idx < compacted_idx {
            Ok(Some(IndexEntry::Compacted))
        } else if idx + 1 < accepted_idx {
            Ok(Some(IndexEntry::Entry))
        } else if idx + 1 == accepted_idx {
            match self.get_stopsign() {
                Some(ss) => Ok(Some(IndexEntry::StopSign(ss))),
                _ => Ok(Some(IndexEntry::Entry)),
            }
        } else {
            Ok(None)
        }
    }

    async fn create_read_log_entries(
        &self,
        from: usize,
        to: usize,
    ) -> StorageResult<Vec<LogEntry<T>>> {
        let decided_idx = self.get_decided_idx();
        let entries = self
            .get_entries(from, to)
            .await?
            .into_iter()
            .enumerate()
            .map(|(idx, e)| {
                let log_idx = idx + from;
                if log_idx < decided_idx {
                    LogEntry::Decided(e)
                } else {
                    LogEntry::Undecided(e)
                }
            })
            .collect();
        Ok(entries)
    }

    async fn create_compacted_entry(&self, compacted_idx: usize) -> StorageResult<LogEntry<T>> {
        self.storage.get_snapshot().await.map(|snap| match snap {
            Some(s) => LogEntry::Snapshotted(SnapshottedEntry::with(compacted_idx, s)),
            None => LogEntry::Trimmed(compacted_idx),
        })
    }

    // Append entry, if the batch size is reached, flush the batch and return the actual
    // accepted index (not including the batched entries)
    pub(crate) async fn append_entry_with_batching(
        &mut self,
        entry: T,
    ) -> StorageResult<Option<AcceptedMetaData<T>>> {
        let append_res = self.state_cache.append_entry(entry);
        self.flush_if_full_batch(append_res).await
    }

    // Append entries in batch, if the batch size is reached, flush the batch and return the
    // accepted index and the flushed entries. If the batch size is not reached, return None.
    pub(crate) async fn append_entries_with_batching(
        &mut self,
        entries: Vec<T>,
    ) -> StorageResult<Option<AcceptedMetaData<T>>> {
        let append_res = self.state_cache.append_entries(entries);
        self.flush_if_full_batch(append_res).await
    }

    // Flushes batched entries and appends a stopsign to the log atomically.
    // Returns the AcceptedMetaData associated with any flushed entries if there were any.
    pub(crate) async fn append_stopsign(
        &mut self,
        ss: StopSign,
    ) -> StorageResult<Option<AcceptedMetaData<T>>> {
        let has_batched = !self.state_cache.batched_entries.is_empty();
        let num_new = self.state_cache.batched_entries.len();
        // Build atomic transaction: flush entries + set stopsign in one write
        let mut ops = Vec::with_capacity(2);
        if has_batched {
            ops.push(StorageOp::AppendEntries(
                self.state_cache.batched_entries.clone(),
            ));
        }
        ops.push(StorageOp::SetStopsign(Some(ss.clone())));
        self.storage.write_atomically(ops).await?;
        // Only update cache after successful atomic write
        let accepted_entries_metadata = if has_batched {
            let entries = self.state_cache.take_batched_entries();
            self.state_cache.accepted_idx += num_new;
            Some(AcceptedMetaData {
                accepted_idx: self.state_cache.accepted_idx,
                entries: Arc::new(entries),
            })
        } else {
            None
        };
        self.state_cache.stopsign = Some(ss);
        self.state_cache.accepted_idx += 1;
        Ok(accepted_entries_metadata)
    }

    async fn flush_if_full_batch(
        &mut self,
        append_res: Option<Vec<T>>,
    ) -> StorageResult<Option<AcceptedMetaData<T>>> {
        if let Some(flushed_entries) = append_res {
            let shared = Arc::new(flushed_entries);
            let accepted_idx = self
                .append_entries_without_batching((*shared).clone())
                .await?;
            Ok(Some(AcceptedMetaData {
                accepted_idx,
                entries: shared,
            }))
        } else {
            Ok(None)
        }
    }

    // Append entries in batch, if the batch size is reached, flush the batch and return the
    // accepted index. If the batch size is not reached, return None.
    pub(crate) async fn append_entries_and_get_accepted_idx(
        &mut self,
        entries: Vec<T>,
    ) -> StorageResult<Option<usize>> {
        let append_res = self.state_cache.append_entries(entries);
        if let Some(flushed_entries) = append_res {
            let accepted_idx = self
                .append_entries_without_batching(flushed_entries)
                .await?;
            Ok(Some(accepted_idx))
        } else {
            Ok(None)
        }
    }

    pub(crate) async fn flush_batch(&mut self) -> StorageResult<usize> {
        if self.state_cache.batched_entries.is_empty() {
            return Ok(self.state_cache.accepted_idx);
        }
        let entries = self.state_cache.batched_entries.clone();
        let num_new = entries.len();
        self.storage.append_entries(entries).await?;
        // Only clear cache after successful write
        self.state_cache.batched_entries.clear();
        self.state_cache.accepted_idx += num_new;
        Ok(self.state_cache.accepted_idx)
    }

    #[allow(dead_code)]
    pub(crate) async fn flush_batch_and_get_entries(
        &mut self,
    ) -> StorageResult<Option<AcceptedMetaData<T>>> {
        let flushed_entries = if !self.state_cache.batched_entries.is_empty() {
            Some(self.state_cache.take_batched_entries())
        } else {
            None
        };
        self.flush_if_full_batch(flushed_entries).await
    }

    // Append entries without batching, return the accepted index
    pub(crate) async fn append_entries_without_batching(
        &mut self,
        entries: Vec<T>,
    ) -> StorageResult<usize> {
        let num_new_entries = entries.len();
        self.storage.append_entries(entries).await?;
        self.state_cache.accepted_idx += num_new_entries;
        Ok(self.state_cache.accepted_idx)
    }

    pub(crate) async fn sync_log(
        &mut self,
        accepted_round: Ballot,
        decided_idx: usize,
        log_sync: Option<LogSync<T>>,
    ) -> StorageResult<usize> {
        // Compute new cache values before writing to storage (fix B2: cache-before-write)
        let new_accepted_round = accepted_round;
        let new_decided_idx = decided_idx;
        let mut new_compacted_idx = self.state_cache.compacted_idx;
        let mut new_stopsign = self.state_cache.stopsign.clone();
        let mut new_accepted_idx = self.state_cache.accepted_idx;

        let mut sync_txn: Vec<StorageOp<T>> = vec![
            StorageOp::SetAcceptedRound(accepted_round),
            StorageOp::SetDecidedIndex(decided_idx),
        ];
        if let Some(sync) = log_sync {
            match sync.decided_snapshot {
                Some(SnapshotType::Complete(c)) => {
                    new_compacted_idx = sync.sync_idx;
                    sync_txn.push(StorageOp::Trim(sync.sync_idx));
                    sync_txn.push(StorageOp::SetCompactedIdx(sync.sync_idx));
                    sync_txn.push(StorageOp::SetSnapshot(Some(c)));
                }
                Some(SnapshotType::Delta(d)) => {
                    let mut snapshot = self.create_decided_snapshot().await?;
                    snapshot.merge(d);
                    new_compacted_idx = sync.sync_idx;
                    sync_txn.push(StorageOp::Trim(sync.sync_idx));
                    sync_txn.push(StorageOp::SetCompactedIdx(sync.sync_idx));
                    sync_txn.push(StorageOp::SetSnapshot(Some(snapshot)));
                }
                None => (),
            }
            new_accepted_idx = sync.sync_idx + sync.suffix.len();
            sync_txn.push(StorageOp::AppendOnPrefix(sync.sync_idx, sync.suffix));
            match sync.stopsign {
                Some(ss) => {
                    new_stopsign = Some(ss.clone());
                    new_accepted_idx += 1;
                    sync_txn.push(StorageOp::SetStopsign(Some(ss)));
                }
                None if new_stopsign.is_some() => {
                    new_stopsign = None;
                    sync_txn.push(StorageOp::SetStopsign(None));
                }
                None => (),
            }
        }
        // Write atomically first, only update cache on success
        self.storage.write_atomically(sync_txn).await?;
        self.state_cache.accepted_round = new_accepted_round;
        self.state_cache.decided_idx = new_decided_idx;
        self.state_cache.compacted_idx = new_compacted_idx;
        self.state_cache.stopsign = new_stopsign;
        self.state_cache.accepted_idx = new_accepted_idx;
        Ok(self.state_cache.accepted_idx)
    }

    async fn create_decided_snapshot(&mut self) -> StorageResult<T::Snapshot> {
        let log_decided_idx = self.get_decided_idx_without_stopsign();
        self.create_snapshot(log_decided_idx).await
    }

    pub(crate) async fn create_snapshot(&self, compact_idx: usize) -> StorageResult<T::Snapshot> {
        let current_compacted_idx = self.get_compacted_idx();
        if compact_idx < current_compacted_idx {
            Err(CompactionErr::TrimmedIndex(current_compacted_idx))?
        }
        let entries = self
            .storage
            .get_entries(current_compacted_idx, compact_idx)
            .await?;
        let delta = T::Snapshot::create(entries.as_slice());
        match self.storage.get_snapshot().await? {
            Some(mut s) => {
                s.merge(delta);
                Ok(s)
            }
            None => Ok(delta),
        }
    }

    // Creates a Delta snapshot of entries from `from_idx` to the end of the decided log and also
    // returns the compacted idx of the created snapshot. If the range of entries contains entries
    // which have already been compacted a valid delta cannot be created, so creates a Complete
    // snapshot of the entire decided log instead.
    pub(crate) async fn create_diff_snapshot(
        &self,
        from_idx: usize,
    ) -> StorageResult<(Option<SnapshotType<T>>, usize)> {
        let log_decided_idx = self.get_decided_idx_without_stopsign();
        let compacted_idx = self.get_compacted_idx();
        let snapshot = if from_idx <= compacted_idx {
            // Some entries in range are compacted, snapshot entire decided log
            if compacted_idx < log_decided_idx {
                Some(SnapshotType::Complete(
                    self.create_snapshot(log_decided_idx).await?,
                ))
            } else {
                // Entire decided log already snapshotted
                self.get_snapshot()
                    .await?
                    .map(|s| SnapshotType::Complete(s))
            }
        } else {
            let diff_entries = self.get_entries(from_idx, log_decided_idx).await?;
            Some(SnapshotType::Delta(T::Snapshot::create(
                diff_entries.as_slice(),
            )))
        };
        Ok((snapshot, log_decided_idx))
    }

    pub(crate) async fn try_trim(&mut self, idx: usize) -> StorageResult<()> {
        let decided_idx = self.get_decided_idx();
        let log_decided_idx = self.get_decided_idx_without_stopsign();
        let new_compacted_idx = match idx.cmp(&decided_idx) {
            Ordering::Less => idx,
            Ordering::Equal => log_decided_idx,
            Ordering::Greater => Err(CompactionErr::UndecidedIndex(decided_idx))?,
        };
        if new_compacted_idx > self.get_compacted_idx() {
            self.storage
                .write_atomically(vec![
                    StorageOp::Trim(new_compacted_idx),
                    StorageOp::SetCompactedIdx(new_compacted_idx),
                ])
                .await?;
            self.state_cache.compacted_idx = new_compacted_idx;
        }
        Ok(())
    }

    pub(crate) async fn try_snapshot(&mut self, snapshot_idx: Option<usize>) -> StorageResult<()> {
        let decided_idx = self.get_decided_idx();
        let log_decided_idx = self.get_decided_idx_without_stopsign();
        let new_compacted_idx = match snapshot_idx {
            Some(i) => match i.cmp(&decided_idx) {
                Ordering::Less => i,
                Ordering::Equal => log_decided_idx,
                Ordering::Greater => Err(CompactionErr::UndecidedIndex(decided_idx))?,
            },
            None => log_decided_idx,
        };
        if new_compacted_idx > self.get_compacted_idx() {
            let snapshot = self.create_snapshot(new_compacted_idx).await?;
            self.storage
                .write_atomically(vec![
                    StorageOp::Trim(new_compacted_idx),
                    StorageOp::SetCompactedIdx(new_compacted_idx),
                    StorageOp::SetSnapshot(Some(snapshot)),
                ])
                .await?;
            self.state_cache.compacted_idx = new_compacted_idx;
        }
        Ok(())
    }

    pub(crate) async fn set_promise(&mut self, n_prom: Ballot) -> StorageResult<()> {
        self.storage.set_promise(n_prom).await?;
        self.state_cache.promise = n_prom;
        Ok(())
    }

    /// Atomically flush batched entries and set promise.
    pub(crate) async fn flush_batch_and_set_promise(
        &mut self,
        n_prom: Ballot,
    ) -> StorageResult<usize> {
        let num_new = self.state_cache.batched_entries.len();
        let mut ops = Vec::with_capacity(2);
        if num_new > 0 {
            ops.push(StorageOp::AppendEntries(
                self.state_cache.batched_entries.clone(),
            ));
        }
        ops.push(StorageOp::SetPromise(n_prom));
        self.storage.write_atomically(ops).await?;
        // Only clear cache after successful write
        if num_new > 0 {
            self.state_cache.batched_entries.clear();
        }
        self.state_cache.accepted_idx += num_new;
        self.state_cache.promise = n_prom;
        Ok(self.state_cache.accepted_idx)
    }

    /// Atomically flush batched entries and set decided index.
    /// The decided_idx is clamped to the new accepted_idx after flushing.
    pub(crate) async fn flush_batch_and_set_decided_idx(
        &mut self,
        decided_idx: usize,
    ) -> StorageResult<usize> {
        let num_new = self.state_cache.batched_entries.len();
        let new_accepted_idx = self.state_cache.accepted_idx + num_new;
        let clamped_decided_idx = decided_idx.min(new_accepted_idx);
        let mut ops = Vec::with_capacity(2);
        if num_new > 0 {
            ops.push(StorageOp::AppendEntries(
                self.state_cache.batched_entries.clone(),
            ));
        }
        ops.push(StorageOp::SetDecidedIndex(clamped_decided_idx));
        self.storage.write_atomically(ops).await?;
        // Only clear cache after successful write
        if num_new > 0 {
            self.state_cache.batched_entries.clear();
        }
        self.state_cache.accepted_idx = new_accepted_idx;
        self.state_cache.decided_idx = clamped_decided_idx;
        Ok(new_accepted_idx)
    }

    /// Atomically flush batched entries and set decided index, returning the
    /// flushed entries as AcceptedMetaData for sending AcceptDecide messages.
    /// Returns None if there are no batched entries.
    pub(crate) async fn flush_batch_and_decide_with_entries(
        &mut self,
        decided_idx: usize,
    ) -> StorageResult<Option<AcceptedMetaData<T>>> {
        if self.state_cache.batched_entries.is_empty() {
            return Ok(None);
        }
        let entries_for_op = self.state_cache.batched_entries.clone();
        let num_new = entries_for_op.len();
        let new_accepted_idx = self.state_cache.accepted_idx + num_new;
        let clamped_decided_idx = decided_idx.min(new_accepted_idx);
        let ops = vec![
            StorageOp::AppendEntries(entries_for_op),
            StorageOp::SetDecidedIndex(clamped_decided_idx),
        ];
        self.storage.write_atomically(ops).await?;
        // Only take entries from cache after successful write
        let entries = self.state_cache.take_batched_entries();
        self.state_cache.accepted_idx = new_accepted_idx;
        self.state_cache.decided_idx = clamped_decided_idx;
        Ok(Some(AcceptedMetaData {
            accepted_idx: new_accepted_idx,
            entries: Arc::new(entries),
        }))
    }

    /// Atomically flush batched entries and set stopsign.
    pub(crate) async fn flush_batch_and_set_stopsign(
        &mut self,
        ss: Option<StopSign>,
    ) -> StorageResult<usize> {
        let num_new = self.state_cache.batched_entries.len();
        let mut ops = Vec::with_capacity(2);
        if num_new > 0 {
            ops.push(StorageOp::AppendEntries(
                self.state_cache.batched_entries.clone(),
            ));
        }
        ops.push(StorageOp::SetStopsign(ss.clone()));
        self.storage.write_atomically(ops).await?;
        // Only clear cache after successful write
        if num_new > 0 {
            self.state_cache.batched_entries.clear();
        }
        self.state_cache.accepted_idx += num_new;
        // Update stopsign-related accepted_idx
        if ss.is_some() && self.state_cache.stopsign.is_none() {
            self.state_cache.accepted_idx += 1;
        } else if ss.is_none() && self.state_cache.stopsign.is_some() {
            self.state_cache.accepted_idx -= 1;
        }
        self.state_cache.stopsign = ss;
        Ok(self.state_cache.accepted_idx)
    }

    pub(crate) async fn set_decided_idx(&mut self, idx: usize) -> StorageResult<()> {
        self.storage.set_decided_idx(idx).await?;
        self.state_cache.decided_idx = idx;
        Ok(())
    }

    pub(crate) fn get_decided_idx(&self) -> usize {
        self.state_cache.decided_idx
    }

    fn get_decided_idx_without_stopsign(&self) -> usize {
        match self.stopsign_is_decided() {
            true => self.get_decided_idx() - 1,
            false => self.get_decided_idx(),
        }
    }

    pub(crate) fn get_accepted_round(&self) -> Ballot {
        self.state_cache.accepted_round
    }

    pub(crate) async fn get_entries(&self, from: usize, to: usize) -> StorageResult<Vec<T>> {
        self.storage.get_entries(from, to).await
    }

    /// The length of the replicated log, as if log was never compacted.
    pub(crate) fn get_accepted_idx(&self) -> usize {
        self.state_cache.accepted_idx
    }

    pub(crate) async fn get_suffix(&self, from: usize) -> StorageResult<Vec<T>> {
        self.storage.get_suffix(from).await
    }

    pub(crate) fn get_promise(&self) -> Ballot {
        self.state_cache.promise
    }

    pub(crate) fn get_stopsign(&self) -> Option<StopSign> {
        self.state_cache.stopsign.clone()
    }

    // Returns whether a stopsign is decided
    pub(crate) fn stopsign_is_decided(&self) -> bool {
        self.state_cache.stopsign_is_decided()
    }

    pub(crate) fn get_batched_count(&self) -> usize {
        self.state_cache.batched_entries.len()
    }

    pub(crate) async fn get_snapshot(&self) -> StorageResult<Option<T::Snapshot>> {
        self.storage.get_snapshot().await
    }

    pub(crate) fn get_compacted_idx(&self) -> usize {
        self.state_cache.compacted_idx
    }
}
