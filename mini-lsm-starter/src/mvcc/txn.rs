#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use std::{
    collections::HashSet,
    ops::Bound,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
};

use anyhow::{bail, Ok, Result};
use bytes::Bytes;
use crossbeam_skiplist::SkipMap;
use ouroboros::self_referencing;
use parking_lot::Mutex;

use crate::{
    iterators::{two_merge_iterator::TwoMergeIterator, StorageIterator},
    lsm_iterator::{FusedIterator, LsmIterator},
    lsm_storage::{LsmStorageInner, WriteBatchRecord},
};

use super::CommittedTxnData;

pub struct Transaction {
    pub(crate) read_ts: u64,
    pub(crate) inner: Arc<LsmStorageInner>,
    // 跳表不需要存ts,因为本事务内的所有操作的key的ts都是一致有效的
    pub(crate) local_storage: Arc<SkipMap<Bytes, Bytes>>,
    pub(crate) committed: Arc<AtomicBool>,
    /// Write set and read set
    pub(crate) key_hashes: Option<Mutex<(HashSet<u32>, HashSet<u32>)>>,
}

impl Transaction {
    pub fn get(&self, key: &[u8]) -> Result<Option<Bytes>> {
        assert!(!self.committed.load(Ordering::SeqCst), "committed ts");
        if let Some(key_hashes) = self.key_hashes.as_ref() {
            let mut guard = key_hashes.lock();
            guard.1.insert(farmhash::fingerprint32(key));
        }
        // 先从 local_storage 中查找
        if let Some(value) = self.local_storage.get(key).map(|e| e.value().clone()) {
            if value.is_empty() {
                return Ok(None);
            } else {
                return Ok(Some(value));
            }
        }

        // 如果 local_storage 中没有，则调用 inner.get_with_ts
        self.inner.get_with_ts(key, self.read_ts)
    }
    /// Create a bound of `Bytes` from a bound of `&[u8]`.
    pub(crate) fn map_bound(bound: Bound<&[u8]>) -> Bound<Bytes> {
        match bound {
            Bound::Included(x) => Bound::Included(Bytes::copy_from_slice(x)),
            Bound::Excluded(x) => Bound::Excluded(Bytes::copy_from_slice(x)),
            Bound::Unbounded => Bound::Unbounded,
        }
    }
    pub fn scan(self: &Arc<Self>, lower: Bound<&[u8]>, upper: Bound<&[u8]>) -> Result<TxnIterator> {
        assert!(!self.committed.load(Ordering::SeqCst), "committed ts");

        let mut local = TxnLocalIterator::new(
            self.local_storage.clone(),
            |storage| {
                storage.range((
                    lower.map(|x| Bytes::copy_from_slice(x)),
                    upper.map(|x| Bytes::copy_from_slice(x)),
                ))
            },
            (Bytes::new(), Bytes::new()),
        );
        local.next()?;
        let result = TxnIterator::create(
            self.clone(),
            TwoMergeIterator::create(local, self.inner.scan_with_ts(lower, upper, self.read_ts)?)?,
        )?;
        if let Some(key_hashes) = self.key_hashes.as_ref() {
            if result.is_valid() {
                let mut guard = key_hashes.lock();
                guard.1.insert(farmhash::fingerprint32(result.key()));
            }
        }
        Ok(result)
    }

    pub fn put(&self, key: &[u8], value: &[u8]) {
        assert!(!self.committed.load(Ordering::SeqCst), "committed ts");
        if let Some(key_hashes) = self.key_hashes.as_ref() {
            let mut guard = key_hashes.lock();
            guard.0.insert(farmhash::fingerprint32(key));
        }
        self.local_storage
            .insert(Bytes::copy_from_slice(key), Bytes::copy_from_slice(value));
    }

    pub fn delete(&self, key: &[u8]) {
        assert!(!self.committed.load(Ordering::SeqCst), "committed ts");
        if let Some(key_hashes) = self.key_hashes.as_ref() {
            let mut guard = key_hashes.lock();
            guard.0.insert(farmhash::fingerprint32(key));
        }
        self.local_storage
            .insert(Bytes::copy_from_slice(key), Bytes::new());
    }

    pub fn write_batch_inner(&self) -> Result<()> {
        let iter = self.local_storage.iter();
        let mut batch_record = vec![];
        for entry in iter {
            if entry.value().is_empty() {
                batch_record.push(WriteBatchRecord::Del(entry.key().clone()));
            } else {
                batch_record.push(WriteBatchRecord::Put(
                    entry.key().clone(),
                    entry.value().clone(),
                ));
            }
        }
        self.inner.write_batch_inner(&batch_record)?;
        Ok(())
    }
    pub fn commit(&self) -> Result<()> {
        // enable serialize check
        if let Some(work_set) = self.key_hashes.as_ref() {
            let mut work_set = work_set.lock();
            let _lck = self.inner.mvcc().commit_lock.lock();
            if !work_set.0.is_empty() {
                let expect = self.inner.mvcc().latest_commit_ts() + 1;
                let mut txns = self.inner.mvcc().committed_txns.lock();
                let intersect_txns =
                    txns.range((Bound::Excluded(self.read_ts), Bound::Excluded(expect)));
                let committable = true;
                for (_, commit_data) in intersect_txns {
                    let any_conflict = commit_data
                        .key_hashes
                        .iter()
                        // 检查读集合是否和别的事务write_set是否有冲突.
                        // 检查读的原因是因为避免conflict,事务的所有写都有可能是基于读到的内容,而读的key本身可能本身已被改写
                        .any(|hash| work_set.1.contains(hash));
                    if any_conflict {
                        bail!("txn commit failed, conflict with previous txn");
                    }
                }
                txns.insert(
                    expect,
                    CommittedTxnData {
                        key_hashes: std::mem::replace(&mut work_set.0, HashSet::new()),
                        read_ts: self.read_ts,
                        commit_ts: expect,
                    },
                );
            }
            self.write_batch_inner()?;
        }
        self.write_batch_inner()
    }
}

impl Drop for Transaction {
    fn drop(&mut self) {
        self.inner.mvcc().ts.lock().1.remove_reader(self.read_ts);
    }
}

type SkipMapRangeIter<'a> =
    crossbeam_skiplist::map::Range<'a, Bytes, (Bound<Bytes>, Bound<Bytes>), Bytes, Bytes>;

#[self_referencing]
pub struct TxnLocalIterator {
    /// Stores a reference to the skipmap.
    map: Arc<SkipMap<Bytes, Bytes>>,
    /// Stores a skipmap iterator that refers to the lifetime of `TxnLocalIterator` itself.
    #[borrows(map)]
    #[not_covariant]
    iter: SkipMapRangeIter<'this>,
    /// Stores the current key-value pair.
    item: (Bytes, Bytes),
}

impl StorageIterator for TxnLocalIterator {
    type KeyType<'a> = &'a [u8];

    fn value(&self) -> &[u8] {
        self.borrow_item().1.as_ref()
    }

    fn key(&self) -> &[u8] {
        self.borrow_item().0.as_ref()
    }

    fn is_valid(&self) -> bool {
        !self.borrow_item().0.is_empty()
    }

    // TxnLocalIterator不能自行消化delete逻辑;
    // 本地删了 + sst里还有老版本的时候,local_iter有义务返回被删除key,代表最新版本
    fn next(&mut self) -> Result<()> {
        let kv = self.with_iter_mut(|iter| {
            let entry = iter.next();
            entry
                .map(|e| (e.key().clone(), e.value().clone()))
                .unwrap_or_else(|| (Bytes::new(), Bytes::new()))
        });

        self.with_mut(|iter| *iter.item = kv);
        Ok(())
    }
}

pub struct TxnIterator {
    txn: Arc<Transaction>,
    iter: TwoMergeIterator<TxnLocalIterator, FusedIterator<LsmIterator>>,
}

impl TxnIterator {
    pub fn create(
        txn: Arc<Transaction>,
        mut iter: TwoMergeIterator<TxnLocalIterator, FusedIterator<LsmIterator>>,
    ) -> Result<Self> {
        while iter.is_valid() && iter.value().is_empty() {
            iter.next()?;
        }
        Ok(Self {
            txn: txn.clone(),
            iter,
        })
    }
}

impl StorageIterator for TxnIterator {
    type KeyType<'a>
        = &'a [u8]
    where
        Self: 'a;

    fn value(&self) -> &[u8] {
        self.iter.value()
    }

    fn key(&self) -> Self::KeyType<'_> {
        self.iter.key()
    }

    fn is_valid(&self) -> bool {
        self.iter.is_valid()
    }

    fn next(&mut self) -> Result<()> {
        self.iter.next()?;
        while self.iter.is_valid() && self.iter.value().is_empty() {
            self.iter.next()?
        }
        if let Some(key_hashes) = self.txn.key_hashes.as_ref() {
            if self.iter.is_valid() {
                let mut guard = key_hashes.lock();
                guard.1.insert(farmhash::fingerprint32(self.iter.key()));
            }
        }
        Ok(())
    }

    fn num_active_iterators(&self) -> usize {
        self.iter.num_active_iterators()
    }
}
