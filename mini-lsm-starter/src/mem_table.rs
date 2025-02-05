#![allow(dead_code)] // REMOVE THIS LINE after fully implementing this functionality

use std::collections::hash_map::Entry;
use std::ops::Bound;
use std::path::Path;
use std::sync::atomic::AtomicUsize;
use std::sync::Arc;

use anyhow::{ Ok, Result };
use bytes::Bytes;
use clap::builder;
use crossbeam_skiplist::SkipMap;
use ouroboros::self_referencing;
use serde::de::value;

use crate::iterators::StorageIterator;
use crate::key::{ Key, KeyBytes, KeySlice, TS_DEFAULT };
use crate::table::SsTableBuilder;
use crate::wal::Wal;

/// A basic mem-table based on crossbeam-skiplist.
///
/// An initial implementation of memtable is part of week 1, day 1. It will be incrementally implemented in other
/// chapters of week 1 and week 2.
pub struct MemTable {
    map: Arc<SkipMap<KeyBytes, Bytes>>,
    wal: Option<Wal>,
    id: usize,
    approximate_size: Arc<AtomicUsize>,
}

/// Create a bound of `Bytes` from a bound of `&[u8]`.
pub(crate) fn map_bound(bound: Bound<KeySlice>) -> Bound<KeyBytes> {
    match bound {
        Bound::Included(x) => Bound::Included(x.as_key_bytes()),
        Bound::Excluded(x) => Bound::Excluded(x.as_key_bytes()),
        Bound::Unbounded => Bound::Unbounded,
    }
}

impl MemTable {
    /// Create a new mem-table.
    pub fn create(_id: usize) -> Self {
        Self {
            map: Arc::new(SkipMap::new()),
            wal: None,
            id: _id,
            approximate_size: Arc::new(AtomicUsize::new(0)),
        }
    }
    pub fn max_ts(&self) -> u64 {
        self.map
            .iter()
            .map(|k| k.key().ts())
            .max()
            .unwrap_or(0)
    }

    /// Create a new mem-table with WAL
    pub fn create_with_wal(_id: usize, _path: impl AsRef<Path>) -> Result<Self> {
        let wal = Wal::create(_path)?;
        Ok(Self {
            map: Arc::new(SkipMap::new()),
            wal: Some(wal),
            id: _id,
            approximate_size: Arc::new(AtomicUsize::new(0)),
        })
    }

    /// Create a memtable from WAL
    pub fn recover_from_wal(_id: usize, _path: impl AsRef<Path>) -> Result<Self> {
        let skiplist = Arc::new(SkipMap::new());
        let wal = Wal::recover(_path.as_ref(), &skiplist)?;
        Ok(Self {
            map: skiplist,
            wal: Some(wal),
            id: _id,
            approximate_size: Arc::new(AtomicUsize::new(0)),
        })
    }

    pub fn for_testing_put_slice(&self, key: &[u8], value: &[u8]) -> Result<()> {
        self.put(KeySlice::from_slice(key, TS_DEFAULT), value)
    }

    pub fn for_testing_get_slice(&self, key: &[u8]) -> Option<Bytes> {
        self.get(KeySlice::from_slice(key, TS_DEFAULT))
    }

    pub fn for_testing_scan_slice(
        &self,
        lower: Bound<&[u8]>,
        upper: Bound<&[u8]>
    ) -> MemTableIterator {
        self.scan(
            lower.map(|x| KeySlice::from_slice(x, TS_DEFAULT)),
            upper.map(|x| KeySlice::from_slice(x, TS_DEFAULT))
        )
    }

    /// Get a value by key.
    pub fn get(&self, key: KeySlice) -> Option<Bytes> {
        self.map.get(&key.as_key_bytes()).map(|entry| entry.value().clone())
    }

    /// Put a key-value pair into the mem-table.
    ///
    /// In week 1, day 1, simply put the key-value pair into the skipmap.
    /// In week 2, day 6, also flush the data to WAL.
    /// In week 3, day 5, modify the function to use the batch API.
    pub fn put(&self, key: KeySlice, value: &[u8]) -> Result<()> {
        self.map.insert(
            // map生命周期比入参长,这里是一定要拷贝的;不然put结束后bytes指针指向无效地址
            key.to_key_vec().into_key_bytes(),
            Bytes::copy_from_slice(value)
        );
        if let Some(wal) = self.wal.as_ref() {
            wal.put(key, value)?;
        }
        self.approximate_size.fetch_add(
            key.raw_len() + value.len(),
            std::sync::atomic::Ordering::Relaxed
        );
        Ok(())
    }

    /// Implement this in week 3, day 5.
    pub fn put_batch(&self, _data: &[(KeySlice, &[u8])]) -> Result<()> {
        unimplemented!()
    }

    pub fn sync_wal(&self) -> Result<()> {
        if let Some(ref wal) = self.wal {
            wal.sync()?;
        }
        Ok(())
    }

    /// Get an iterator over a range of keys.
    pub fn scan(&self, lower: Bound<KeySlice>, upper: Bound<KeySlice>) -> MemTableIterator {
        let lower = map_bound(lower);
        let upper = map_bound(upper);

        let mut ret = MemTableIterator::new(self.map.clone(), |map| map.range((lower, upper)), (
            KeyBytes::new(),
            Bytes::new(),
        ));

        ret.with_mut(|ret| {
            if let Some(kv) = ret.iter.next() {
                ret.item.0 = kv.key().clone();
                ret.item.1 = kv.value().clone();
            }
        });
        ret
    }

    /// Flush the mem-table to SSTable. Implement in week 1 day 6.
    pub fn flush(&self, _builder: &mut SsTableBuilder) -> Result<()> {
        let mut iter = self.scan(Bound::Unbounded, Bound::Unbounded);
        while iter.is_valid() {
            _builder.add(iter.key(), iter.value());
            iter.next()?;
        }
        Ok(())
    }

    pub fn id(&self) -> usize {
        self.id
    }

    pub fn approximate_size(&self) -> usize {
        self.approximate_size.load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Only use this function when closing the database
    pub fn is_empty(&self) -> bool {
        self.map.is_empty()
    }
}

type SkipMapRangeIter<'a> = crossbeam_skiplist::map::Range<
    'a,
    KeyBytes,
    (Bound<KeyBytes>, Bound<KeyBytes>),
    KeyBytes,
    Bytes
>;

/// An iterator over a range of `SkipMap`. This is a self-referential structure and please refer to week 1, day 2
/// chapter for more information.
///
/// This is part of week 1, day 2.
#[self_referencing]
pub struct MemTableIterator {
    /// Stores a reference to the skipmap.
    map: Arc<SkipMap<KeyBytes, Bytes>>,
    /// Stores a skipmap iterator that refers to the lifetime of `MemTableIterator` itself.
    #[borrows(map)]
    #[not_covariant]
    iter: SkipMapRangeIter<'this>,
    /// Stores the current key-value pair.
    item: (KeyBytes, Bytes),
}

impl StorageIterator for MemTableIterator {
    type KeyType<'a> = KeySlice<'a>;

    fn value(&self) -> &[u8] {
        return self.borrow_item().1.as_ref();
    }

    fn key(&self) -> KeySlice {
        self.borrow_item().0.as_key_slice()
    }

    fn is_valid(&self) -> bool {
        !self.borrow_item().0.is_empty()
    }

    fn next(&mut self) -> Result<()> {
        let kv = self.with_iter_mut(|iter| {
            let entry = iter.next();
            entry
                .map(|e| (e.key().clone(), e.value().clone()))
                .unwrap_or_else(|| (KeyBytes::new(), Bytes::new()))
        });

        self.with_item_mut(|item| {
            *item = kv;
            Ok(())
        })
    }
}
