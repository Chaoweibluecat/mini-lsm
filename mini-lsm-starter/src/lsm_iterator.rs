#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use std::{ops::Bound, thread::sleep};

use anyhow::{bail, Result};
use bytes::Bytes;

use crate::{
    iterators::{
        concat_iterator::SstConcatIterator, merge_iterator::MergeIterator,
        two_merge_iterator::TwoMergeIterator, StorageIterator,
    },
    key::KeySlice,
    mem_table::MemTableIterator,
    table::SsTableIterator,
};

/// Represents the internal type for an LSM iterator. This type will be changed across the tutorial for multiple times.
type LsmIteratorInner = TwoMergeIterator<
    TwoMergeIterator<MergeIterator<MemTableIterator>, MergeIterator<SsTableIterator>>,
    MergeIterator<SstConcatIterator>,
>;

pub struct LsmIterator {
    inner: LsmIteratorInner,
    end_bound: Bound<Bytes>,
    is_valid: bool,
}

impl LsmIterator {
    pub(crate) fn new(iter: LsmIteratorInner, end_bound: Bound<Bytes>) -> Result<Self> {
        // new完需要先找到第一个非tombstone的节点，否则第一次调用key的节点可能是非法的
        let mut iter = Self {
            inner: iter,
            end_bound,
            is_valid: true,
        };
        if iter.inner.is_valid() && iter.value().is_empty() {
            iter.inner_next()?;
        }
        iter.is_valid = iter.is_valid && iter.inner.is_valid();
        Ok(iter)
    }

    fn inner_next(&mut self) -> Result<()> {
        loop {
            let next = self.inner.next()?;
            if !self.inner.is_valid() || self.out_bound() {
                self.is_valid = false;
                break;
            } else if !self.inner.value().is_empty() {
                println!("{:?}", self.inner.value());
                break;
            }
        }
        Ok(())
    }

    fn out_bound(&self) -> bool {
        match &self.end_bound {
            Bound::Included(end) => self.inner.key() > KeySlice::from_slice(&end),
            Bound::Excluded(end) => self.inner.key() >= KeySlice::from_slice(&end),
            _ => false,
        }
    }
}

impl StorageIterator for LsmIterator {
    type KeyType<'a> = &'a [u8];

    fn is_valid(&self) -> bool {
        self.is_valid
    }

    fn key(&self) -> &[u8] {
        &self.inner.key().raw_ref()
    }

    fn value(&self) -> &[u8] {
        self.inner.value()
    }

    fn next(&mut self) -> Result<()> {
        // loop直到下一个非tombstone节点
        self.inner_next()
    }

    fn num_active_iterators(&self) -> usize {
        if self.inner.use_A() {
            self.inner.num_active_iterators()
        } else {
            1
        }
    }
}

/// A wrapper around existing iterator, will prevent users from calling `next` when the iterator is
/// invalid. If an iterator is already invalid, `next` does not do anything. If `next` returns an error,
/// `is_valid` should return false, and `next` should always return an error.
pub struct FusedIterator<I: StorageIterator> {
    iter: I,
    has_errored: bool,
}

impl<I: StorageIterator> FusedIterator<I> {
    pub fn new(iter: I) -> Self {
        Self {
            iter,
            has_errored: false,
        }
    }
}

impl<I: StorageIterator> StorageIterator for FusedIterator<I> {
    type KeyType<'a>
        = I::KeyType<'a>
    where
        Self: 'a;

    fn is_valid(&self) -> bool {
        !self.has_errored && self.iter.is_valid()
    }

    fn key(&self) -> Self::KeyType<'_> {
        self.iter.key()
    }

    fn value(&self) -> &[u8] {
        self.iter.value()
    }

    fn next(&mut self) -> Result<()> {
        if self.has_errored {
            bail!("fused iterator")
        }
        if !self.iter.is_valid() {
            return Ok(());
        }
        match self.iter.next() {
            Err(e) => {
                self.has_errored = true;
                Err(e)
            }
            Ok(_) => Ok(()),
        }
    }
    fn num_active_iterators(&self) -> usize {
        return self.iter.num_active_iterators();
    }
}
