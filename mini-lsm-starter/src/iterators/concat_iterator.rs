#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use std::sync::Arc;

use anyhow::{Ok, Result};

use super::StorageIterator;
use crate::{
    key::KeySlice,
    table::{SsTable, SsTableIterator},
};

/// Concat multiple iterators ordered in key order and their key ranges do not overlap. We do not want to create the
/// iterators when initializing this iterator to reduce the overhead of seeking.
pub struct SstConcatIterator {
    current: Option<SsTableIterator>,
    next_sst_idx: usize,
    sstables: Vec<Arc<SsTable>>,
}

impl SstConcatIterator {
    pub fn create_and_seek_to_first(sstables: Vec<Arc<SsTable>>) -> Result<Self> {
        if sstables.is_empty() {
            Ok(Self {
                current: None,
                next_sst_idx: 0,
                sstables,
            })
        } else {
            Ok(Self {
                current: Some(SsTableIterator::create_and_seek_to_first(
                    sstables[0].clone(),
                )?),
                next_sst_idx: 1,
                sstables,
            })
        }
    }

    pub fn create_and_seek_to_key(sstables: Vec<Arc<SsTable>>, key: KeySlice) -> Result<Self> {
        let mut target = sstables
            .partition_point(|sst| sst.first_key().as_key_slice() <= key)
            .saturating_sub(1);
        if sstables[target].last_key().as_key_slice() < key {
            target = target.saturating_add(1);
        }

        if target >= sstables.len() {
            Ok(Self {
                current: None,
                next_sst_idx: target,
                sstables,
            })
        } else {
            Ok(Self {
                current: Some(SsTableIterator::create_and_seek_to_key(
                    sstables[target].clone(),
                    key,
                )?),
                next_sst_idx: target.saturating_add(1),
                sstables,
            })
        }
    }
}

impl StorageIterator for SstConcatIterator {
    type KeyType<'a> = KeySlice<'a>;

    fn key(&self) -> KeySlice {
        self.current.as_ref().unwrap().key()
    }

    fn value(&self) -> &[u8] {
        self.current.as_ref().unwrap().value()
    }

    fn is_valid(&self) -> bool {
        self.current.is_some() && self.current.as_ref().unwrap().is_valid()
    }

    fn next(&mut self) -> Result<()> {
        if !self.current.as_ref().unwrap().is_valid() {
            self.next_sst_idx = self.next_sst_idx.saturating_add(1);
            if self.next_sst_idx >= self.sstables.len() {
                self.current = None;
                return Ok(());
            } else {
                self.current = Some(SsTableIterator::create_and_seek_to_first(
                    self.sstables[self.next_sst_idx].clone(),
                )?);
                return Ok(());
            }
        }
        self.current.as_mut().unwrap().next()?;
        Ok(())
    }

    fn num_active_iterators(&self) -> usize {
        1
    }
}
