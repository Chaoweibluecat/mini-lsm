#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use std::{cmp::Ordering, sync::Arc};

use bytes::Buf;

use crate::key::{KeySlice, KeyVec};

use super::Block;

/// Iterates on a block.
pub struct BlockIterator {
    /// The internal `Block`, wrapped by an `Arc`
    block: Arc<Block>,
    /// The current key, empty represents the iterator is invalid
    key: KeyVec,
    /// the current value range in the block.data, corresponds to the current key
    value_range: (usize, usize),
    /// Current index of the key-value pair, should be in range of [0, num_of_elements)
    idx: usize,
    /// The first key in the block
    first_key: KeyVec,
}

impl BlockIterator {
    fn new(block: Arc<Block>) -> Self {
        Self {
            block,
            key: KeyVec::new(),
            value_range: (0, 0),
            idx: 0,
            first_key: KeyVec::new(),
        }
    }

    /// Creates a block iterator and seek to the first entry.
    pub fn create_and_seek_to_first(block: Arc<Block>) -> Self {
        let mut iter = BlockIterator::new(block.clone());
        if block.offsets.is_empty() {
            return iter;
        }
        iter.seek_to_first();
        iter
    }

    /// Creates a block iterator and seek to the first key that >= `key`.
    pub fn create_and_seek_to_key(block: Arc<Block>, key: KeySlice) -> Self {
        let mut iter = BlockIterator::new(block.clone());
        if block.offsets.is_empty() {
            return iter;
        }
        iter.seek_to_key(key);
        iter
    }

    /// Returns the key of the current entry.
    pub fn key(&self) -> KeySlice {
        self.key.as_key_slice()
    }

    /// Returns the value of the current entry.
    pub fn value(&self) -> &[u8] {
        &self.block.data[self.value_range.0..self.value_range.1]
    }

    /// Returns true if the iterator is valid.
    /// Note: You may want to make use of `key`
    pub fn is_valid(&self) -> bool {
        !self.key.is_empty()
    }

    /// Seeks to the first key in the block.
    pub fn seek_to_first(&mut self) {
        // 幂等api,调用后重置idx,并根据idx设置iter的其他暂存属性
        self.idx = 0;
        self.set_by_idx();
    }

    /// Move to the next key in the block.
    pub fn next(&mut self) {
        self.seek_next();
    }

    /// Seek to the first key that >= `key`.
    /// Note: You should assume the key-value pairs in the block are sorted when being added by
    /// callers.
    pub fn seek_to_key(&mut self, key: KeySlice) {
        // 简单期间,从0开始,向右遍历直到找到一个不小于key的 block_key
        self.seek_to_first();
        while self.is_valid() && key.cmp(&self.key()) == Ordering::Greater {
            self.seek_next();
        }
    }

    // set current value range and key by current idx,needs to be called whenever there's a idx update
    fn set_by_idx(&mut self) {
        // idx太大,通过清空key的方式反馈iter已经非法
        if self.idx >= self.block.offsets.len() {
            self.key.clear();
            return;
        }
        let key_offset: u16 = self.block.offsets[self.idx];
        let key_len = (&self.block.data[key_offset as usize..]).get_u16();
        let key_start = key_offset + 2;
        let mut key_end: u16 = key_start + key_len;
        let key_slice = &self.block.data[key_start as usize..key_end as usize];
        let mut ts = &self.block.data[key_end as usize..(key_end + 8) as usize];
        let ts = ts.get_u64();
        key_end += 8;
        let value_len = (&self.block.data[key_end as usize..]).get_u16();
        let value_start = key_end + 2;
        let value_end = value_start + value_len;
        self.value_range = (value_start as usize, value_end as usize);
        if self.idx == 0 {
            self.first_key.clear();
            self.first_key.append(key_slice);
            self.first_key.set_ts(ts);
        }
        self.key.clear();
        self.key.append(key_slice);
        self.key.set_ts(ts);
    }

    fn seek_next(&mut self) {
        self.idx = self.idx + 1;
        // 设置为next
        self.set_by_idx();
    }
}
