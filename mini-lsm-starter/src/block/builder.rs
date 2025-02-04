#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use bytes::BufMut;

use crate::key::{KeySlice, KeyVec};

use super::Block;

/// Builds a block.
pub struct BlockBuilder {
    /// Offsets of each key-value entries.
    offsets: Vec<u16>,
    /// All serialized key-value pairs in the block.
    data: Vec<u8>,
    /// The expected block size.
    block_size: usize,
    /// The first key in the block
    first_key: KeyVec,
}

impl BlockBuilder {
    /// Creates a new block builder.
    pub fn new(block_size: usize) -> Self {
        Self {
            offsets: vec![],
            data: vec![],
            block_size,
            first_key: KeyVec::new(),
        }
    }

    /// Adds a key-value pair to the block. Returns false when the block is full.
    #[must_use]
    pub fn add(&mut self, key: KeySlice, value: &[u8]) -> bool {
        // offset size 2字节 + kv长度各两个字节
        if !self.is_empty()
            && self.data.len() + 2 * self.offsets.len() + key.raw_len() + value.len() + 6
                > self.block_size
        {
            return false;
        }
        if self.is_empty() {
            self.first_key.set_from_slice(key);
        }

        let cur_offset = self.data.len();
        self.offsets.push(cur_offset as u16);

        self.data.put_u16(key.key_len() as u16);

        self.data.extend(key.key_ref());
        // add timestamp
        self.data.put_u64(key.ts());
        self.data.put_u16(value.len() as u16);
        self.data.extend(value);
        return true;
    }

    /// Check if there is no key-value pair in the block.
    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }

    /// Finalize the block.
    pub fn build(self) -> Block {
        Block {
            data: self.data,
            offsets: self.offsets,
        }
    }
}
