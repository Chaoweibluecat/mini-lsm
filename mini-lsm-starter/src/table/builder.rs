use super::{bloom::Bloom, BlockMeta, FileObject, SsTable};
use crate::{
    block::BlockBuilder,
    key::{Key, KeySlice, KeyVec},
    lsm_storage::BlockCache,
};
use anyhow::{Ok, Result};
use bytes::{BufMut, Bytes};
use std::path::Path;
use std::sync::Arc;

/// Builds an SSTable from key-value pairs.
pub struct SsTableBuilder {
    builder: BlockBuilder,
    // 当前block的first_key;过程数据
    first_key: KeyVec,
    last_key: KeyVec,
    data: Vec<u8>,
    pub(crate) meta: Vec<BlockMeta>,
    block_size: usize,
    hash_keys: Vec<u32>,
}

impl SsTableBuilder {
    /// Create a builder based on target block size.
    pub fn new(block_size: usize) -> Self {
        Self {
            builder: BlockBuilder::new(block_size),
            first_key: KeyVec::new(),
            last_key: KeyVec::new(),
            data: vec![],
            meta: vec![],
            block_size,
            hash_keys: vec![],
        }
    }
    /// Adds a key-value pair to SSTable.
    ///
    /// Note: You should split a new block when the current block is full.(`std::mem::replace` may
    /// be helpful here)
    pub fn add(&mut self, key: KeySlice, value: &[u8]) {
        if self.first_key.is_empty() {
            self.first_key.set_from_slice(key);
        }

        if !self.builder.add(key, value) {
            self.finish_cur_block();
            assert!(self.builder.add(key, value));
            // finish_cur_block会消耗当前的first_key;需要reset
            self.first_key.set_from_slice(key);
            self.last_key.set_from_slice(key);
            return;
        }

        self.last_key.clear();
        self.last_key.set_from_slice(key);
        self.hash_keys.push(farmhash::fingerprint32(&key.key_ref()));
    }

    /// Get the estimated size of the SSTable.
    ///
    /// Since the data blocks contain much more data than meta blocks, just return the size of data
    /// blocks here.
    pub fn estimated_size(&self) -> usize {
        self.data.len()
    }

    pub fn is_empty(&self) -> bool {
        self.builder.is_empty() && self.data.is_empty()
    }

    pub fn get_fisrt_key(&self) -> &Key<Bytes> {
        &self.meta[0].first_key
    }

    fn finish_cur_block(&mut self) {
        if self.first_key.is_empty() {
            return;
        }
        // block split, 生成并存入meta信息, reset
        let old_first_key = std::mem::replace(&mut self.first_key, KeyVec::new());
        let old_last_key = std::mem::replace(&mut self.last_key, KeyVec::new());
        let meta = BlockMeta {
            offset: self.data.len(),
            first_key: Key::from_bytes_with_ts(
                Bytes::copy_from_slice(old_first_key.key_ref()),
                old_first_key.ts(),
            ),
            last_key: Key::from_bytes_with_ts(
                Bytes::copy_from_slice(old_last_key.key_ref()),
                old_last_key.ts(),
            ),
        };
        self.meta.push(meta);
        let new_block_builder = BlockBuilder::new(self.block_size);
        let old_block: BlockBuilder = std::mem::replace(&mut self.builder, new_block_builder);
        let block = old_block.build();
        let block_data = block.encode();
        let checksum = crc32fast::hash(&block_data);
        self.data.extend(block_data);
        self.data.put_u32(checksum);
    }

    /// Builds the SSTable and writes it to the given path. Use the `FileObject` structure to manipulate the disk objects.
    pub fn build(
        mut self,
        id: usize,
        block_cache: Option<Arc<BlockCache>>,
        path: impl AsRef<Path>,
    ) -> Result<SsTable> {
        // should me include current block??

        self.finish_cur_block();
        let mut output = self.data;
        let meta_offset = output.len();
        BlockMeta::encode_block_meta(&self.meta, &mut output);
        output.put_u32(meta_offset as u32);

        let bloom_offset = output.len() as u32;
        let bloom = if !self.hash_keys.is_empty() {
            let bits_per_key = Bloom::bloom_bits_per_key(self.hash_keys.len(), 0.01);
            let bloom = Bloom::build_from_key_hashes(&self.hash_keys, 4);
            bloom.encode(&mut output);
            Some(bloom)
        } else {
            None
        };
        output.put_u32(bloom_offset);

        let bloom_offset = output.len();

        let file = FileObject::create(path.as_ref(), output)?;
        let first_key = self.meta[0].first_key.clone();
        let last_key = self.meta.last().unwrap().last_key.clone();

        Ok(SsTable {
            file,
            block_meta: self.meta,
            block_meta_offset: meta_offset,
            id,
            block_cache,
            first_key,
            last_key,
            bloom,
            max_ts: 0,
        })
    }

    #[cfg(test)]
    pub(crate) fn build_for_test(self, path: impl AsRef<Path>) -> Result<SsTable> {
        self.build(0, Some(Arc::new(BlockCache::new(100))), path)
    }
}
