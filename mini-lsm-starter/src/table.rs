#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

pub(crate) mod bloom;
mod builder;
mod iterator;

use std::fs::File;
use std::io::{Read, Seek};
use std::os::unix::fs::FileExt;
use std::path::Path;
use std::sync::Arc;

use anyhow::{anyhow, bail, Ok, Result};
pub use builder::SsTableBuilder;
use bytes::{Buf, BufMut, Bytes};
pub use iterator::SsTableIterator;

use crate::block::Block;
use crate::key::{self, KeyBytes, KeySlice};
use crate::lsm_storage::BlockCache;

use self::bloom::Bloom;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BlockMeta {
    /// Offset of this data block.
    pub offset: usize,
    /// The first key of the data block.
    pub first_key: KeyBytes,
    /// The last key of the data block.
    pub last_key: KeyBytes,
}

impl BlockMeta {
    /// Encode block meta to a buffer.
    /// You may add extra fields to the buffer,
    /// in order to help keep track of `first_key` when decoding from the same buffer in the future.
    pub fn encode_block_meta(
        block_meta: &[BlockMeta],
        #[allow(clippy::ptr_arg)] // remove this allow after you finish
        buf: &mut Vec<u8>,
    ) {
        if block_meta.len() == 0 {
            return;
        }
        buf.put_u16(block_meta.len() as u16);
        for meta in block_meta.iter() {
            buf.put_u32(meta.offset as u32);
            buf.put_u16(meta.first_key.len() as u16);
            buf.extend(meta.first_key.as_key_slice().raw_ref());
            buf.put_u16(meta.last_key.len() as u16);
            buf.extend(meta.last_key.as_key_slice().raw_ref());
        }
    }

    /// Decode block meta from a buffer.
    pub fn decode_block_meta(mut buf: impl Buf) -> Vec<BlockMeta> {
        let meta_len: u16 = buf.get_u16();
        let mut ret = Vec::with_capacity(meta_len as usize);
        for i in 0..meta_len {
            let offset = buf.get_u32() as usize;
            let first_key_len = buf.get_u16();
            let first_key = KeyBytes::from_bytes(buf.copy_to_bytes(first_key_len as usize));
            let last_key_len = buf.get_u16();
            let last_key = KeyBytes::from_bytes(buf.copy_to_bytes(last_key_len as usize));
            ret.push(BlockMeta {
                offset,
                first_key,
                last_key,
            });
        }
        ret
    }
}

/// A file object.
pub struct FileObject(Option<File>, u64);

impl FileObject {
    pub fn read(&self, offset: u64, len: u64) -> Result<Vec<u8>> {
        use std::os::unix::fs::FileExt;
        let mut data = vec![0; len as usize];
        self.0
            .as_ref()
            .unwrap()
            .read_exact_at(&mut data[..], offset)?;
        Ok(data)
    }

    pub fn size(&self) -> u64 {
        self.1
    }

    /// Create a new file object (day 2) and write the file to the disk (day 4).
    pub fn create(path: &Path, data: Vec<u8>) -> Result<Self> {
        std::fs::write(path, &data)?;
        File::open(path)?.sync_all()?;
        Ok(FileObject(
            Some(File::options().read(true).write(false).open(path)?),
            data.len() as u64,
        ))
    }

    pub fn open(path: &Path) -> Result<Self> {
        let file = File::options().read(true).write(false).open(path)?;
        let size = file.metadata()?.len();
        Ok(FileObject(Some(file), size))
    }
}

/// An SSTable.
pub struct SsTable {
    /// The actual storage unit of SsTable, the format is as above.
    pub(crate) file: FileObject,
    /// The meta blocks that hold info for data blocks.
    pub(crate) block_meta: Vec<BlockMeta>,
    /// The offset that indicates the start point of meta blocks in `file`.
    pub(crate) block_meta_offset: usize,
    id: usize,
    block_cache: Option<Arc<BlockCache>>,
    first_key: KeyBytes,
    last_key: KeyBytes,
    pub(crate) bloom: Option<Bloom>,
    /// The maximum timestamp stored in this SST, implemented in week 3.
    max_ts: u64,
}

impl SsTable {
    #[cfg(test)]
    pub(crate) fn open_for_test(file: FileObject) -> Result<Self> {
        Self::open(0, None, file)
    }

    /// Open SSTable from a file.
    pub fn open(id: usize, block_cache: Option<Arc<BlockCache>>, file: FileObject) -> Result<Self> {
        let content = file.read(0, file.size())?;

        let bloom_offset = (&content[content.len() - 4..]).get_u32() as usize;

        let bloom = if bloom_offset != content.len() - 4 {
            Some(Bloom::decode(&content[bloom_offset..content.len() - 4])?)
        } else {
            None
        };

        let meta_offset = (&content[bloom_offset - 4..]).get_u32() as usize;
        let block_meta = BlockMeta::decode_block_meta(&content[meta_offset..bloom_offset - 4]);

        let first_key = block_meta[0].first_key.clone();
        let last_key = block_meta.last().unwrap().last_key.clone();
        Ok(SsTable {
            file,
            block_meta,
            block_meta_offset: meta_offset,
            id,
            block_cache: None,
            first_key,
            last_key,
            bloom,
            max_ts: 0,
        })
    }

    /// Create a mock SST with only first key + last key metadata
    pub fn create_meta_only(
        id: usize,
        file_size: u64,
        first_key: KeyBytes,
        last_key: KeyBytes,
    ) -> Self {
        Self {
            file: FileObject(None, file_size),
            block_meta: vec![],
            block_meta_offset: 0,
            id,
            block_cache: None,
            first_key,
            last_key,
            bloom: None,
            max_ts: 0,
        }
    }

    /// Read a block from the disk.
    pub fn read_block(&self, block_idx: usize) -> Result<Arc<Block>> {
        // 通过blockmeta中的当前block和下一段的block offset 确定读出的位置
        let offset = self.block_meta[block_idx].offset;
        let end = self
            .block_meta
            .get(block_idx + 1)
            .map_or(self.block_meta_offset as usize, |meta| meta.offset);
        let len = end - offset;
        let block_data = self.file.read(offset as u64, len as u64)?;
        Ok(Arc::new(Block::decode(&block_data)))
    }

    /// Read a block from disk, with block cache. (Day 4)
    pub fn read_block_cached(&self, block_idx: usize) -> Result<Arc<Block>> {
        if self.block_cache.is_some() {
            let block = self
                .block_cache
                .as_ref()
                .unwrap()
                .try_get_with((self.id, block_idx), || self.read_block(block_idx));
            // anyhow返回anyhow::Error, bail返回的是一个Result
            block.map_err(|e| anyhow!(format!("{}", e)))
        } else {
            self.read_block(block_idx)
        }
    }

    /// Find the block that may contain `key`.
    /// Note: You may want to make use of the `first_key` stored in `BlockMeta`.
    /// You may also assume the key-value pairs stored in each consecutive block are sorted.
    pub fn find_block_idx(&self, key: KeySlice) -> usize {
        // 牛大了,什么api大王
        self.block_meta
            .partition_point(|meta| meta.first_key.as_key_slice() <= key)
            .saturating_sub(1) // pp返回的是第二个分区的第一个idx,(即第一个大于key的block_idx),此时需要-1
    }

    /// Get number of data blocks.
    pub fn num_of_blocks(&self) -> usize {
        self.block_meta.len()
    }

    pub fn first_key(&self) -> &KeyBytes {
        &self.first_key
    }

    pub fn last_key(&self) -> &KeyBytes {
        &self.last_key
    }

    pub fn table_size(&self) -> u64 {
        self.file.1
    }

    pub fn sst_id(&self) -> usize {
        self.id
    }

    pub fn max_ts(&self) -> u64 {
        self.max_ts
    }
}
