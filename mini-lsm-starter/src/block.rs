#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

mod builder;
mod iterator;

pub use builder::BlockBuilder;
use bytes::{Buf, BufMut, Bytes};
pub use iterator::BlockIterator;

/// A block is the smallest unit of read and caching in LSM tree. It is a collection of sorted key-value pairs.
pub struct Block {
    pub(crate) data: Vec<u8>,
    pub(crate) offsets: Vec<u16>,
}

impl Block {
    /// Encode the internal data to the data layout illustrated in the tutorial
    /// Note: You may want to recheck if any of the expected field is missing from your output
    pub fn encode(&self) -> Bytes {
        // tood 学习一下Bytes::from
        let mut vec: Vec<u8> = self.data.clone();
        for offset in &self.offsets {
            vec.put_u16(*offset);
        }
        vec.put_u16(self.offsets.len() as u16);
        // vec.into()
        let ret = Bytes::from(vec);
        ret
        // Bytes::copy_from_slice(&vec)
    }

    /// Decode from the data layout, transform the input `data` to a single `Block`
    pub fn decode(data: &[u8]) -> Self {
        // get number of elements in the block
        let entry_offsets_len = (&data[data.len() - 2..]).get_u16() as usize;
        let data_end = data.len() - 2 - entry_offsets_len * 2;
        let offsets_raw = &data[data_end..data.len() - 2];
        // get offset array
        let offsets = offsets_raw.chunks(2).map(|mut x| x.get_u16()).collect();
        // retrieve data
        let data = data[0..data_end].to_vec();
        Self { data, offsets }
    }
}
