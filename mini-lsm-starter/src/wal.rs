#![allow(dead_code)] // REMOVE THIS LINE after fully implementing this functionality

use std::io::{BufWriter, Read};
use std::path::Path;
use std::sync::Arc;
use std::{fs::File, io::Write};

use anyhow::{Ok, Result};
use bytes::{Buf, BufMut, Bytes};
use crossbeam_skiplist::SkipMap;
use parking_lot::Mutex;

pub struct Wal {
    file: Arc<Mutex<BufWriter<File>>>,
}

impl Wal {
    pub fn create(_path: impl AsRef<Path>) -> Result<Self> {
        let file = File::options()
            .read(true)
            .append(true)
            .create(true)
            .open(_path)?;
        Ok(Self {
            file: Arc::new(Mutex::new(BufWriter::new(file))),
        })
    }

    pub fn recover(_path: impl AsRef<Path>, _skiplist: &SkipMap<Bytes, Bytes>) -> Result<Self> {
        let mut file = File::open(_path)?;
        let mut buf = Vec::with_capacity(file.metadata()?.len() as usize);
        file.read_to_end(&mut buf)?;
        let mut buf_ptr = &buf[0..];
        while !buf_ptr.is_empty() {
            let mut hasher = crc32fast::Hasher::new();
            hasher.update(&buf_ptr[..2]);
            let key_len = buf_ptr.get_u16();
            let key = Bytes::copy_from_slice(&buf_ptr[0..key_len as usize]);
            hasher.update(&buf_ptr[..key_len as usize]);
            buf_ptr.advance(key_len as usize);
            hasher.update(&buf_ptr[..2]);
            let val_len = buf_ptr.get_u16();
            let val = Bytes::copy_from_slice(&buf_ptr[0..val_len as usize]);
            hasher.update(&buf_ptr[..val_len as usize]);
            let cur_checksum = hasher.finalize();
            buf_ptr.advance(val_len as usize);
            let real_checksum = buf_ptr.get_u32();
            assert_eq!(cur_checksum, real_checksum, "wal checksum mismatch");

            _skiplist.insert(key, val);
        }
        Ok(Self {
            file: Arc::new(Mutex::new(BufWriter::new(file))),
        })
    }

    pub fn put(&self, _key: &[u8], _value: &[u8]) -> Result<()> {
        let mut guard = self.file.lock();
        let mut vec: Vec<u8> = vec![];
        vec.put_u16(_key.len() as u16);
        vec.extend(_key);
        vec.put_u16(_value.len() as u16);
        vec.extend(_value);
        let checksum = crc32fast::hash(&vec);
        vec.put_u32(checksum);
        assert_eq!(vec.len(), guard.get_mut().write(&vec)?);
        Ok(())
    }

    /// Implement this in week 3, day 5.
    pub fn put_batch(&self, _data: &[(&[u8], &[u8])]) -> Result<()> {
        unimplemented!()
    }

    pub fn sync(&self) -> Result<()> {
        let mut guard = self.file.lock();
        guard.flush()?;
        guard.get_mut().sync_all()?;
        Ok(())
    }
}
