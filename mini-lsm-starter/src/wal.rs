#![allow(dead_code)] // REMOVE THIS LINE after fully implementing this functionality

use std::io::{BufWriter, Read};
use std::path::Path;
use std::sync::Arc;
use std::{fs::File, io::Write};

use anyhow::{Ok, Result};
use bytes::{Buf, BufMut, Bytes};
use crossbeam_skiplist::SkipMap;
use parking_lot::Mutex;

use crate::key::{KeyBytes, KeySlice};

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

    pub fn recover(_path: impl AsRef<Path>, _skiplist: &SkipMap<KeyBytes, Bytes>) -> Result<Self> {
        let mut file = File::open(_path)?;
        let mut buf = Vec::with_capacity(file.metadata()?.len() as usize);
        file.read_to_end(&mut buf)?;
        let mut buf_ptr = &buf[0..];
        while !buf_ptr.is_empty() {
            let batch_size = buf_ptr.get_u32();
            let mut hasher = crc32fast::Hasher::new();
            let mut kvs = vec![];
            for _ in 0..batch_size {
            hasher.update(&buf_ptr[..2]);
            let key_len = buf_ptr.get_u16();
            //key_data
            let key = Bytes::copy_from_slice(&buf_ptr[0..key_len as usize]);
            hasher.update(&buf_ptr[..key_len as usize]);
            buf_ptr.advance(key_len as usize);
            //ts
            hasher.update(&buf_ptr[..8]);
            let ts = buf_ptr.get_u64();
            //val
            hasher.update(&buf_ptr[..2]);
            let val_len = buf_ptr.get_u16();
            let val = Bytes::copy_from_slice(&buf_ptr[0..val_len as usize]);
            hasher.update(&buf_ptr[..val_len as usize]);
            buf_ptr.advance(val_len as usize);
            kvs.push((KeyBytes::from_bytes_with_ts(key, ts), val));
            }
            let real_checksum = buf_ptr.get_u32();
            let cur_checksum = hasher.finalize();
            assert_eq!(real_checksum, cur_checksum, "wal checksum doesn't match");
            for (k, v) in kvs {
            _skiplist.insert(k,v);
            }
        }
        Ok(Self {
            file: Arc::new(Mutex::new(BufWriter::new(file))),
        })
    }

    pub fn put(&self, key: KeySlice, value: &[u8]) -> Result<()> {
        // let mut guard = self.file.lock();
        // let mut vec: Vec<u8> = vec![];
        // vec.put_u16(key.key_len() as u16);
        // vec.extend(key.key_ref());
        // vec.put_u64(key.ts());
        // vec.put_u16(value.len() as u16);
        // vec.extend(value);
        // let checksum = crc32fast::hash(&vec);
        // vec.put_u32(checksum);
        // assert_eq!(vec.len(), guard.get_mut().write(&vec)?);
        let mut key_bytes = key.key_ref().to_vec();
        key_bytes.put_u64(key.ts());
        self.put_batch(&vec![(&key_bytes[0..], value)])
    }

    /// Implement this in week 3, day 5.
    pub fn put_batch(&self, data: &[(&[u8], &[u8])]) -> Result<()> {
        let mut guard = self.file.lock();
        let mut vec: Vec<u8> = vec![];
        let batch_size = data.len() as u32;
        vec.put_u32(batch_size);
        for &(key, value) in data {
        vec.put_u16((key.len() - 8) as u16);
        vec.extend(key);
        vec.put_u16(value.len() as u16);
        vec.extend(value);
        }
        let checksum = crc32fast::hash(&vec[4..]);
        vec.put_u32(checksum);
        assert_eq!(vec.len(), guard.get_mut().write(&vec)?);
        Ok(())
        
    }

    pub fn sync(&self) -> Result<()> {
        let mut guard = self.file.lock();
        guard.flush()?;
        guard.get_mut().sync_all()?;
        Ok(())
    }
}
