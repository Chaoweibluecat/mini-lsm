#![allow(dead_code)] // REMOVE THIS LINE after fully implementing this functionality

use std::io::{ Read, SeekFrom, Write };
use std::ops::Deref;
use std::{ fs::File, os::windows::fs::FileExt };
use std::path::Path;
use std::sync::Arc;

use anyhow::Result;
use bytes::{ Buf, BufMut };
use parking_lot::{ Mutex, MutexGuard };
use serde::{ Deserialize, Serialize };
use serde_json::de::IoRead;

use crate::compact::CompactionTask;

pub struct Manifest {
    file: Arc<Mutex<File>>,
}

#[derive(Serialize, Deserialize)]
pub enum ManifestRecord {
    Flush(usize),
    NewMemtable(usize),
    Compaction(CompactionTask, Vec<usize>),
}

impl Manifest {
    pub fn create(_path: impl AsRef<Path>) -> Result<Self> {
        let file = File::options().read(true).append(true).create(true).open(_path)?;

        Ok(Self {
            file: Arc::new(Mutex::new(file)),
        })
    }

    pub fn recover(_path: impl AsRef<Path>) -> Result<(Self, Vec<ManifestRecord>)> {
        let mut file = File::open(_path)?;
        let len = file.metadata()?.len() as usize;
        let mut vec = Vec::with_capacity(len);
        file.read_to_end(&mut vec);
        let mut res = vec![];
        let mut vec_ptr = &vec[0..];
        while !vec_ptr.is_empty() {
            let len = vec_ptr.get_u64();
            vec_ptr.advance(8);
            let record = serde_json::from_slice::<ManifestRecord>(vec_ptr)?;
            res.push(record);
            vec_ptr.advance(len as usize);
            break;
        }
        Ok((
            Self {
                file: Arc::new(Mutex::new(file)),
            },
            res,
        ))
    }

    pub fn add_record(
        &self,
        _state_lock_observer: &MutexGuard<()>,
        record: ManifestRecord
    ) -> Result<()> {
        self.add_record_when_init(record)
    }

    pub fn add_record_when_init(&self, _record: ManifestRecord) -> Result<()> {
        let mut guard = self.file.lock();
        let buf = serde_json::to_vec(&_record)?;
        guard.write_all(&(buf.len() as u64).to_be_bytes())?;
        guard.write_all(&buf);
        guard.sync_all();
        Ok(())
    }
}
