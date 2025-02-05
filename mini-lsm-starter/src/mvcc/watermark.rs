#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use std::collections::BTreeMap;

pub struct Watermark {
    readers: BTreeMap<u64, usize>,
}

impl Watermark {
    pub fn new() -> Self {
        Self {
            readers: BTreeMap::new(),
        }
    }

    pub fn add_reader(&mut self, ts: u64) {
        self.readers.insert(ts, 0);
    }

    pub fn remove_reader(&mut self, ts: u64) {
        self.readers.remove(&ts);
    }

    pub fn watermark(&self) -> Option<u64> {
        self.readers.first_key_value().map(|e| e.0.clone())
    }
}
