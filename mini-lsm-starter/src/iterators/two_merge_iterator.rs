#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use anyhow::{Ok, Result};

use super::StorageIterator;

/// Merges two iterators of different types into one. If the two iterators have the same key, only
/// produce the key once and prefer the entry from A.
pub struct TwoMergeIterator<A: StorageIterator, B: StorageIterator> {
    a: A,
    b: B,
    cur: u8,
}

impl<
        A: 'static + StorageIterator,
        B: 'static + for<'a> StorageIterator<KeyType<'a> = A::KeyType<'a>>,
    > TwoMergeIterator<A, B>
{
    pub fn create(a: A, b: B) -> Result<Self> {
        let cur = if !a.is_valid() {
            1
        } else if !b.is_valid() {
            0
        } else {
            if a.key() <= b.key() {
                0
            } else {
                1
            }
        };
        Ok(Self { a, b, cur })
    }

    pub fn use_A(&self) -> bool {
        self.cur == 0
    }
}

impl<
        A: 'static + StorageIterator,
        B: 'static + for<'a> StorageIterator<KeyType<'a> = A::KeyType<'a>>,
    > StorageIterator for TwoMergeIterator<A, B>
{
    type KeyType<'a> = A::KeyType<'a>;

    fn key(&self) -> Self::KeyType<'_> {
        if self.cur == 0 {
            self.a.key()
        } else {
            self.b.key()
        }
    }

    fn value(&self) -> &[u8] {
        if self.cur == 0 {
            self.a.value()
        } else {
            self.b.value()
        }
    }

    fn is_valid(&self) -> bool {
        self.a.is_valid() || self.b.is_valid()
    }

    fn next(&mut self) -> Result<()> {
        if self.cur == 0 {
            // 处理 a.key = b.key的情况,此时需要一起advance
            if self.b.is_valid() && self.a.key() == self.b.key() {
                self.b.next();
            }
            self.a.next()?;

            if self.a.is_valid() {
                self.cur = if !self.b.is_valid() || self.a.key() <= self.b.key() {
                    0
                } else {
                    1
                };
            } else {
                self.cur = 1;
            }
        } else {
            self.b.next()?;
            if self.b.is_valid() {
                self.cur = if !self.a.is_valid() || self.b.key() < self.a.key() {
                    1
                } else {
                    0
                };
            } else {
                self.cur = 0;
            }
        }
        Ok(())
    }

    fn num_active_iterators(&self) -> usize {
        self.a.num_active_iterators() + self.b.num_active_iterators()
    }
}
