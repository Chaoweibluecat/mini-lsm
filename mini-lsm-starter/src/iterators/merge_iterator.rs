#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use std::cmp::{self};
use std::collections::binary_heap::PeekMut;
use std::collections::BinaryHeap;
use std::thread::current;

use anyhow::{Ok, Result};

use crate::key::KeySlice;

use super::StorageIterator;

struct HeapWrapper<I: StorageIterator>(pub usize, pub Box<I>);

impl<I: StorageIterator> PartialEq for HeapWrapper<I> {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == cmp::Ordering::Equal
    }
}

impl<I: StorageIterator> Eq for HeapWrapper<I> {}

impl<I: StorageIterator> PartialOrd for HeapWrapper<I> {
    fn partial_cmp(&self, other: &Self) -> Option<cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl<I: StorageIterator> Ord for HeapWrapper<I> {
    fn cmp(&self, other: &Self) -> cmp::Ordering {
        self.1
            .key()
            .cmp(&other.1.key())
            .then(self.0.cmp(&other.0))
            .reverse()
    }
}

/// Merge multiple iterators of the same type. If the same key occurs multiple times in some
/// iterators, prefer the one with smaller index.
pub struct MergeIterator<I: StorageIterator> {
    iters: BinaryHeap<HeapWrapper<I>>,
    current: Option<HeapWrapper<I>>,
}

impl<I: StorageIterator> MergeIterator<I> {
    pub fn create(iters: Vec<Box<I>>) -> Self {
        let mut heap = BinaryHeap::new();
        for (i, iter) in iters.into_iter().enumerate() {
            if iter.is_valid() {
                heap.push(HeapWrapper { 0: i, 1: iter });
            }
        }
        let cur = heap.pop();
        Self {
            iters: heap,
            current: cur,
        }
    }
}

impl<I: 'static + for<'a> StorageIterator<KeyType<'a> = KeySlice<'a>>> StorageIterator
    for MergeIterator<I>
{
    type KeyType<'a> = KeySlice<'a>;

    fn num_active_iterators(&self) -> usize {
        self.iters.len() + (if self.current.is_some() { 1 } else { 0 })
    }

    fn key(&self) -> KeySlice {
        self.current.as_ref().unwrap().1.key()
    }

    fn value(&self) -> &[u8] {
        self.current.as_ref().unwrap().1.value()
    }

    fn is_valid(&self) -> bool {
        self.current.is_some()
            && (self.current.as_ref().unwrap().1.is_valid()
                || self.iters.iter().any(|iter| iter.1.is_valid()))
    }

    fn next(&mut self) -> Result<()> {
        let cur_ref = self.current.as_mut().unwrap();
        let cur_key = cur_ref.1.key();

        // 处理 同key但是index不同的情况（一个key在memtable内有不同拷贝）
        while let Some(mut top) = self.iters.peek_mut() {
            if top.1.key() == cur_key {
                // 出现异常时,iter不再合法,如果重新入堆,会导致访问非法iter的key,此处手动进行pop,并返回异常
                if let e @ Err(_) = top.1.next() {
                    PeekMut::pop(top);
                    return e;
                // 同理,当前iter走完了,也不能放回堆里,放回堆重组过程一定会调用iter.key(),这里可能就会数组越界
                } else if !top.1.is_valid() {
                    PeekMut::pop(top);
                }
            } else {
                break;
            }
        }
        cur_ref.1.next()?;

        if !cur_ref.1.is_valid() {
            self.current = self.iters.pop();
            return Ok(());
        }

        //必要时更新cur值,这里直接和堆顶inplace swap即可, peekMut结束后,堆自动重组
        if let Some(mut top) = self.iters.peek_mut() {
            if *top > *cur_ref {
                std::mem::swap(&mut (*top), cur_ref);
            }
        }
        Ok(())
    }
}
