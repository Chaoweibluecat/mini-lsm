#![allow(dead_code)] // REMOVE THIS LINE after fully implementing this functionality

mod leveled;
mod simple_leveled;
mod tiered;

use std::collections::HashMap;
use std::iter;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Ok, Result};
use bytes::BufMut;
pub use leveled::{LeveledCompactionController, LeveledCompactionOptions, LeveledCompactionTask};
use serde::{Deserialize, Serialize};
pub use simple_leveled::{
    SimpleLeveledCompactionController, SimpleLeveledCompactionOptions, SimpleLeveledCompactionTask,
};
pub use tiered::{TieredCompactionController, TieredCompactionOptions, TieredCompactionTask};

use crate::iterators::concat_iterator::SstConcatIterator;
use crate::iterators::merge_iterator::MergeIterator;
use crate::iterators::two_merge_iterator::TwoMergeIterator;
use crate::iterators::StorageIterator;
use crate::key::KeySlice;
use crate::lsm_storage::{LsmStorageInner, LsmStorageState};
use crate::table::{SsTable, SsTableBuilder, SsTableIterator};

#[derive(Debug, Serialize, Deserialize)]
pub enum CompactionTask {
    Leveled(LeveledCompactionTask),
    Tiered(TieredCompactionTask),
    Simple(SimpleLeveledCompactionTask),
    ForceFullCompaction {
        l0_sstables: Vec<usize>,
        l1_sstables: Vec<usize>,
    },
}

impl CompactionTask {
    fn compact_to_bottom_level(&self) -> bool {
        match self {
            CompactionTask::ForceFullCompaction { .. } => true,
            CompactionTask::Leveled(task) => task.is_lower_level_bottom_level,
            CompactionTask::Simple(task) => task.is_lower_level_bottom_level,
            CompactionTask::Tiered(task) => task.bottom_tier_included,
        }
    }
}

pub(crate) enum CompactionController {
    Leveled(LeveledCompactionController),
    Tiered(TieredCompactionController),
    Simple(SimpleLeveledCompactionController),
    NoCompaction,
}

impl CompactionController {
    pub fn generate_compaction_task(&self, snapshot: &LsmStorageState) -> Option<CompactionTask> {
        match self {
            CompactionController::Leveled(ctrl) => ctrl
                .generate_compaction_task(snapshot)
                .map(CompactionTask::Leveled),
            CompactionController::Simple(ctrl) => ctrl
                .generate_compaction_task(snapshot)
                .map(CompactionTask::Simple),
            CompactionController::Tiered(ctrl) => ctrl
                .generate_compaction_task(snapshot)
                .map(CompactionTask::Tiered),
            CompactionController::NoCompaction => unreachable!(),
        }
    }

    pub fn apply_compaction_result(
        &self,
        snapshot: &LsmStorageState,
        task: &CompactionTask,
        output: &[usize],
        in_recovery: bool,
    ) -> (LsmStorageState, Vec<usize>) {
        match (self, task) {
            (CompactionController::Leveled(ctrl), CompactionTask::Leveled(task)) => {
                ctrl.apply_compaction_result(snapshot, task, output, in_recovery)
            }
            (CompactionController::Simple(ctrl), CompactionTask::Simple(task)) => {
                ctrl.apply_compaction_result(snapshot, task, output)
            }
            (CompactionController::Tiered(ctrl), CompactionTask::Tiered(task)) => {
                ctrl.apply_compaction_result(snapshot, task, output)
            }
            _ => unreachable!(),
        }
    }
}

impl CompactionController {
    pub fn flush_to_l0(&self) -> bool {
        matches!(
            self,
            Self::Leveled(_) | Self::Simple(_) | Self::NoCompaction
        )
    }
}

#[derive(Debug, Clone)]
pub enum CompactionOptions {
    /// Leveled compaction with partial compaction + dynamic level support (= RocksDB's Leveled
    /// Compaction)
    Leveled(LeveledCompactionOptions),
    /// Tiered compaction (= RocksDB's universal compaction)
    Tiered(TieredCompactionOptions),
    /// Simple leveled compaction
    Simple(SimpleLeveledCompactionOptions),
    /// In no compaction mode (week 1), always flush to L0
    NoCompaction,
}

impl LsmStorageInner {
    fn compact_by_iter(
        &self,
        mut iter: impl for<'a> StorageIterator<KeyType<'a> = KeySlice<'a>>,
    ) -> Result<Vec<Arc<SsTable>>> {
        let mut cur = SsTableBuilder::new(self.options.block_size);
        let mut result = vec![];
        while iter.is_valid() {
            // full compact,删除的key不需要保留
            if !iter.value().is_empty() {
                cur.add(iter.key(), iter.value());
                if cur.estimated_size() > self.options.target_sst_size {
                    let sst_id = self.next_sst_id();
                    let ss_table = cur.build(
                        sst_id,
                        Some(self.block_cache.clone()),
                        self.path_of_sst(sst_id),
                    )?;
                    result.push(Arc::new(ss_table));
                    cur = SsTableBuilder::new(self.options.block_size);
                }
            }
            iter.next()?;
        }
        if !cur.is_empty() {
            let sst_id = self.next_sst_id();
            let ss_table = cur.build(
                sst_id,
                Some(self.block_cache.clone()),
                self.path_of_sst(sst_id),
            )?;
            result.push(Arc::new(ss_table));
        }
        Ok(result)
    }

    fn force_full_compact(
        &self,
        l0_sstables: &Vec<usize>,
        l1_sstables: &Vec<usize>,
    ) -> Result<Vec<Arc<SsTable>>> {
        // todo remove file
        let mut vec: Vec<usize> = Vec::with_capacity(l0_sstables.len() + l1_sstables.len());
        vec.extend(l0_sstables);
        vec.extend(l1_sstables);
        let snap = {
            let mut iter = vec![];
            let guard = self.state.read();
            for id in &vec {
                iter.push(guard.sstables.get(id).unwrap().clone());
            }
            iter
        };
        let mut iters = vec![];
        for ss_table in snap {
            iters.push(Box::new(SsTableIterator::create_and_seek_to_first(
                ss_table,
            )?));
        }
        let merge_iter = MergeIterator::create(iters);
        self.compact_by_iter(merge_iter)
    }

    fn compact(&self, _task: &CompactionTask) -> Result<Vec<Arc<SsTable>>> {
        match _task {
            CompactionTask::ForceFullCompaction {
                l0_sstables,
                l1_sstables,
            } => self.force_full_compact(l0_sstables, l1_sstables),

            CompactionTask::Leveled(LeveledCompactionTask {
                upper_level,
                upper_level_sst_ids,
                lower_level: _,
                lower_level_sst_ids,
                ..
            })
            | CompactionTask::Simple(SimpleLeveledCompactionTask {
                upper_level,
                upper_level_sst_ids,
                lower_level: _,
                lower_level_sst_ids,
                ..
            }) => {
                let snapshot = {
                    let guard = self.state.read();
                    guard.clone()
                };
                let lower: Vec<Arc<SsTable>> = lower_level_sst_ids
                    .iter()
                    .map(|id| snapshot.sstables.get(id).unwrap().clone())
                    .collect();

                let upper: Vec<Arc<SsTable>> = upper_level_sst_ids
                    .iter()
                    .map(|id| snapshot.sstables.get(id).unwrap().clone())
                    .collect();

                if upper_level.is_none() {
                    let l0 = upper
                        .iter()
                        .map(|sst| {
                            Box::new(
                                SsTableIterator::create_and_seek_to_first(sst.clone())
                                    .expect("fail to generate sst iter"),
                            )
                        })
                        .collect();
                    let l0_iter = MergeIterator::create(l0);
                    let l1_iter: SstConcatIterator =
                        SstConcatIterator::create_and_seek_to_first(lower)
                            .expect("fail to generate l1 sst iter");
                    let two = TwoMergeIterator::create(l0_iter, l1_iter)?;
                    self.compact_by_iter(two)
                } else {
                    let upper_iter = SstConcatIterator::create_and_seek_to_first(upper)
                        .expect("fail to generate l1 sst iter");
                    let lower_iter = SstConcatIterator::create_and_seek_to_first(lower)
                        .expect("fail to generate l1 sst iter");
                    let two = TwoMergeIterator::create(upper_iter, lower_iter)?;
                    self.compact_by_iter(two)
                }
            }
            // tiered没有l0,但是涉及层数可能不止两层, 取所有level的sst_concat的mergeIter即可
            CompactionTask::Tiered(task) => {
                let snapshot = {
                    let guard = self.state.read();
                    guard.clone()
                };
                let mut iters = vec![];
                for (_, ssts) in &task.tiers {
                    let sst_arcs = ssts
                        .iter()
                        .map(|id| snapshot.sstables.get(id).unwrap().clone())
                        .collect::<Vec<_>>();
                    iters.push(Box::new(SstConcatIterator::create_and_seek_to_first(
                        sst_arcs,
                    )?));
                }
                let iter = MergeIterator::create(iters);
                self.compact_by_iter(iter)
            }
            _ => unimplemented!(),
        }
    }

    pub fn force_full_compaction(&self) -> Result<()> {
        let snap = {
            let guard = self.state.read();
            (guard.l0_sstables.clone(), guard.levels[0].1.clone())
        };
        let compacted_sst = self.compact(
            &(CompactionTask::ForceFullCompaction {
                l0_sstables: snap.0.clone(),
                l1_sstables: snap.1.clone(),
            }),
        )?;

        let mutex = self.state_lock.lock();
        let mut write_guard = self.state.write();
        let mut copy = write_guard.as_ref().clone();
        for prev_l1 in &snap.1 {
            copy.sstables.remove(prev_l1);
        }
        for prev_l1 in &snap.0 {
            copy.sstables.remove(prev_l1);
        }
        let new_l1 = compacted_sst.iter().map(|sst| sst.sst_id()).collect();
        for new_sst in compacted_sst {
            copy.sstables.insert(new_sst.sst_id(), new_sst);
        }
        copy.levels[0] = (0, new_l1);
        copy.l0_sstables.clear();
        *write_guard = Arc::new(copy);
        drop(mutex);
        Ok(())
    }

    fn trigger_compaction(&self) -> Result<()> {
        let snapshot = {
            let guard = self.state.read();
            guard.clone()
        };
        let task = self
            .compaction_controller
            .generate_compaction_task(&*snapshot);
        match task {
            None => Ok(()),
            Some(task) => {
                let result = self.compact(&task)?;
                let mut output: Vec<usize> = vec![];
                // lock state,确保这段时间只有自己写state的指针
                let state_mutex = self.state_lock.lock();
                // 需要重读指针再clone内容,(state是cow的, compact期间同时l0 flush会导致指针更新)
                let mut clone = self.state.read().as_ref().clone();
                for sst in &result {
                    clone.sstables.insert(sst.sst_id(), sst.clone());
                    output.push(sst.sst_id());
                }
                let (state, deleted) = self
                    .compaction_controller
                    .apply_compaction_result(&clone, &task, &output, false);
                let mut ref1 = self.state.write();
                *ref1 = Arc::new(state);
                drop(state_mutex);

                for delete_f in deleted {
                    std::fs::remove_file(self.path_of_sst(delete_f));
                }
                Ok(())
            }
        }
    }

    pub(crate) fn spawn_compaction_thread(
        self: &Arc<Self>,
        rx: crossbeam_channel::Receiver<()>,
    ) -> Result<Option<std::thread::JoinHandle<()>>> {
        if let CompactionOptions::Leveled(_)
        | CompactionOptions::Simple(_)
        | CompactionOptions::Tiered(_) = self.options.compaction_options
        {
            let this = self.clone();
            let handle = std::thread::spawn(move || {
                let ticker = crossbeam_channel::tick(Duration::from_millis(50));
                loop {
                    crossbeam_channel::select! {
                        recv(ticker) -> _ => if let Err(e) = this.trigger_compaction() {
                            eprintln!("compaction failed: {}", e);
                        },
                        recv(rx) -> _ => return
                    }
                }
            });
            return Ok(Some(handle));
        }
        Ok(None)
    }

    fn trigger_flush(&self) -> Result<()> {
        // 使用读锁读要不要flush,
        let need_flush = {
            let read = self.state.read();
            read.imm_memtables.len() + 1 > self.options.num_memtable_limit
        };

        if need_flush {
            self.force_flush_next_imm_memtable()?;
        }
        Ok(())
    }

    pub(crate) fn spawn_flush_thread(
        self: &Arc<Self>,
        rx: crossbeam_channel::Receiver<()>,
    ) -> Result<Option<std::thread::JoinHandle<()>>> {
        let this = self.clone();
        let handle = std::thread::spawn(move || {
            let ticker = crossbeam_channel::tick(Duration::from_millis(50));
            loop {
                crossbeam_channel::select! {
                    recv(ticker) -> _ => if let Err(e) = this.trigger_flush() {
                        eprintln!("flush failed: {}", e);
                    },
                    recv(rx) -> _ => return
                }
            }
        });
        Ok(Some(handle))
    }
}
