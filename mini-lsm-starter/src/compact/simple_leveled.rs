use std::{collections::HashSet, sync::Arc};

use serde::{Deserialize, Serialize};

use crate::{
    iterators::{
        concat_iterator::SstConcatIterator, merge_iterator::MergeIterator,
        two_merge_iterator::TwoMergeIterator, StorageIterator,
    },
    lsm_storage::LsmStorageState,
    table::{SsTable, SsTableIterator},
};

#[derive(Debug, Clone)]
pub struct SimpleLeveledCompactionOptions {
    pub size_ratio_percent: usize,
    pub level0_file_num_compaction_trigger: usize,
    pub max_levels: usize,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct SimpleLeveledCompactionTask {
    // if upper_level is `None`, then it is L0 compaction
    pub upper_level: Option<usize>,
    pub upper_level_sst_ids: Vec<usize>,
    pub lower_level: usize,
    pub lower_level_sst_ids: Vec<usize>,
    pub is_lower_level_bottom_level: bool,
}

pub struct SimpleLeveledCompactionController {
    options: SimpleLeveledCompactionOptions,
}

impl SimpleLeveledCompactionController {
    pub fn new(options: SimpleLeveledCompactionOptions) -> Self {
        Self { options }
    }

    /// Generates a compaction task.
    ///
    /// Returns `None` if no compaction needs to be scheduled. The order of SSTs in the compaction task id vector matters.
    pub fn generate_compaction_task(
        &self,
        _snapshot: &LsmStorageState,
    ) -> Option<SimpleLeveledCompactionTask> {
        if _snapshot.l0_sstables.len() >= self.options.level0_file_num_compaction_trigger
            && ((_snapshot.levels[0].1.len() as f64) / (_snapshot.l0_sstables.len() as f64)
                < (self.options.size_ratio_percent as f64) / 100.0)
        {
            Some(SimpleLeveledCompactionTask {
                upper_level: None,
                upper_level_sst_ids: _snapshot.l0_sstables.clone(),
                lower_level: 1,
                lower_level_sst_ids: _snapshot.levels[0].1.clone(),
                is_lower_level_bottom_level: false,
            })
        } else {
            let mut i = 0;
            let level = loop {
                // i = base层
                if i >= self.options.max_levels - 1 {
                    break None;
                }
                if _snapshot.levels[i].1.is_empty() {
                    i = i + 1;
                    continue;
                }
                let ratio =
                    (_snapshot.levels[i + 1].1.len() as f64) / (_snapshot.levels[i].1.len() as f64);
                if ratio < (self.options.size_ratio_percent as f64) / 100.0 {
                    break Some(i);
                }
                i = i + 1;
            };
            match level {
                None => None,
                Some(i) => Some(SimpleLeveledCompactionTask {
                    upper_level: Some(i + 1),
                    upper_level_sst_ids: _snapshot.levels[i].1.clone(),
                    lower_level: i + 2,
                    lower_level_sst_ids: _snapshot.levels[i + 1].1.clone(),
                    is_lower_level_bottom_level: i + 1 == self.options.max_levels,
                }),
            }
        }
    }

    /// Apply the compaction result.
    ///
    /// The compactor will call this function with the compaction task and the list of SST ids generated. This function applies the
    /// result and generates a new LSM state. The functions should only change `l0_sstables` and `levels` without changing memtables
    /// and `sstables` hash map. Though there should only be one thread running compaction jobs, you should think about the case
    /// where an L0 SST gets flushed while the compactor generates new SSTs, and with that in mind, you should do some sanity checks
    /// in your implementation.
    pub fn apply_compaction_result(
        &self,
        _snapshot: &LsmStorageState,
        _task: &SimpleLeveledCompactionTask,
        _output: &[usize],
    ) -> (LsmStorageState, Vec<usize>) {
        let mut clone = _snapshot.clone();
        let mut remove: Vec<usize> = vec![];
        for sst in _task
            .lower_level_sst_ids
            .iter()
            .chain(_task.upper_level_sst_ids.iter())
        {
            clone.sstables.remove(&sst);
            remove.push(*sst);
        }

        if let Some(upper) = _task.upper_level {
            clone.levels[upper - 1].1.clear();
        } else {
            // 可能有l0 flush同时发生,需要remove而不是clear
            let snap_l0 = _snapshot.l0_sstables.iter().collect::<HashSet<_>>();
            let new_l0 = clone
                .l0_sstables
                .iter()
                .filter(|sst| !snap_l0.contains(sst))
                .copied()
                .collect::<Vec<_>>();
            clone.l0_sstables = new_l0;
        }
        clone.levels[_task.lower_level - 1].1.clear();
        clone.levels[_task.lower_level - 1].1.extend(_output);
        (clone, remove)
    }
}
