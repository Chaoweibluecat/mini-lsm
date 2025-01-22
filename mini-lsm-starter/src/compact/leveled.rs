use serde::{ Deserialize, Serialize };

use crate::lsm_storage::LsmStorageState;

#[derive(Debug, Serialize, Deserialize)]
pub struct LeveledCompactionTask {
    // if upper_level is `None`, then it is L0 compaction
    pub upper_level: Option<usize>,
    pub upper_level_sst_ids: Vec<usize>,
    pub lower_level: usize,
    pub lower_level_sst_ids: Vec<usize>,
    pub is_lower_level_bottom_level: bool,
}

#[derive(Debug, Clone)]
pub struct LeveledCompactionOptions {
    pub level_size_multiplier: usize,
    pub level0_file_num_compaction_trigger: usize,
    pub max_levels: usize,
    pub base_level_size_mb: usize,
}

pub struct LeveledCompactionController {
    options: LeveledCompactionOptions,
}

impl LeveledCompactionController {
    pub fn new(options: LeveledCompactionOptions) -> Self {
        Self { options }
    }

    fn find_overlapping_ssts(
        &self,
        _snapshot: &LsmStorageState,
        _sst_ids: &[usize],
        _in_level: usize
    ) -> Vec<usize> {
        unimplemented!()
    }

    pub fn generate_compaction_task(
        &self,
        _snapshot: &LsmStorageState
    ) -> Option<LeveledCompactionTask> {
        let mut real_size = vec![];
        for i in 0..self.options.max_levels {
            let size = _snapshot.levels[i].1
                .iter()
                .fold(0, |acc, id| { acc + _snapshot.sstables.get(id).unwrap().table_size() });
            real_size.push(size);
        }
        // 1.计算target_size,
        let mut target_size = (0..self.options.max_levels)
            .into_iter()
            .map(|_| 0)
            .collect::<Vec<u64>>();
        let last_target_size = real_size
            .last()
            .unwrap()
            .clone()
            .max(self.options.base_level_size_mb as u64);

        std::mem::replace(target_size.last_mut().unwrap(), last_target_size.clone());

        for i in 1..self.options.max_levels {
            let cur_level = real_size.len() - 1 - i;
            let cur_level_target_size = target_size[cur_level];
            if cur_level_target_size >= (self.options.base_level_size_mb as u64) {
                target_size[cur_level - 1] =
                    cur_level_target_size / (self.options.level_size_multiplier as u64);
            } else {
                break;
            }
        }
        let base_idx = target_size.partition_point(|size| *size == 0);
        let l0_size = _snapshot.l0_sstables
            .iter()
            .fold(0, |acc, id| { acc + _snapshot.sstables.get(id).unwrap().table_size() });
        let l0_sst = _snapshot.l0_sstables.last().unwrap().clone();
        if l0_size >= (self.options.level0_file_num_compaction_trigger as u64) {
            let lower_level_sst_ids = self.find_overlapping_ssts(
                _snapshot,
                &_snapshot.l0_sstables[_snapshot.l0_sstables.len() - 1..],
                base_idx + 1
            );

            Some(LeveledCompactionTask {
                upper_level: None,
                upper_level_sst_ids: vec![],
                lower_level: base_idx + 1,
                lower_level_sst_ids,
                is_lower_level_bottom_level: base_idx == self.options.max_levels,
            })
        } else {
            None
        }
    }

    pub fn apply_compaction_result(
        &self,
        _snapshot: &LsmStorageState,
        _task: &LeveledCompactionTask,
        _output: &[usize],
        _in_recovery: bool
    ) -> (LsmStorageState, Vec<usize>) {
        unimplemented!()
    }
}
