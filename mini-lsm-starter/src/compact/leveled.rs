use serde::{Deserialize, Serialize};

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

    //此实现要求入参sst对应的keyRange是单调增的
    fn find_overlapping_ssts(
        &self,
        _snapshot: &LsmStorageState,
        _sst_ids: &[usize],
        _in_level: usize,
    ) -> Vec<usize> {
        let ssts = _sst_ids
            .iter()
            .map(|sst_id| _snapshot.sstables.get(sst_id))
            .filter(|op| op.is_some())
            .map(|op| op.unwrap().clone())
            .collect::<Vec<_>>();
        if ssts.is_empty() {
            return vec![];
        }
        let min = ssts.iter().map(|sst| sst.first_key()).min().unwrap();
        let max = ssts.iter().map(|sst| sst.last_key()).max().unwrap();
        let target_ssts = _snapshot.levels[_in_level - 1]
            .1
            .iter()
            .map(|sst_id| _snapshot.sstables.get(sst_id))
            .filter(|op| op.is_some())
            .map(|op| op.unwrap().clone())
            .collect::<Vec<_>>();
        let start = target_ssts.partition_point(|sst| sst.last_key() < min);
        let mut end = start;
        while end < target_ssts.len() && target_ssts[end].first_key() <= max {
            end = end + 1;
        }
        return (start..end)
            .map(|idx| target_ssts.get(idx).unwrap().sst_id())
            .collect();
    }

    pub fn generate_compaction_task(
        &self,
        _snapshot: &LsmStorageState,
    ) -> Option<LeveledCompactionTask> {
        let mut real_size = vec![];
        for i in 0..self.options.max_levels {
            let size = _snapshot.levels[i].1.iter().fold(0, |acc, id| {
                acc + _snapshot.sstables.get(id).unwrap().table_size()
            });
            real_size.push(size);
        }
        println!("genate task real size: {:?}", real_size);
        // 1.基于两条规则计算target_size
        // 1.1只有一层的targetsize < base_level_size_mb
        // 1.2 最底层的size >= base_level_size_mb, 此后每往上一层变为 1/level_size_multiplier
        let mut target_size = (0..self.options.max_levels)
            .into_iter()
            .map(|_| 0)
            .collect::<Vec<u64>>();
        let target_size_option: u64 = self.options.base_level_size_mb as u64 * 1024 * 1024;
        let base_size_option = self.options.base_level_size_mb as u64 * 1024 * 1024;
        let last_target_size = real_size.last().unwrap().clone().max(target_size_option);
        std::mem::replace(target_size.last_mut().unwrap(), last_target_size.clone());

        for i in (1..self.options.max_levels).rev() {
            let cur_level_target_size = target_size[i];
            if cur_level_target_size > base_size_option && real_size[i] >= base_size_option {
                target_size[i - 1] =
                    cur_level_target_size / (self.options.level_size_multiplier as u64);
            } else {
                break;
            }
        }
        println!("genate task target size: {:?}", target_size);

        let base_idx = target_size.partition_point(|size| *size == 0);
        let l0_size = _snapshot.l0_sstables.len();
        // l0 flush to base level
        if l0_size >= self.options.level0_file_num_compaction_trigger {
            let lower_level_sst_ids =
                self.find_overlapping_ssts(_snapshot, &_snapshot.l0_sstables, base_idx + 1);
            Some(LeveledCompactionTask {
                upper_level: None,
                upper_level_sst_ids: _snapshot.l0_sstables.clone(),
                lower_level: base_idx + 1,
                lower_level_sst_ids,
                is_lower_level_bottom_level: base_idx + 1 == self.options.max_levels,
            })
        } else {
            // under l0, 选出和target_size ratio最大的level,向下compact
            let ratios = real_size
                .iter()
                .enumerate()
                .map(|(idx, real_size)| {
                    if *real_size == 0 {
                        0.0
                    } else {
                        (*real_size as f64) / (target_size[idx] as f64)
                    }
                })
                .collect::<Vec<f64>>();

            let mut max_idx = 0;
            let mut max = 0.0;
            for i in 0..ratios.len() {
                if ratios[i] <= 1.0 {
                    continue;
                } else if ratios[i] > max {
                    max_idx = i;
                    max = ratios[i];
                }
            }
            if max == 0.0 {
                return None;
            }
            // 上层选出最老(sst_id 最小的sst), find下层overlap
            let upper = _snapshot.levels[max_idx]
                .1
                .iter()
                .min()
                .expect("no shit found")
                .clone();
            let upper_level_sst_ids = vec![upper];
            let lower_level_sst_ids =
                self.find_overlapping_ssts(_snapshot, &upper_level_sst_ids, max_idx + 2);
            Some(LeveledCompactionTask {
                upper_level: Some(max_idx + 1),
                upper_level_sst_ids,
                lower_level: max_idx + 2,
                lower_level_sst_ids,
                is_lower_level_bottom_level: base_idx + 1 == self.options.max_levels,
            })
        }
    }

    pub fn apply_compaction_result(
        &self,
        _snapshot: &LsmStorageState,
        _task: &LeveledCompactionTask,
        _output: &[usize],
        _in_recovery: bool,
    ) -> (LsmStorageState, Vec<usize>) {
        let mut clone = _snapshot.clone();
        if _task.upper_level.is_none() {
            let new_l0 = clone
                .l0_sstables
                .iter()
                .filter(|id| !_task.upper_level_sst_ids.contains(id))
                .map(|x| *x)
                .collect::<Vec<_>>();
            clone.l0_sstables = new_l0;
        } else {
            let prev_upper = &clone.levels[_task.upper_level.unwrap() - 1].1;
            let mut new_upper = prev_upper
                .iter()
                .filter(|sst_id| !_task.upper_level_sst_ids.contains(&sst_id))
                .copied()
                .collect::<Vec<_>>();
            if !_in_recovery {
                new_upper.sort_by_key(|sst_id| _snapshot.sstables.get(sst_id).unwrap().first_key());
            }
            clone.levels[_task.upper_level.unwrap() - 1].1 = new_upper;
        }
        let prev_lower = &clone.levels[_task.lower_level - 1].1;
        let mut new_lower = prev_lower
            .iter()
            .filter(|id| !_task.lower_level_sst_ids.contains(&id))
            .copied()
            .collect::<Vec<_>>();
        // 此处必须先插入后排序,因为可能没有交集,导致不能用交集的idx定位output的位置
        new_lower.extend(_output);
        // recover阶段在进行完所有的恢复对，对各个level在外层统一排序
        if !_in_recovery {
            new_lower.sort_by_key(|sst_id| _snapshot.sstables.get(sst_id).unwrap().first_key());
        }

        clone.levels[_task.lower_level - 1].1 = new_lower;
        let mut remove_files = vec![];
        remove_files.extend(&_task.upper_level_sst_ids);
        remove_files.extend(&_task.lower_level_sst_ids);
        (clone, remove_files)
    }
}
