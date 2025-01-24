use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use crate::lsm_storage::LsmStorageState;

#[derive(Debug, Serialize, Deserialize)]
pub struct TieredCompactionTask {
    pub tiers: Vec<(usize, Vec<usize>)>,
    pub bottom_tier_included: bool,
}

#[derive(Debug, Clone)]
pub struct TieredCompactionOptions {
    pub num_tiers: usize,
    pub max_size_amplification_percent: usize,
    pub size_ratio: usize,
    pub min_merge_width: usize,
    pub max_merge_width: Option<usize>,
}

pub struct TieredCompactionController {
    options: TieredCompactionOptions,
}

impl TieredCompactionController {
    pub fn new(options: TieredCompactionOptions) -> Self {
        Self { options }
    }

    pub fn generate_compaction_task(
        &self,
        _snapshot: &LsmStorageState,
    ) -> Option<TieredCompactionTask> {
        // write amp
        let sum = &_snapshot
            .levels
            .iter()
            .fold(0, |acc, level| acc + level.1.len());
        let bottom_level_sst_num = if let Some((_, ssts)) = _snapshot.levels.last() {
            ssts.len()
        } else {
            0
        };
        let sum_except_last = sum - bottom_level_sst_num;
        let amp_ratio = ((sum_except_last as f64) / (bottom_level_sst_num as f64)) * 100.0;
        if amp_ratio > (self.options.max_size_amplification_percent as f64) {
            return Some(TieredCompactionTask {
                tiers: _snapshot.levels.clone(),
                bottom_tier_included: true,
            });
        }
        // size ratio,找到第一个size/prev_size_sum的level, todo handle 0
        let mut counter = 0;
        for (idx, (_, ssts)) in _snapshot.levels.iter().enumerate() {
            if idx == 0 {
                counter = counter + ssts.len();
                continue;
            } else {
                let ratio = (ssts.len() as f64) / (counter as f64);
                if ratio > ((100 + self.options.size_ratio) as f64) / 100.0 {
                    return Some(TieredCompactionTask {
                        tiers: _snapshot.levels.iter().take(idx + 1).cloned().collect(),
                        bottom_tier_included: idx == _snapshot.levels.len(),
                    });
                } else {
                    continue;
                }
            }
        }
        // reduce tier num (self.options.num_tiers - 1 = 目标levels大小)
        let n = _snapshot.levels.len() - self.options.num_tiers + 2;
        Some(TieredCompactionTask {
            tiers: _snapshot.levels.iter().take(n).cloned().collect(),
            bottom_tier_included: n >= _snapshot.levels.len(),
        })
    }

    pub fn apply_compaction_result(
        &self,
        _snapshot: &LsmStorageState,
        _task: &TieredCompactionTask,
        _output: &[usize],
    ) -> (LsmStorageState, Vec<usize>) {
        // tier compaction,只修改levels数组
        let mut clone = _snapshot.clone();
        let mut new_levels = vec![];
        let mut delete_sst_ids = vec![];
        let mut new_level_added = false;
        let mut task_map = _task.tiers.iter().cloned().collect::<HashMap<_, _>>();

        // 遍历原生levels, 如果是要compact的tier id, remove;如果不是,则retain
        for (id, sst_ids) in clone.levels {
            if let Some(ssts) = task_map.remove(&id) {
                assert_eq!(ssts, sst_ids, "tiers changed during compaction");
                delete_sst_ids.extend(sst_ids);
            } else {
                new_levels.push((id, sst_ids));
            }
            if task_map.is_empty() && !new_level_added {
                // 使用最小sstid作为tier id
                new_levels.push((_output[0], _output.to_vec()));
                new_level_added = true;
            }
        }
        assert!(task_map.is_empty(), "no corresponding tier found");
        clone.levels = new_levels;
        return (clone, delete_sst_ids);
    }
}
