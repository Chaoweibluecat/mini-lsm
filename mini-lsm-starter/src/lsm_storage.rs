#![allow(dead_code)] // REMOVE THIS LINE after fully implementing this functionality

use std::collections::HashMap;
use std::fs::File;
use std::ops::{ Bound, DerefMut };
use std::path::{ Path, PathBuf };
use std::ptr::read;
use std::sync::atomic::AtomicUsize;
use std::sync::Arc;

use anyhow::{ Ok, Result };
use bytes::Bytes;
use parking_lot::{ Mutex, MutexGuard, RwLock };

use crate::block::Block;
use crate::compact::{
    CompactionController,
    CompactionOptions,
    CompactionTask,
    LeveledCompactionController,
    LeveledCompactionOptions,
    SimpleLeveledCompactionController,
    SimpleLeveledCompactionOptions,
    TieredCompactionController,
};
use crate::iterators::concat_iterator::SstConcatIterator;
use crate::iterators::merge_iterator::MergeIterator;
use crate::iterators::two_merge_iterator::TwoMergeIterator;
use crate::iterators::StorageIterator;
use crate::key::{ self, KeySlice };
use crate::lsm_iterator::{ FusedIterator, LsmIterator };
use crate::manifest::{ self, Manifest, ManifestRecord };
use crate::mem_table::{ map_bound, MemTable, MemTableIterator };
use crate::mvcc::LsmMvccInner;
use crate::table::{ FileObject, SsTable, SsTableBuilder, SsTableIterator };

pub type BlockCache = moka::sync::Cache<(usize, usize), Arc<Block>>;

/// Represents the state of the storage engine.
#[derive(Clone)]
pub struct LsmStorageState {
    /// The current memtable.
    pub memtable: Arc<MemTable>,
    /// Immutable memtables, from latest to earliest.
    pub imm_memtables: Vec<Arc<MemTable>>,
    /// L0 SSTs, from latest to earliest.
    pub l0_sstables: Vec<usize>,
    /// SsTables sorted by key range; L1 - L_max for leveled compaction, or tiers for tiered
    /// compaction.
    pub levels: Vec<(usize, Vec<usize>)>,
    /// SST objects.
    pub sstables: HashMap<usize, Arc<SsTable>>,
}

pub enum WriteBatchRecord<T: AsRef<[u8]>> {
    Put(T, T),
    Del(T),
}

impl LsmStorageState {
    fn create(options: &LsmStorageOptions) -> Self {
        let levels = match &options.compaction_options {
            | CompactionOptions::Leveled(LeveledCompactionOptions { max_levels, .. })
            | CompactionOptions::Simple(SimpleLeveledCompactionOptions { max_levels, .. }) =>
                (1..=*max_levels).map(|level| (level, Vec::new())).collect::<Vec<_>>(),
            CompactionOptions::Tiered(_) => Vec::new(),
            CompactionOptions::NoCompaction => vec![(1, Vec::new())],
        };
        Self {
            memtable: Arc::new(MemTable::create(0)),
            imm_memtables: Vec::new(),
            l0_sstables: Vec::new(),
            levels,
            sstables: Default::default(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct LsmStorageOptions {
    // Block size in bytes
    pub block_size: usize,
    // SST size in bytes, also the approximate memtable capacity limit
    pub target_sst_size: usize,
    // Maximum number of memtables in memory, flush to L0 when exceeding this limit
    pub num_memtable_limit: usize,
    pub compaction_options: CompactionOptions,
    pub enable_wal: bool,
    pub serializable: bool,
}

impl LsmStorageOptions {
    pub fn default_for_week1_test() -> Self {
        Self {
            block_size: 4096,
            target_sst_size: 2 << 20,
            compaction_options: CompactionOptions::NoCompaction,
            enable_wal: false,
            num_memtable_limit: 50,
            serializable: false,
        }
    }

    pub fn default_for_week1_day6_test() -> Self {
        Self {
            block_size: 4096,
            target_sst_size: 2 << 20,
            compaction_options: CompactionOptions::NoCompaction,
            enable_wal: false,
            num_memtable_limit: 2,
            serializable: false,
        }
    }

    pub fn default_for_week2_test(compaction_options: CompactionOptions) -> Self {
        Self {
            block_size: 4096,
            target_sst_size: 1 << 20, // 1MB
            compaction_options,
            enable_wal: false,
            num_memtable_limit: 2,
            serializable: false,
        }
    }
}

#[derive(Clone, Debug)]
pub enum CompactionFilter {
    Prefix(Bytes),
}

/// The storage interface of the LSM tree.
pub(crate) struct LsmStorageInner {
    pub(crate) state: Arc<RwLock<Arc<LsmStorageState>>>,
    pub(crate) state_lock: Mutex<()>,
    path: PathBuf,
    pub(crate) block_cache: Arc<BlockCache>,
    next_sst_id: AtomicUsize,
    pub(crate) options: Arc<LsmStorageOptions>,
    pub(crate) compaction_controller: CompactionController,
    pub(crate) manifest: Option<Manifest>,
    pub(crate) mvcc: Option<LsmMvccInner>,
    pub(crate) compaction_filters: Arc<Mutex<Vec<CompactionFilter>>>,
}

/// A thin wrapper for `LsmStorageInner` and the user interface for MiniLSM.
pub struct MiniLsm {
    pub(crate) inner: Arc<LsmStorageInner>,
    /// Notifies the L0 flush thread to stop working. (In week 1 day 6)
    flush_notifier: crossbeam_channel::Sender<()>,
    /// The handle for the flush thread. (In week 1 day 6)
    flush_thread: Mutex<Option<std::thread::JoinHandle<()>>>,
    /// Notifies the compaction thread to stop working. (In week 2)
    compaction_notifier: crossbeam_channel::Sender<()>,
    /// The handle for the compaction thread. (In week 2)
    compaction_thread: Mutex<Option<std::thread::JoinHandle<()>>>,
}

impl Drop for MiniLsm {
    fn drop(&mut self) {
        self.compaction_notifier.send(()).ok();
        self.flush_notifier.send(()).ok();
    }
}

impl MiniLsm {
    pub fn close(&self) -> Result<()> {
        self.flush_notifier.send(())?;

        if !self.inner.options.enable_wal {
            if !self.inner.state.read().memtable.is_empty() {
                let lock = self.inner.state_lock.lock();
                self.inner.force_freeze_memtable(&lock);
                drop(lock);
            }
            // 每次update都会更新state引用,这里需要循环重读
            while !self.inner.state.read().imm_memtables.is_empty() {
                self.inner.force_flush_next_imm_memtable();
            }
        }
        self.inner.sync_dir();
        Ok(())
    }

    /// Start the storage engine by either loading an existing directory or creating a new one if the directory does
    /// not exist.
    pub fn open(path: impl AsRef<Path>, options: LsmStorageOptions) -> Result<Arc<Self>> {
        let inner = Arc::new(LsmStorageInner::open(path, options)?);
        let (tx1, rx) = crossbeam_channel::unbounded();
        let compaction_thread = inner.spawn_compaction_thread(rx)?;
        let (tx2, rx) = crossbeam_channel::unbounded();
        let flush_thread = inner.spawn_flush_thread(rx)?;
        Ok(
            Arc::new(Self {
                inner,
                flush_notifier: tx2,
                flush_thread: Mutex::new(flush_thread),
                compaction_notifier: tx1,
                compaction_thread: Mutex::new(compaction_thread),
            })
        )
    }

    pub fn new_txn(&self) -> Result<()> {
        self.inner.new_txn()
    }

    pub fn write_batch<T: AsRef<[u8]>>(&self, batch: &[WriteBatchRecord<T>]) -> Result<()> {
        self.inner.write_batch(batch)
    }

    pub fn add_compaction_filter(&self, compaction_filter: CompactionFilter) {
        self.inner.add_compaction_filter(compaction_filter)
    }

    pub fn get(&self, key: &[u8]) -> Result<Option<Bytes>> {
        self.inner.get(key)
    }

    pub fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        self.inner.put(key, value)
    }

    pub fn delete(&self, key: &[u8]) -> Result<()> {
        self.inner.delete(key)
    }

    pub fn sync(&self) -> Result<()> {
        self.inner.sync()
    }

    pub fn scan(
        &self,
        lower: Bound<&[u8]>,
        upper: Bound<&[u8]>
    ) -> Result<FusedIterator<LsmIterator>> {
        self.inner.scan(lower, upper)
    }

    /// Only call this in test cases due to race conditions
    pub fn force_flush(&self) -> Result<()> {
        if !self.inner.state.read().memtable.is_empty() {
            self.inner.force_freeze_memtable(&self.inner.state_lock.lock())?;
        }
        if !self.inner.state.read().imm_memtables.is_empty() {
            self.inner.force_flush_next_imm_memtable()?;
        }
        Ok(())
    }

    pub fn force_full_compaction(&self) -> Result<()> {
        self.inner.force_full_compaction()
    }
}

impl LsmStorageInner {
    pub(crate) fn next_sst_id(&self) -> usize {
        self.next_sst_id.fetch_add(1, std::sync::atomic::Ordering::SeqCst)
    }

    /// Start the storage engine by either loading an existing directory or creating a new one if the directory does
    /// not exist.
    pub(crate) fn open(path: impl AsRef<Path>, options: LsmStorageOptions) -> Result<Self> {
        let path = path.as_ref();
        let mut state = LsmStorageState::create(&options);

        let compaction_controller = match &options.compaction_options {
            CompactionOptions::Leveled(options) => {
                CompactionController::Leveled(LeveledCompactionController::new(options.clone()))
            }
            CompactionOptions::Tiered(options) => {
                CompactionController::Tiered(TieredCompactionController::new(options.clone()))
            }
            CompactionOptions::Simple(options) =>
                CompactionController::Simple(
                    SimpleLeveledCompactionController::new(options.clone())
                ),
            CompactionOptions::NoCompaction => CompactionController::NoCompaction,
        };
        let mut next_sst_id = 1;

        let manifest_path = path.join("MANIFEST");
        let block_cache = Arc::new(BlockCache::new(1024));
        let manifest = if manifest_path.exists() {
            // recover state from manifest
            let (manifest, records) = Manifest::recover(manifest_path)?;
            for record in records {
                // replay record,模拟state的变化
                match record {
                    ManifestRecord::Compaction(task, outputs) => {
                        match task {
                            CompactionTask::ForceFullCompaction { l0_sstables, l1_sstables } => {
                                // todo 真的会有不等效于clear的场景吗? 好像可以直接clear
                                let new_l0 = state.l0_sstables
                                    .iter()
                                    .filter(|id| !l0_sstables.contains(id))
                                    .map(|x| *x)
                                    .collect::<Vec<_>>();
                                let new_l1 = state.levels[0].1
                                    .iter()
                                    .filter(|id| !l1_sstables.contains(id))
                                    .map(|x| *x)
                                    .collect::<Vec<_>>();
                                state.l0_sstables = new_l0;
                                state.levels[0].1 = new_l1;
                                state.levels[0].1.extend(outputs);
                            }
                            _ => {
                                let (new_state, _) = compaction_controller.apply_compaction_result(
                                    &state,
                                    &task,
                                    &outputs,
                                    true
                                );
                                state = new_state;
                            }
                        }
                    }
                    ManifestRecord::Flush(sst_id) => {
                        if compaction_controller.flush_to_l0() {
                            state.l0_sstables.insert(0, sst_id);
                        } else {
                            state.levels[0].1.insert(0, sst_id);
                        }
                        next_sst_id = next_sst_id.max(sst_id);
                    }
                    _ => unimplemented!(),
                }
            }

            // replay 结束后reopen所有sst file
            for l0_sst in &state.l0_sstables {
                let sst = SsTable::open(
                    *l0_sst,
                    Some(block_cache.clone()),
                    FileObject::open(&Self::path_of_sst_static(path, *l0_sst))?
                )?;
                state.sstables.insert(*l0_sst, Arc::new(sst));
            }
            for (_, ssts) in &state.levels {
                for sst_id in ssts {
                    let sst = SsTable::open(
                        *sst_id,
                        Some(block_cache.clone()),
                        FileObject::open(&Self::path_of_sst_static(path, *sst_id))?
                    )?;
                    state.sstables.insert(*sst_id, Arc::new(sst));
                }
            }
            Some(manifest)
        } else {
            Some(Manifest::create(manifest_path)?)
        };
        let storage = Self {
            state: Arc::new(RwLock::new(Arc::new(state))),
            state_lock: Mutex::new(()),
            path: path.to_path_buf(),
            block_cache,
            next_sst_id: AtomicUsize::new(next_sst_id),
            compaction_controller,
            manifest,
            options: options.into(),
            mvcc: None,
            compaction_filters: Arc::new(Mutex::new(Vec::new())),
        };

        Ok(storage)
    }

    pub fn sync(&self) -> Result<()> {
        unimplemented!()
    }

    pub fn add_compaction_filter(&self, compaction_filter: CompactionFilter) {
        let mut compaction_filters = self.compaction_filters.lock();
        compaction_filters.push(compaction_filter);
    }

    /// Get a key from the storage. In day 7, this can be further optimized by using a bloom filter.
    pub fn get(&self, _key: &[u8]) -> Result<Option<Bytes>> {
        // memtable本身是lock-free的,但是这里首先还是需要获取state的读锁,
        // 因为这把读锁守护state内部,state内的memtable引用指向谁都是可变的；
        let snapshot = {
            let guard = self.state.read();
            Arc::clone(&guard)
        };
        let mut cur_ref = Some(snapshot.memtable.clone());
        let mut i = 0;
        while let Some(memtable) = cur_ref {
            match memtable.get(_key) {
                None => {
                    cur_ref = snapshot.imm_memtables.get(i).cloned();
                    i = i + 1;
                }
                Some(bytes) => {
                    if bytes.is_empty() {
                        return Ok(None);
                    } else {
                        return Ok(Some(bytes));
                    }
                }
            }
        }

        // 创建l0sstable的mergeIter(可以优化,理论上来说是有多余IO);SsTableIterator::create_and_seek_to_key都至少会读一个块（因为要读block）
        // 创建的SsTableIterator都seek到第一个>= _key的key
        // 1. mergeIter非法,说明key太大了
        // 2. mergeIter.key (代表所有sstable中>=_key的最小key) > _key ; 说明没有这个key
        // 3. key match,兼容删除场景即可
        let mut l0_iter = Vec::with_capacity(snapshot.l0_sstables.len());
        for i in snapshot.l0_sstables.iter() {
            let table = snapshot.sstables.get(i).unwrap().clone();
            let iter = Box::new(
                SsTableIterator::create_and_seek_to_key(table, KeySlice::from_slice(_key))?
            );
            l0_iter.push(iter);
        }

        let iters = (0..snapshot.levels.len())
            .into_iter()
            .map(|level| self.get_sst_concat_iter(snapshot.clone(), level, Bound::Included(_key)))
            .collect::<Result<Vec<_>>>()?;
        let level_iters: Vec<_> = iters
            .into_iter()
            .map(|iter| Box::new(iter))
            .collect();
        let levels_iter = MergeIterator::create(level_iters);

        let merge_iter = TwoMergeIterator::create(MergeIterator::create(l0_iter), levels_iter)?;
        if !merge_iter.is_valid() {
            return Ok(None);
        }
        if merge_iter.key() > KeySlice::from_slice(_key) {
            return Ok(None);
        }
        return if merge_iter.value().is_empty() {
            Ok(None)
        } else {
            Ok(Some(Bytes::copy_from_slice(merge_iter.value())))
        };
    }

    /// Write a batch of data into the storage. Implement in week 2 day 7.
    pub fn write_batch<T: AsRef<[u8]>>(&self, _batch: &[WriteBatchRecord<T>]) -> Result<()> {
        unimplemented!()
    }

    /// Put a key-value pair into the storage by writing into the current memtable.
    pub fn put(&self, _key: &[u8], _value: &[u8]) -> Result<()> {
        let guard = self.state.read();
        let size = {
            guard.memtable.put(_key, _value)?;
            guard.memtable.approximate_size()
        };
        drop(guard);
        if size > self.options.target_sst_size {
            let state_lock = self.state_lock.lock();
            // 进同步块以后state rwlock本身守护的对memtable的引用可能变了, 需要重新获取引用
            let guard = self.state.read();
            if guard.memtable.approximate_size() > self.options.target_sst_size {
                drop(guard);
                self.force_freeze_memtable(&state_lock)?;
            }
        }
        Ok(())
    }

    /// Remove a key from the storage by writing an empty value.
    pub fn delete(&self, _key: &[u8]) -> Result<()> {
        let guard = self.state.read();
        let size = {
            guard.memtable.put(_key, &[])?;
            guard.memtable.approximate_size()
        };
        drop(guard);
        if size > self.options.target_sst_size {
            let state_lock = self.state_lock.lock();
            // 进同步块以后state rwlock本身守护的对memtable的引用可能变了, 需要重新获取引用
            let guard = self.state.read();
            if guard.memtable.approximate_size() > self.options.target_sst_size {
                drop(guard);
                self.force_freeze_memtable(&state_lock)?;
            }
        }
        Ok(())
    }

    pub(crate) fn path_of_sst_static(path: impl AsRef<Path>, id: usize) -> PathBuf {
        path.as_ref().join(format!("{:05}.sst", id))
    }

    pub(crate) fn path_of_sst(&self, id: usize) -> PathBuf {
        Self::path_of_sst_static(&self.path, id)
    }

    pub(crate) fn path_of_wal_static(path: impl AsRef<Path>, id: usize) -> PathBuf {
        path.as_ref().join(format!("{:05}.wal", id))
    }

    pub(crate) fn path_of_wal(&self, id: usize) -> PathBuf {
        Self::path_of_wal_static(&self.path, id)
    }

    pub(super) fn sync_dir(&self) -> Result<()> {
        File::open(&self.path)?.sync_all()?;
        Ok(())
    }

    /// Force freeze the current memtable to an immutable memtable
    pub fn force_freeze_memtable(&self, _state_lock_observer: &MutexGuard<'_, ()>) -> Result<()> {
        // 1.创建新的memtable
        let new_memtable = MemTable::create(self.next_sst_id());
        // 获取state的写锁
        let mut state = self.state.write();
        // 因为拿的是state的读锁,这里需要clone后才能写state的字段,（不能直接insert!,rc不能直接写）
        let mut snap = state.as_ref().clone();
        snap.imm_memtables.insert(0, snap.memtable);
        snap.memtable = Arc::new(new_memtable);
        // 将cow后的state作为最新的读写锁保护的引用
        *state = Arc::new(snap);
        drop(state);
        Ok(())
    }

    /// Force flush the earliest-created immutable memtable to disk
    pub fn force_flush_next_imm_memtable(&self) -> Result<()> {
        // 需要串行state的写;
        // 如果两个线程同时调用force_flush_next_imm_memtable,临界区在第一个read外的话,
        // 两个线程会读到同一个mmt,并导致flush两遍
        let mutex = self.state_lock.lock();
        // 1.读出last memtable
        let last_mmt = {
            let read1 = self.state.read();
            read1.imm_memtables.last().unwrap().clone()
        };

        // write sst
        let mut builder: SsTableBuilder = SsTableBuilder::new(self.options.block_size);
        last_mmt.flush(&mut builder)?;
        let id = last_mmt.id();
        let ss_table = Arc::new(
            builder.build(id, Some(self.block_cache.clone()), self.path_of_sst(id))?
        );

        // 删除memtable, cow更新state
        let mut write_guard = self.state.write();
        let mut snap = write_guard.as_ref().clone();
        snap.imm_memtables.pop();
        if self.compaction_controller.flush_to_l0() {
            snap.l0_sstables.insert(0, ss_table.sst_id());
        } else {
            snap.levels.insert(0, (ss_table.sst_id(), vec![ss_table.sst_id()]));
        }
        snap.sstables.insert(ss_table.sst_id(), ss_table);
        // 将cow后的state作为最新的读写锁保护的引用
        *write_guard = Arc::new(snap);
        drop(write_guard);

        // sync new written file and storage directory
        self.sync_dir()?;

        // write manifest and sync
        if let Some(manifest) = self.manifest.as_ref() {
            manifest.add_record(&mutex, ManifestRecord::Flush(id));
        }
        // todo 这里需要sync吗??
        Ok(())
    }

    pub fn new_txn(&self) -> Result<()> {
        // no-op
        Ok(())
    }

    pub fn out_bound(table: Arc<SsTable>, _lower: Bound<&[u8]>, _upper: Bound<&[u8]>) -> bool {
        // 查询区间最大值小于ss_table最小值
        let first = table.first_key();
        let smaller_than_min = match _upper {
            Bound::Unbounded => false,
            Bound::Included(k) => k < first.raw_ref(),
            Bound::Excluded(k) => k <= first.raw_ref(),
        };

        // 区间最小值大于ss_table最大值
        let last = table.last_key();

        let greater_than_max = match _lower {
            Bound::Unbounded => false,
            Bound::Included(k) => k > last.raw_ref(),
            Bound::Excluded(k) => k >= last.raw_ref(),
        };
        return smaller_than_min || greater_than_max;
    }
    /// Create an iterator over a range of keys.
    pub fn scan(
        &self,
        _lower: Bound<&[u8]>,
        _upper: Bound<&[u8]>
    ) -> Result<FusedIterator<LsmIterator>> {
        let snapshot = {
            let guard = self.state.read();
            Arc::clone(&guard)
        };

        let cur = Box::new(snapshot.memtable.scan(_lower, _upper));
        let mut memtable_iter: Vec<Box<MemTableIterator>> = snapshot.imm_memtables
            .iter()
            .map(|table| Box::new(table.scan(_lower, _upper)))
            .collect();
        memtable_iter.insert(0, cur);

        let mut l0_iter: Vec<Box<SsTableIterator>> = Vec::with_capacity(
            snapshot.l0_sstables.capacity()
        );
        for i in snapshot.l0_sstables.iter() {
            let table = snapshot.sstables.get(i).unwrap().clone();
            if Self::out_bound(table.clone(), _lower, _upper) {
                continue;
            }
            let iter = match _lower {
                Bound::Included(lower) =>
                    Box::new(
                        SsTableIterator::create_and_seek_to_key(table, KeySlice::from_slice(lower))?
                    ),
                Bound::Excluded(lower) => {
                    let mut iter = Box::new(
                        SsTableIterator::create_and_seek_to_key(table, KeySlice::from_slice(lower))?
                    );
                    while iter.is_valid() && iter.key() == KeySlice::from_slice(lower) {
                        iter.next()?;
                    }
                    iter
                }
                _ => Box::new(SsTableIterator::create_and_seek_to_first(table)?),
            };
            l0_iter.push(iter);
        }
        let two = TwoMergeIterator::create(
            MergeIterator::create(memtable_iter),
            MergeIterator::create(l0_iter)
        )?;

        // 处理levels
        let iters = (0..snapshot.levels.len())
            .into_iter()
            .map(|level| self.get_sst_concat_iter(snapshot.clone(), level, _lower))
            .collect::<Result<Vec<_>>>()?;
        let level_iters: Vec<_> = iters
            .into_iter()
            .map(|iter| Box::new(iter))
            .collect();
        let levels_iter = MergeIterator::create(level_iters);
        let inner_iter = TwoMergeIterator::create(two, levels_iter)?;
        // 这里是拷贝，否则数组引用逃逸出方法体
        let bytes = _upper.map(|slice| Bytes::copy_from_slice(slice));
        let iter = FusedIterator::new(LsmIterator::new(inner_iter, bytes)?);
        Ok(iter)
    }

    pub fn get_sst_concat_iter(
        &self,
        state: Arc<LsmStorageState>,
        level: usize,
        _lower: Bound<&[u8]>
    ) -> Result<SstConcatIterator> {
        let ssts = state.levels[level].1
            .iter()
            .map(|id| state.sstables.get(id).unwrap().clone())
            .collect();
        let iter = match _lower {
            Bound::Included(lower) => {
                SstConcatIterator::create_and_seek_to_key(ssts, KeySlice::from_slice(lower))?
            }
            Bound::Unbounded => SstConcatIterator::create_and_seek_to_first(ssts)?,
            Bound::Excluded(lower) => {
                let mut iter = SstConcatIterator::create_and_seek_to_key(
                    ssts,
                    KeySlice::from_slice(lower)
                )?;
                while iter.is_valid() && iter.key().raw_ref() == lower {
                    iter.next()?;
                }
                iter
            }
        };
        Ok(iter)
    }
}
