use crate::buffer_pool::buffer_pool_stats::BPStats;
use crate::container_file_catalog::ContainerFileCatalog;
use common::ids::{ContainerId, ContainerPageId, PageId};
use common::rwlatch::RwLatch;

use super::{
    buffer_frame::{BufferFrame, FrameReadGuard, FrameWriteGuard},
    eviction_policy::EvictionPolicy,
    mem_pool_trait::{MemPool, MemPoolStatus, PageFrameId},
    mem_stats::MemoryStats,
};

use std::{
    cell::UnsafeCell,
    collections::{BTreeMap, HashMap},
    sync::{atomic::Ordering, Arc},
};

use concurrent_queue::ConcurrentQueue;

pub struct PageToFrame {
    map: HashMap<ContainerId, HashMap<PageId, usize>>, // (c_key, page_id) -> frame_index
}

impl PageToFrame {
    pub fn new() -> Self {
        PageToFrame {
            map: HashMap::new(),
        }
    }

    pub fn contains_key(&self, p_key: &ContainerPageId) -> bool {
        self.map
            .get(&p_key.c_id)
            .is_some_and(|m| m.contains_key(&p_key.page_id))
    }

    pub fn get(&self, p_key: &ContainerPageId) -> Option<&usize> {
        // Get by c_key and then page_id
        self.map
            .get(&p_key.c_id)
            .and_then(|m| m.get(&p_key.page_id))
    }

    pub fn get_page_frame_ids(&self, c_key: ContainerId) -> Vec<PageFrameId> {
        self.map.get(&c_key).map_or(Vec::new(), |m| {
            m.iter()
                .map(|(page_id, frame_index)| {
                    PageFrameId::new_with_frame_id(c_key, *page_id, *frame_index as u32)
                })
                .collect()
        })
    }

    pub fn insert(&mut self, p_key: ContainerPageId, frame_id: usize) {
        if self
            .map
            .entry(p_key.c_id)
            .or_default()
            .insert(p_key.page_id, frame_id)
            .is_some()
        {
            panic!(
                "Duplicate page id: {} in container: {}",
                p_key.page_id, p_key.c_id
            );
        }
    }

    pub fn remove(&mut self, p_key: &ContainerPageId) -> Option<usize> {
        self.map
            .get_mut(&p_key.c_id)
            .and_then(|m| m.remove(&p_key.page_id))
    }

    pub fn clear(&mut self) {
        self.map.clear();
    }

    #[allow(dead_code)]
    pub fn iter(&self) -> impl Iterator<Item = (&ContainerId, &PageId, &usize)> {
        self.map.iter().flat_map(|(c_key, page_map)| {
            page_map
                .iter()
                .map(move |(page_id, frame_index)| (c_key, page_id, frame_index))
        })
    }

    pub fn iter_container(&self, c_key: ContainerId) -> impl Iterator<Item = (&PageId, &usize)> {
        self.map
            .get(&c_key)
            .into_iter()
            .flat_map(|page_map| page_map.iter())
    }
}

/// Buffer pool that manages the buffer frames.
pub struct BufferPool {
    /// Reference to the container file catalog.
    cfc: Arc<ContainerFileCatalog>,
    /// The latch used for concurrent access to the buffer pool.
    latch: RwLatch,
    /// A hint for quickly finding a clean frame or a frame to evict. Whenever a clean frame is found, it is pushed to this queue so that it can be quickly found.
    eviction_hints: ConcurrentQueue<usize>,
    /// The Vec<frames> is fixed size. If not fixed size, then Pin must be used to ensure that the frame does not move when the vector is resized.
    frames: UnsafeCell<Vec<BufferFrame>>,
    /// The PageToFrame mapping is used to quickly find the frame index for a given page id.
    page_to_frame: UnsafeCell<PageToFrame>, // (c_key, page_id) -> frame_index
    /// Statistics for the buffer pool.
    stats: BPStats,
}

impl Drop for BufferPool {
    fn drop(&mut self) {
        if self.cfc.remove_dir_on_drop() {
            // Do nothing. Directory will be removed when the container manager is dropped.
        } else {
            // Persist all the pages to disk
            self.flush_all_and_reset().unwrap();
        }
    }
}

impl BufferPool {
    /// Create a new buffer pool with the given number of frames.
    pub fn new(
        num_frames: usize,
        container_manager: Arc<ContainerFileCatalog>,
    ) -> Result<Self, MemPoolStatus> {
        let eviction_hints = ConcurrentQueue::unbounded();
        for i in 0..num_frames {
            eviction_hints.push(i).unwrap();
        }

        let frames = (0..num_frames)
            .map(|i| BufferFrame::new(i as u32))
            .collect();

        Ok(BufferPool {
            cfc: container_manager,
            latch: RwLatch::default(),
            page_to_frame: UnsafeCell::new(PageToFrame::new()),
            eviction_hints,
            frames: UnsafeCell::new(frames),
            stats: BPStats::new(),
        })
    }

    pub fn eviction_stats(&self) -> String {
        "Eviction stats not supported".to_string()
    }

    pub fn file_stats(&self) -> String {
        "File stat is disabled".to_string()
    }

    fn shared(&self) {
        self.latch.shared();
    }

    fn exclusive(&self) {
        self.latch.exclusive();
    }

    fn release_shared(&self) {
        self.latch.release_shared();
    }

    fn release_exclusive(&self) {
        self.latch.release_exclusive();
    }

    /// Choose a frame to be evicted.
    fn choose_eviction_candidate(&self) -> Option<FrameWriteGuard> {
        // 33550 ONLY
        //TODO last milestone NOT hs
        None
    }

    /// Choose a victim frame to be used for allocating a new page.
    /// If all the frames are latched, then return None.
    fn choose_victim(&self) -> Option<FrameWriteGuard> {
        let frames = unsafe { &*self.frames.get() };

        // First, try the eviction hints
        while let Ok(victim) = self.eviction_hints.pop() {
            let frame = frames[victim].try_write(false);
            if let Some(guard) = frame {
                return Some(guard);
            } else {
                // The frame is latched. Try the next frame.
            }
        }

        self.choose_eviction_candidate()
    }

    /// Choose multiple victim frames to be used for allocating new pages.
    /// The returned vector may contain fewer frames thant he requested number of victims.
    /// It can also return an empty vector.
    fn choose_victims(&self, num_victims: usize) -> Vec<FrameWriteGuard> {
        let frames = unsafe { &*self.frames.get() };
        let num_victims = frames.len().min(num_victims);
        let mut victims = Vec::with_capacity(num_victims);

        // First, try the eviction hints
        while let Ok(victim) = self.eviction_hints.pop() {
            let frame = frames[victim].try_write(false);
            if let Some(guard) = frame {
                victims.push(guard);
                if victims.len() == num_victims {
                    return victims;
                }
            } else {
                // The frame is latched. Try the next frame.
            }
        }

        while victims.len() < num_victims {
            if let Some(victim) = self.choose_eviction_candidate() {
                victims.push(victim);
            } else {
                break;
            }
        }

        victims
    }

    // The exclusive latch is NOT NEEDED when calling this function
    // This function will write the victim page to disk if it is dirty, and set the dirty bit to false.
    fn write_victim_to_disk_if_dirty_w(
        &self,
        victim: &FrameWriteGuard,
    ) -> Result<(), MemPoolStatus> {
        if let Some(key) = victim.page_id() {
            if victim
                .dirty()
                .compare_exchange(true, false, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                let container = self.cfc.get_container(key.c_id);
                container.write_page(key.page_id, victim)?;
            }
        }

        Ok(())
    }

    // The exclusive latch is NOT NEEDED when calling this function
    // This function will write the victim page to disk if it is dirty, and set the dirty bit to false.
    fn write_victim_to_disk_if_dirty_r(
        &self,
        victim: &FrameReadGuard,
    ) -> Result<(), MemPoolStatus> {
        if let Some(key) = victim.page_id() {
            // Compare and swap is_dirty because we don't want to write the page if it is already written by another thread.
            if victim
                .dirty()
                .compare_exchange(true, false, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                let container = self.cfc.get_container(key.c_id);
                container.write_page(key.page_id, victim)?;
            }
        }

        Ok(())
    }
}

impl MemPool for BufferPool {
    fn create_container(&self, c_key: ContainerId, is_temp: bool) -> Result<(), MemPoolStatus> {
        self.cfc.register_container(c_key, is_temp);
        Ok(())
    }

    fn drop_container(&self, c_key: ContainerId) -> Result<(), MemPoolStatus> {
        self.cfc.get_container(c_key).set_temp(true);
        self.shared();
        let page_to_frame = unsafe { &mut *self.page_to_frame.get() };
        for (_, frame_index) in page_to_frame.iter_container(c_key) {
            self.eviction_hints.push(*frame_index).unwrap();
        }
        self.release_shared();
        Ok(())
    }

    /// Create a new page for write in memory.
    /// NOTE: This function does not write the page to disk.
    /// See more at `handle_page_fault(key, new_page=true)`
    /// The newly allocated page is not formatted except for the page id.
    /// The caller is responsible for initializing the page.
    fn create_new_page_for_write(
        &self,
        c_key: ContainerId,
    ) -> Result<FrameWriteGuard, MemPoolStatus> {
        self.stats.inc_new_page();

        // 1. Choose victim
        if let Some(mut victim) = self.choose_victim() {
            // 2. Handle eviction if the victim is dirty
            let res = self.write_victim_to_disk_if_dirty_w(&victim);

            match res {
                Ok(()) => {
                    // 3. Modify the page_to_frame mapping. Critical section.
                    // Need to remove the old mapping and insert the new mapping.
                    let page_key = {
                        self.exclusive();
                        let page_to_frame = unsafe { &mut *self.page_to_frame.get() };
                        // Remove the old mapping
                        if let Some(old_key) = victim.page_id() {
                            page_to_frame.remove(old_key).unwrap(); // Unwrap is safe because victim's write latch is held. No other thread can remove the old key from page_to_frame before this thread.
                        }
                        // Insert the new mapping
                        let container = self.cfc.get_container(c_key);
                        let page_id = container.inc_page_count(1) as PageId;
                        let index = victim.frame_id();
                        let key = ContainerPageId::new(c_key, page_id);
                        page_to_frame.insert(key, index as usize);
                        self.release_exclusive();
                        key
                    };

                    // 4. Initialize the page
                    victim.set_page_id(page_key.page_id); // Initialize the page with the page id
                    victim.page_id_mut().replace(page_key); // Set the frame key to the new page key
                    victim.dirty().store(true, Ordering::Release);

                    Ok(victim)
                }
                Err(e) => Err(e),
            }
        } else {
            // Victim Selection failed
            Err(MemPoolStatus::CannotEvictPage)
        }
    }

    fn create_new_pages_for_write(
        &self,
        c_key: ContainerId,
        num_pages: usize,
    ) -> Result<Vec<FrameWriteGuard>, MemPoolStatus> {
        assert!(num_pages > 0);
        self.stats.inc_new_pages(num_pages);

        // 1. Choose victims
        let mut victims = self.choose_victims(num_pages);
        if !victims.is_empty() {
            // 2. Handle eviction if the page is dirty
            for victim in victims.iter_mut() {
                self.write_victim_to_disk_if_dirty_w(victim)?;
            }

            let start_page_id = {
                // 3. Modify the page_to_frame mapping. Critical section.
                // Need to remove the old mapping and insert the new mapping.
                self.exclusive();
                let page_to_frame = unsafe { &mut *self.page_to_frame.get() };

                // Remove the old mapping
                for victim in victims.iter() {
                    if let Some(old_key) = victim.page_id() {
                        page_to_frame.remove(old_key).unwrap(); // Unwrap is safe because victim's write latch is held. No other thread can remove the old key from page_to_frame before this thread.
                    }
                }

                // Insert the new mapping
                let container = self.cfc.get_container(c_key);
                let start_page_id = container.inc_page_count(num_pages) as PageId;
                for (i, victim) in victims.iter_mut().enumerate().take(num_pages) {
                    let page_id = start_page_id + i as u32;
                    let key = ContainerPageId::new(c_key, page_id);
                    page_to_frame.insert(key, victim.frame_id() as usize);
                }

                self.release_exclusive();
                start_page_id
            };

            // Victim modification will be done outside the critical section
            // as the frame is already write-latched.
            for (i, victim) in victims.iter_mut().enumerate() {
                let page_id = start_page_id + i as u32;
                let key = ContainerPageId::new(c_key, page_id);
                victim.set_page_id(page_id);
                victim.page_id_mut().replace(key);
                victim.dirty().store(true, Ordering::Release);
            }

            Ok(victims)
        } else {
            // Victims not found
            Err(MemPoolStatus::CannotEvictPage)
        }
    }

    fn is_in_mem(&self, key: PageFrameId) -> bool {
        {
            // Fast path access to the frame using frame_id
            let frame_id = key.frame_id();
            let frames = unsafe { &*self.frames.get() };
            if (frame_id as usize) < frames.len() {
                if let Some(g) = frames[frame_id as usize].try_read() {
                    if g.page_id().map(|k| k == key.p_key()).unwrap_or(false) {
                        return true;
                    }
                }
            }
        }

        // Critical section.
        {
            self.shared();
            let page_to_frame = unsafe { &*self.page_to_frame.get() };
            let res = page_to_frame.contains_key(&key.p_key());
            self.release_shared();
            res
        }
    }

    fn get_max_page_id(&self, c_id: ContainerId) -> Option<PageId> {
        self.cfc.get_container_page_count(c_id)
    }

    fn get_page_ids_in_mem(&self, c_key: ContainerId) -> Vec<PageFrameId> {
        self.shared();
        let page_to_frame = unsafe { &*self.page_to_frame.get() };
        let keys = page_to_frame.get_page_frame_ids(c_key);
        self.release_shared();
        keys
    }

    fn get_page_for_write(&self, key: PageFrameId) -> Result<FrameWriteGuard, MemPoolStatus> {
        self.stats.inc_write_count();

        // #[cfg(not(feature = "no_bp_hint"))]
        // {
        //     // Fast path access to the frame using frame_id
        //     let frame_id = key.frame_id();
        //     let frames = unsafe { &*self.frames.get() };
        //     if (frame_id as usize) < frames.len() {
        //         match frames[frame_id as usize].try_write(false) {
        //             Some(g) if g.page_key().map(|k| k == key.p_key()).unwrap_or(false) => {
        //                 g.evict_info().update();
        //                 g.dirty().store(true, Ordering::Release);
        //                 return Ok(g);
        //             }
        //             _ => {}
        //         }
        //     }
        //     // Failed due to one of the following reasons:
        //     // 1. The page key does not match.
        //     // 2. The page key is not set (empty frame).
        //     // 3. The frame is latched.
        //     // 4. The frame id is out of bounds.
        // }

        // Critical section.
        // 1. Check the page-to-frame mapping and get a frame index.
        // 2. If the page is found, then try to acquire a write-latch, after which, the critical section ends.
        // 3. If the page is not found, then a victim must be chosen to evict.
        {
            self.shared();
            let page_to_frame = unsafe { &mut *self.page_to_frame.get() };
            let frames = unsafe { &mut *self.frames.get() };

            if let Some(&index) = page_to_frame.get(&key.p_key()) {
                let guard = frames[index].try_write(true);
                self.release_shared(); // Critical section ends here
                return guard
                    .inspect(|g| {
                        g.evict_info().update();
                    })
                    .ok_or(MemPoolStatus::FrameWriteLatchGrantFailed);
            }
            self.release_shared();
        }

        // Critical section.
        // 1. Check the page-to-frame mapping and get a frame index.
        // 2. If the page is found, then try to acquire a write-latch, after which, the critical section ends.
        // 3. If the page is not found, then choose a victim and remove this mapping and insert the new mapping, after which, the critical section ends.
        // 3.1. An optimization is to find a victim and handle IO outside the critical section.

        // Before entering the critical section, we will find a frame that we can write to.
        let mut victim = self.choose_victim().ok_or(MemPoolStatus::CannotEvictPage)?;
        self.write_victim_to_disk_if_dirty_w(&victim).unwrap();
        // Now we have a clean victim that can be used for writing.
        assert!(!victim.dirty().load(Ordering::Acquire));

        // Start the critical section.
        {
            self.exclusive();

            let page_to_frame = unsafe { &mut *self.page_to_frame.get() };
            let frames = unsafe { &mut *self.frames.get() };
            match page_to_frame.get(&key.p_key()) {
                Some(&index) => {
                    // Unlikely path as it is already checked in the critical section above with the shared latch.
                    let guard = frames[index].try_write(true);
                    self.release_exclusive();

                    self.eviction_hints.push(index).unwrap();
                    drop(victim); // Release the write latch on the unused victim

                    guard
                        .inspect(|g| {
                            g.evict_info().update();
                        })
                        .ok_or(MemPoolStatus::FrameWriteLatchGrantFailed)
                }
                None => {
                    // Likely path as the page has not been found in the page_to_frame mapping.
                    // Remove the victim from the page_to_frame mapping
                    if let Some(old_key) = victim.page_id() {
                        page_to_frame.remove(old_key).unwrap();
                        // Unwrap is safe because victim's write latch is held. No other thread can remove the old key from page_to_frame before this thread.
                    }
                    // Insert the new mapping
                    page_to_frame.insert(key.p_key(), victim.frame_id() as usize);

                    self.release_exclusive();

                    // Read the wanted page from disk.
                    let container = self.cfc.get_container(key.p_key().c_id);
                    container
                        .read_page(key.p_key().page_id, &mut victim)
                        .map(|()| {
                            victim.page_id_mut().replace(key.p_key());
                            victim.evict_info().reset();
                            victim.evict_info().update();
                        })?;
                    victim.dirty().store(true, Ordering::Release); // Prepare the page for writing.
                    Ok(victim)
                }
            }
        }
    }

    fn get_page_for_read(&self, key: PageFrameId) -> Result<FrameReadGuard, MemPoolStatus> {
        self.stats.inc_read_count();

        // #[cfg(not(feature = "no_bp_hint"))]
        // {
        //     // Fast path access to the frame using frame_id
        //     let frame_id = key.frame_id();
        //     let frames = unsafe { &*self.frames.get() };
        //     if (frame_id as usize) < frames.len() {
        //         let guard = frames[frame_id as usize].try_read();
        //         match guard {
        //             Some(g) if g.page_key().map(|k| k == key.p_key()).unwrap_or(false) => {
        //                 // Update the eviction info
        //                 g.evict_info().update();
        //                 return Ok(g);
        //             }
        //             _ => {}
        //         }
        //     }
        //     // Failed due to one of the following reasons:
        //     // 1. The page key does not match.
        //     // 2. The page key is not set (empty frame).
        //     // 3. The frame is latched.
        //     // 4. The frame id is out of bounds.
        // };

        // Critical section.
        // 1. Check the page-to-frame mapping and get a frame index.
        // 2. If the page is found, then try to acquire a read-latch, after which, the critical section ends.
        // 3. If the page is not found, then a victim must be chosen to evict.
        {
            self.shared();
            let page_to_frame = unsafe { &mut *self.page_to_frame.get() };
            let frames = unsafe { &mut *self.frames.get() };

            if let Some(&index) = page_to_frame.get(&key.p_key()) {
                let guard = frames[index].try_read();
                self.release_shared();
                return guard
                    .inspect(|g| {
                        g.evict_info().update();
                    })
                    .ok_or(MemPoolStatus::FrameReadLatchGrantFailed);
            }
            self.release_shared();
        }

        // Critical section.
        // 1. Check the page-to-frame mapping and get a frame index.
        // 2. If the page is found, then try to acquire a read-latch, after which, the critical section ends.
        // 3. If the page is not found, then choose a victim and remove this mapping and insert the new mapping, after which, the critical section ends.
        // 3.1. An optimization is to find a victim and handle IO outside the critical section.

        // Before entering the critical section, we will find a frame that we can read from.
        let mut victim = self.choose_victim().ok_or(MemPoolStatus::CannotEvictPage)?;
        if victim.dirty().load(Ordering::Acquire) {
            self.stats.inc_read_request_waiting_for_write_count();
        }
        self.write_victim_to_disk_if_dirty_w(&victim).unwrap();
        // Now we have a clean victim that can be used for reading.
        assert!(!victim.dirty().load(Ordering::Acquire));

        // Start the critical section.
        {
            self.exclusive();

            let page_to_frame = unsafe { &mut *self.page_to_frame.get() };
            let frames = unsafe { &mut *self.frames.get() };
            match page_to_frame.get(&key.p_key()) {
                Some(&index) => {
                    // Unlikely path as it is already checked in the critical section above with the shared latch.
                    let guard = frames[index].try_read();
                    self.release_exclusive();

                    self.eviction_hints.push(index).unwrap();
                    drop(victim); // Release the write latch on the unused victim

                    guard
                        .inspect(|g| {
                            g.evict_info().update();
                        })
                        .ok_or(MemPoolStatus::FrameReadLatchGrantFailed)
                }
                None => {
                    // Likely path as the page has not been found in the page_to_frame mapping.
                    // Remove the victim from the page_to_frame mapping
                    if let Some(old_key) = victim.page_id() {
                        page_to_frame.remove(old_key).unwrap(); // Unwrap is safe because victim's write latch is held. No other thread can remove the old key from page_to_frame before this thread.
                    }
                    // Insert the new mapping
                    page_to_frame.insert(key.p_key(), victim.frame_id() as usize);

                    self.release_exclusive();

                    let container = self.cfc.get_container(key.p_key().c_id);
                    container
                        .read_page(key.p_key().page_id, &mut victim)
                        .map(|()| {
                            victim.page_id_mut().replace(key.p_key());
                            victim.evict_info().reset();
                            victim.evict_info().update();
                        })?;
                    Ok(victim.downgrade())
                }
            }
        }
    }

    fn prefetch_page(&self, _key: PageFrameId) -> Result<(), MemPoolStatus> {
        Ok(())
    }

    fn flush_all(&self) -> Result<(), MemPoolStatus> {
        self.shared();

        let frames = unsafe { &*self.frames.get() };
        for frame in frames.iter() {
            let frame = loop {
                if let Some(guard) = frame.try_read() {
                    break guard;
                }
                // spin
                std::hint::spin_loop();
            };
            self.write_victim_to_disk_if_dirty_r(&frame)
                .inspect_err(|_| {
                    self.release_shared();
                })?;
        }

        // Call fsync on all the files
        self.cfc.flush_all().inspect_err(|_| {
            self.release_shared();
        })?;

        self.release_shared();
        Ok(())
    }

    fn fast_evict(&self, _frame_id: u32) -> Result<(), MemPoolStatus> {
        // do nothing for now.
        Ok(())
    }

    // Just return the runtime stats
    fn stats(&self) -> MemoryStats {
        let new_page = self.stats.new_page();
        let read_count = self.stats.read_count();
        let read_count_waiting_for_write = self.stats.read_request_waiting_for_write_count();
        let write_count = self.stats.write_count();
        let mut num_frames_per_container = BTreeMap::new();
        for frame in unsafe { &*self.frames.get() }.iter() {
            let frame = frame.read();
            if let Some(key) = frame.page_id() {
                *num_frames_per_container.entry(key.c_id).or_insert(0) += 1;
            }
        }
        let mut disk_io_per_container = BTreeMap::new();
        for (c_key, (count, file_stats)) in &self.cfc.get_stats() {
            disk_io_per_container.insert(
                *c_key,
                (
                    *count as i64,
                    file_stats.read_count() as i64,
                    file_stats.write_count() as i64,
                ),
            );
        }
        let (total_created, total_disk_read, total_disk_write) = disk_io_per_container
            .iter()
            .fold((0, 0, 0), |acc, (_, (created, read, write))| {
                (acc.0 + created, acc.1 + read, acc.2 + write)
            });
        MemoryStats {
            bp_num_frames_in_mem: unsafe { &*self.frames.get() }.len(),
            bp_new_page: new_page,
            bp_read_frame: read_count,
            bp_read_frame_wait: read_count_waiting_for_write,
            bp_write_frame: write_count,
            bp_num_frames_per_container: num_frames_per_container,
            disk_created: total_created as usize,
            disk_read: total_disk_read as usize,
            disk_write: total_disk_write as usize,
            disk_io_per_container,
        }
    }

    fn reset(&self) -> Result<(), MemPoolStatus> {
        self.exclusive();

        let frames = unsafe { &*self.frames.get() };
        let page_to_frame = unsafe { &mut *self.page_to_frame.get() };

        for frame in frames.iter() {
            let mut frame = loop {
                if let Some(guard) = frame.try_write(false) {
                    break guard;
                }
                // spin
                std::hint::spin_loop();
            };
            frame.clear();
        }

        self.cfc.flush_all().inspect_err(|_| {
            self.release_exclusive();
        })?;

        page_to_frame.clear();

        while self.eviction_hints.pop().is_ok() {}
        for i in 0..frames.len() {
            self.eviction_hints.push(i).unwrap();
        }

        self.release_exclusive();
        Ok(())
    }

    // Reset the runtime stats
    fn reset_stats(&self) {
        self.stats.clear();
    }

    /// Reset the buffer pool to its initial state.
    /// This will write all the dirty pages to disk and flush the files.
    /// After this operation, the buffer pool will have all the frames cleared.
    fn flush_all_and_reset(&self) -> Result<(), MemPoolStatus> {
        self.exclusive();

        let frames = unsafe { &*self.frames.get() };
        let page_to_frame = unsafe { &mut *self.page_to_frame.get() };

        for frame in frames.iter() {
            let mut frame = loop {
                if let Some(guard) = frame.try_write(false) {
                    break guard;
                }
                // spin
                std::hint::spin_loop();
            };
            self.write_victim_to_disk_if_dirty_w(&frame)
                .inspect_err(|_| {
                    self.release_exclusive();
                })?;
            frame.clear();
        }

        self.cfc.flush_all().inspect_err(|_| {
            self.release_exclusive();
        })?;

        page_to_frame.clear();

        while self.eviction_hints.pop().is_ok() {}
        for i in 0..frames.len() {
            self.eviction_hints.push(i).unwrap();
        }

        self.release_exclusive();
        Ok(())
    }

    fn clear_dirty_flags(&self) -> Result<(), MemPoolStatus> {
        self.exclusive();

        let frames = unsafe { &*self.frames.get() };

        for frame in frames.iter() {
            let frame = loop {
                if let Some(guard) = frame.try_write(false) {
                    break guard;
                }
                // spin
                std::hint::spin_loop();
            };
            frame.dirty().store(false, Ordering::Release);
        }

        self.cfc.flush_all().inspect_err(|_| {
            self.release_exclusive();
        })?;

        // container_to_file.clear();
        self.stats.clear();

        self.release_exclusive();
        Ok(())
    }
}

#[cfg(test)]
impl BufferPool {
    pub fn run_checks(&self) {
        self.check_all_frames_unlatched();
        self.check_page_to_frame();
        self.check_frame_id_and_page_id_match();
    }

    pub fn check_all_frames_unlatched(&self) {
        let frames = unsafe { &*self.frames.get() };
        for frame in frames.iter() {
            frame.try_write(false).unwrap();
        }
    }

    // Invariant: page_to_frame contains all the pages in the buffer pool
    pub fn check_page_to_frame(&self) {
        let page_to_frame = unsafe { &*self.page_to_frame.get() };
        let mut frame_to_page = HashMap::new();
        for (c, k, &v) in page_to_frame.iter() {
            let p_key = ContainerPageId::new(*c, *k);
            frame_to_page.insert(v, p_key);
        }
        let frames = unsafe { &*self.frames.get() };
        for (i, frame) in frames.iter().enumerate() {
            let frame = frame.read();
            if frame_to_page.contains_key(&i) {
                assert_eq!(frame.page_id().unwrap(), frame_to_page[&i]);
            } else {
                assert_eq!(frame.page_id(), &None);
            }
        }
        // println!("page_to_frame: {:?}", page_to_frame);
    }

    pub fn check_frame_id_and_page_id_match(&self) {
        let frames = unsafe { &*self.frames.get() };
        for frame in frames.iter() {
            let frame = frame.read();
            if let Some(key) = frame.page_id() {
                let page_id = frame.get_page_id();
                assert_eq!(key.page_id, page_id);
            }
        }
    }

    pub fn count_empty_frames(&self) -> usize {
        let frames = unsafe { &*self.frames.get() };
        let mut count = 0;
        for frame in frames.iter() {
            if frame.read().page_id().is_none() {
                count += 1;
            }
        }
        count
    }

    pub fn disk_size(&self) -> usize {
        // For each container, get the size of the file
        let mut size = 0;
        for (_, c) in self.cfc.iter() {
            size += c.num_pages_in_disk();
        }
        size as usize
    }
}

unsafe impl Sync for BufferPool {}

pub fn gen_random_pathname(prefix: Option<&str>) -> String {
    let ts_in_ns = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let salt = rand::random::<u64>(); // Add random salt to make it unique
    let dir_name = format!("{}_{}_{}", prefix.unwrap_or("random_path"), ts_in_ns, salt);
    dir_name
}

#[allow(dead_code)]
pub fn get_test_bp(num_frames: usize) -> Arc<BufferPool> {
    let base_dir = gen_random_pathname(Some("test_bp_direct"));
    let cfc = Arc::new(ContainerFileCatalog::new(base_dir, true).unwrap());
    Arc::new(BufferPool::new(num_frames, cfc).unwrap())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread::{self};
    use tempfile::TempDir;

    #[test]
    fn test_bp_and_frame_latch() {
        let num_frames = 10;
        let bp = get_test_bp(num_frames);
        let c_id = 0;
        let frame = bp.create_new_page_for_write(c_id).unwrap();
        let key = frame.page_frame_id().unwrap();
        drop(frame);

        let num_threads = 3;
        let num_iterations = 80; // Note: u8 max value is 255
        thread::scope(|s| {
            for _ in 0..num_threads {
                s.spawn(|| {
                    for _ in 0..num_iterations {
                        loop {
                            if let Ok(mut guard) = bp.get_page_for_write(key) {
                                guard[0] += 1;
                                break;
                            } else {
                                // spin
                                std::hint::spin_loop();
                            }
                        }
                    }
                });
            }
        });
        bp.run_checks();
        {
            assert!(bp.is_in_mem(key));
            let guard = bp.get_page_for_read(key).unwrap();
            assert_eq!(guard[0], num_threads * num_iterations);
        }
        bp.run_checks();
    }

    #[test]
    fn test_bp_write_back_simple() {
        let num_frames = 1;
        let bp = get_test_bp(num_frames);
        let c_id = 0;

        let key1 = {
            let mut guard = bp.create_new_page_for_write(c_id).unwrap();
            guard[0] = 1;
            guard.page_frame_id().unwrap()
        };
        let key2 = {
            let mut guard = bp.create_new_page_for_write(c_id).unwrap();
            guard[0] = 2;
            guard.page_frame_id().unwrap()
        };
        bp.run_checks();
        // check contents of evicted page
        {
            assert!(!bp.is_in_mem(key1));
            let guard = bp.get_page_for_read(key1).unwrap();
            assert_eq!(guard[0], 1);
        }
        // check contents of the second page
        {
            assert!(!bp.is_in_mem(key2));
            let guard = bp.get_page_for_read(key2).unwrap();
            assert_eq!(guard[0], 2);
        }
        bp.run_checks();
    }

    #[test]
    fn test_bp_write_back_many() {
        let mut keys = Vec::new();
        let num_frames = 1;
        let bp = get_test_bp(num_frames);
        let c_id = 0;

        for i in 0..100 {
            let mut guard = bp.create_new_page_for_write(c_id).unwrap();
            guard[0] = i;
            keys.push(guard.page_frame_id().unwrap());
        }
        bp.run_checks();
        for (i, key) in keys.iter().enumerate() {
            let guard = bp.get_page_for_read(*key).unwrap();
            assert_eq!(guard[0], i as u8);
        }
        bp.run_checks();
    }

    #[test]
    fn test_bp_create_new_page() {
        let num_frames = 2;
        let bp = get_test_bp(num_frames);
        let c_id = 0;

        let num_traversal = 100;

        let mut count = 0;
        let mut keys = Vec::new();

        for _ in 0..num_traversal {
            let mut guard1 = bp.create_new_page_for_write(c_id).unwrap();
            guard1[0] = count;
            count += 1;
            keys.push(guard1.page_frame_id().unwrap());

            let mut guard2 = bp.create_new_page_for_write(c_id).unwrap();
            guard2[0] = count;
            count += 1;
            keys.push(guard2.page_frame_id().unwrap());
        }

        bp.run_checks();

        // Traverse by 2 pages at a time
        for i in 0..num_traversal {
            let guard1 = bp.get_page_for_read(keys[i * 2]).unwrap();
            assert_eq!(guard1[0], i as u8 * 2);
            let guard2 = bp.get_page_for_read(keys[i * 2 + 1]).unwrap();
            assert_eq!(guard2[0], i as u8 * 2 + 1);
        }

        bp.run_checks();
    }

    #[test]
    fn test_bp_all_frames_latched() {
        let num_frames = 1;
        let bp = get_test_bp(num_frames);
        let c_id = 0;

        let mut guard1 = bp.create_new_page_for_write(c_id).unwrap();
        guard1[0] = 1;

        // Try to get a new page for write. This should fail because all the frames are latched.
        let res = bp.create_new_page_for_write(c_id);
        assert_eq!(res.unwrap_err(), MemPoolStatus::CannotEvictPage);

        drop(guard1);

        // Now, we should be able to get a new page for write.
        let guard2 = bp.create_new_page_for_write(c_id).unwrap();
        drop(guard2);
    }

    #[test]
    fn test_bp_clear_frames() {
        let num_frames = 10;
        let bp = get_test_bp(num_frames);
        let c_id = 0;

        let mut keys = Vec::new();
        for i in 0..num_frames * 2 {
            let mut guard = bp.create_new_page_for_write(c_id).unwrap();
            guard[0] = i as u8;
            keys.push(guard.page_frame_id().unwrap());
        }

        bp.run_checks();

        // Clear the buffer pool
        bp.flush_all_and_reset().unwrap();

        bp.run_checks();

        // Check the contents of the pages
        for (i, key) in keys.iter().enumerate() {
            let guard = bp.get_page_for_read(*key).unwrap();
            assert_eq!(guard[0], i as u8);
        }

        bp.run_checks();
    }

    #[test]
    fn test_bp_clear_frames_durable() {
        let temp_dir = TempDir::new().unwrap();
        let num_frames = 10;
        let mut keys = Vec::new();

        {
            let cfc = Arc::new(ContainerFileCatalog::new(&temp_dir, false).unwrap());
            let bp1 = BufferPool::new(num_frames, cfc).unwrap();
            let c_key = 0;

            for i in 0..num_frames * 10 {
                let mut guard = bp1.create_new_page_for_write(c_key).unwrap();
                guard[0] = i as u8;
                keys.push(guard.page_frame_id().unwrap());
            }

            bp1.run_checks();

            // Clear the buffer pool
            bp1.flush_all_and_reset().unwrap();

            bp1.run_checks();
        }

        {
            let cfc = Arc::new(ContainerFileCatalog::new(&temp_dir, false).unwrap());
            let bp2 = BufferPool::new(num_frames, cfc).unwrap();

            // Check the contents of the pages
            for (i, key) in keys.iter().enumerate() {
                let guard = bp2.get_page_for_read(*key).unwrap();
                assert_eq!(guard[0], i as u8);
            }

            bp2.run_checks();
        }
    }

    #[test]
    fn test_bp_stats() {
        let num_frames = 1;
        let bp = get_test_bp(num_frames);
        let c_id = 0;

        let key_1 = {
            let mut guard = bp.create_new_page_for_write(c_id).unwrap();
            guard[0] = 1;
            guard.page_frame_id().unwrap()
        };

        let stats = bp.eviction_stats();
        println!("{}", stats);

        let key_2 = {
            let mut guard = bp.create_new_page_for_write(c_id).unwrap();
            guard[0] = 2;
            guard.page_frame_id().unwrap()
        };

        let stats = bp.eviction_stats();
        println!("{}", stats);

        {
            let guard = bp.get_page_for_read(key_1).unwrap();
            assert_eq!(guard[0], 1);
        }

        let stats = bp.eviction_stats();
        println!("{}", stats);

        {
            let guard = bp.get_page_for_read(key_2).unwrap();
            assert_eq!(guard[0], 2);
        }

        let stats = bp.eviction_stats();
        println!("{}", stats);
    }
}
