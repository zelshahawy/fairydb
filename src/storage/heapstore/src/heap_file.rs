use crate::buffer_pool::buffer_frame::FrameReadGuard;
use crate::buffer_pool::buffer_frame::FrameWriteGuard;
use crate::buffer_pool::mem_pool_trait::MemPool;
use crate::buffer_pool::mem_pool_trait::PageFrameId;
use crate::heap_page;
use crate::heap_page::HeapPage;
#[allow(unused_imports)]
use common::ids::AtomicPageId;
use common::prelude::*;
#[allow(unused_imports)]
use std::sync::atomic::Ordering;
use std::sync::Arc;

/// The struct for a heap file.
pub(crate) struct HeapFile<T: MemPool> {
    c_id: ContainerId,
    bp: Arc<T>,
    last_insert_page: AtomicPageId,
}

/// HeapFile required functions
impl<T: MemPool> HeapFile<T> {
    /// Helper function to fetch a page for read from the buffer pool.
    fn get_page_for_read(&self, page_id: PageId) -> FrameReadGuard {
        self.bp
            .get_page_for_read(PageFrameId::new(self.c_id, page_id))
            .unwrap()
    }

    /// Helper function to fetch a page for write from the buffer pool.
    fn get_page_for_write(&self, page_id: PageId) -> FrameWriteGuard {
        self.bp
            .get_page_for_write(PageFrameId::new(self.c_id, page_id))
            .unwrap()
    }

    /// Create a brand-new heap file for container `c_id`.
    pub fn new(c_id: ContainerId, mem_pool: Arc<T>) -> Result<Self, FairyError> {
        // Note that the header page is always page 0, and the data pages start from 1.
        // You may not end up using the header page, but some tests will assume this.

        // Add any extra initialization code in this function.
        let mut header = mem_pool
            .create_new_page_for_write(c_id)
            .map_err(|_| FairyError::StorageError)?;

        header.init_heap_page();

        let heap_file = HeapFile {
            c_id,
            bp: mem_pool.clone(),
            last_insert_page: AtomicPageId::new(0),
        };
        Ok(heap_file)
    }

    /// Load an existing heap file.
    pub fn load(c_id: ContainerId, mem_pool: Arc<T>) -> Result<Self, FairyError> {
        let max_page = mem_pool
            .get_max_page_id(c_id)
            .ok_or(FairyError::StorageError)?;

        let hf = HeapFile {
            c_id,
            bp: mem_pool.clone(),
            last_insert_page: AtomicPageId::new(max_page),
        };

        Ok(hf)
    }

    /// Return the number of pages for this HeapFile.
    /// Return type is PageId (alias for another type) as we cannot have more
    /// pages than PageId can hold.
    pub fn num_pages(&self) -> PageId {
        self.bp.get_max_page_id(self.c_id).unwrap_or(0)
    }

    /// Read a value at (page_id, slot_id) from the heap file.
    pub fn get_val(&self, page_id: PageId, slot_id: SlotId) -> Result<Vec<u8>, FairyError> {
        let page = self.get_page_for_read(page_id);
        page.get_value(slot_id)
            .map(|s| s.to_vec())
            .ok_or(FairyError::StorageError)
    }

    // Delete a value at (page_id, slot_id) from the heap file.
    pub fn delete_val(&self, page_id: PageId, slot_id: SlotId) -> Result<(), FairyError> {
        if page_id == 0 || page_id > self.num_pages() {
            return Err(FairyError::StorageError);
        }
        let mut frame = self.get_page_for_write(page_id);
        frame
            .delete_value(slot_id)
            .ok_or(FairyError::StorageError)?;
        Ok(())
    }

    pub fn update_val(
        &self,
        page_id: PageId,
        slot_id: SlotId,
        val: &[u8],
    ) -> Result<ValueId, FairyError> {
        self.delete_val(page_id, slot_id)?;
        let new_vid = self.add_val(val)?;
        Ok(new_vid)
    }

    // This function is not implemented in a thread-safe way. Can cause deadlocks when used in a multi-threaded environment.
    // We do not care about this for now.
    pub fn add_val(&self, val: &[u8]) -> Result<ValueId, FairyError> {
        let max_pid = self.num_pages();
        let last = self.last_insert_page.load(Ordering::Relaxed);
        if last > 0 && last <= max_pid {
            let mut frame = self.get_page_for_write(last);
            if let Some(slot) = frame.add_value(val) {
                // still fits on same page
                self.last_insert_page.store(last, Ordering::Relaxed);
                return Ok(ValueId {
                    container_id: self.c_id,
                    page_id: Some(last),
                    slot_id: Some(slot),
                    segment_id: Some(0),
                });
            }
        }

        // 2) Nope, allocate a brand‐new page at the end
        let mut new_frame = self
            .bp
            .create_new_page_for_write(self.c_id)
            .map_err(|_| FairyError::StorageError)?;
        new_frame.init_heap_page();
        let slot = new_frame.add_value(val).ok_or(FairyError::StorageError)?;
        let pid = new_frame.page_id().unwrap().page_id;

        // remember for next time
        self.last_insert_page.store(pid, Ordering::Relaxed);

        Ok(ValueId {
            container_id: self.c_id,
            page_id: Some(pid),
            slot_id: Some(slot),
            segment_id: Some(0),
        })
    }

    #[allow(dead_code)]
    pub fn add_vals(
        &self,
        iter: impl Iterator<Item = Vec<u8>>,
    ) -> Result<Vec<ValueId>, FairyError> {
        let mut val_ids = Vec::new();
        for val in iter {
            let val_id = self.add_val(&val)?;
            val_ids.push(val_id);
        }
        Ok(val_ids)
    }

    pub fn iter(self: &Arc<Self>) -> HeapFileIter<T> {
        // Create the HeapFileIter
        HeapFileIter::new_from(self.clone(), 0, 0)
    }

    pub fn iter_from(self: &Arc<Self>, page_id: PageId, slot_id: SlotId) -> HeapFileIter<T> {
        // Create the HeapFileIter
        HeapFileIter::new_from(self.clone(), page_id, slot_id)
    }
}

pub struct HeapFileIter<T: MemPool> {
    /// We are providing the elements of the iterator that we used, you are allowed to
    /// use them in the iterator or make changes. If you change the elements, you
    /// will want to change the new_from constructor to use the new elements.
    heapfile: Arc<HeapFile<T>>,
    initialized: bool,
    finished: bool,
    page_id: PageId,
    slot_id: SlotId,
    max_page: PageId,
    current_frame: Option<FrameReadGuard<'static>>,
    current_iter: Option<heap_page::HeapPageIter<'static>>,
    value_buffer: Vec<u8>,
}

impl<T: MemPool> HeapFileIter<T> {
    fn new_from(heapfile: Arc<HeapFile<T>>, page_id: PageId, slot_id: SlotId) -> Self {
        let max_page = heapfile.num_pages();
        HeapFileIter {
            heapfile,
            initialized: false,
            finished: false,
            page_id: page_id.max(1), // Start from page 1 at minimum (skip header)
            slot_id,
            max_page,
            current_frame: None,
            current_iter: None,
            // Pre-allocate with a reasonable capacity to avoid reallocations
            value_buffer: Vec::with_capacity(4096),
        }
    }

    // Helper function to get a page for read from the buffer pool.
    fn get_page(&self, page_id: PageId) -> FrameReadGuard<'static> {
        // Safety: self.heapfile object has a reference to the buffer pool
        // which makes sure that the frame is not deallocated while this
        // (self) object is alive.
        let page = self.heapfile.get_page_for_read(page_id);
        unsafe { std::mem::transmute::<FrameReadGuard, FrameReadGuard<'static>>(page) }
    }

    fn initialize(&mut self) {
        if !self.initialized {
            self.max_page = self.heapfile.num_pages();
            self.initialized = true;
        }
    }

    fn load_page(&mut self) -> bool {
        self.current_frame = None;
        self.current_iter = None;

        if self.page_id > self.max_page {
            return false;
        }

        let frame = self.get_page(self.page_id);

        let iter = if self.slot_id > 0 {
            frame.iter_from(self.slot_id)
        } else {
            frame.iter()
        };

        let iter = unsafe {
            std::mem::transmute::<heap_page::HeapPageIter<'_>, heap_page::HeapPageIter<'static>>(
                iter,
            )
        };
        self.current_frame = Some(frame);
        self.current_iter = Some(iter);

        true
    }
}

impl<T: MemPool> Iterator for HeapFileIter<T> {
    type Item = (Vec<u8>, ValueId);

    /// This function is called to get the next element of the iterator.
    /// It should return None when the iterator is finished.
    /// Otherwise it should return Some((val, val_id)).
    /// The val is the value that was read from the heap file.
    /// The val_id is the ValueId that was read from the heap file.
    fn next(&mut self) -> Option<Self::Item> {
        self.initialize();
        if self.finished {
            return None;
        }

        loop {
            // Load page if needed
            // this should short circuit.
            if self.current_iter.is_none() && !self.load_page() {
                self.finished = true;
                return None;
            }
            if let Some(iter) = &mut self.current_iter {
                if let Some((bytes, slot)) = iter.next() {
                    self.value_buffer.clear();
                    self.value_buffer.extend_from_slice(bytes);

                    let value_id = ValueId {
                        container_id: self.heapfile.c_id,
                        page_id: Some(self.page_id),
                        slot_id: Some(slot),
                        segment_id: Some(0),
                    };
                    let result_buffer = std::mem::replace(
                        &mut self.value_buffer,
                        Vec::with_capacity(bytes.len().max(4096)),
                    );

                    return Some((result_buffer, value_id));
                } else {
                    self.page_id += 1;
                    self.slot_id = 0;
                    self.current_frame = None;
                    self.current_iter = None;
                    continue;
                }
            }
        }
    }
}
