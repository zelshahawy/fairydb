use super::buffer_frame::{FrameReadGuard, FrameWriteGuard};
use crate::buffer_pool::mem_stats::MemoryStats;
use common::ids::{ContainerId, ContainerPageId, PageId};

#[derive(Debug, PartialEq)]
pub enum MemPoolStatus {
    BaseFileNotFound,
    BaseFileError(String),
    PageNotFound,
    FrameReadLatchGrantFailed,
    FrameWriteLatchGrantFailed,
    CannotEvictPage,
}

impl From<std::io::Error> for MemPoolStatus {
    fn from(s: std::io::Error) -> Self {
        MemPoolStatus::BaseFileError(s.to_string())
    }
}

pub trait MemPool: Sync + Send {
    /// Create a container.
    /// A container is basically a file in the file system if a disk-based storage is used.
    /// If an in-memory storage is used, a container is a logical separation of pages.
    /// This function will register a container in the memory pool.
    /// If a page write is requested to a unregistered container, a container will be lazily created
    /// and the page will be written to the container file.
    /// Therefore, calling this function is not mandatory unless you want to ensure that
    /// the container is created or you want to create a temporary container.
    /// Creation of a already created container will be ignored.
    fn create_container(&self, c_id: ContainerId, is_temp: bool) -> Result<(), MemPoolStatus>;

    /// Drop a container.
    /// This only makes the container temporary, that is, it ensures that future write
    /// requests to the container will be ignored. This does not guarantee that the pages
    /// of the container are deleted from the disk or memory.
    /// However, the page eviction policy could use this information to guide
    /// the eviction of the pages in the memory pool as evicting pages of a temporary container
    /// is virtually free.
    fn drop_container(&self, c_id: ContainerId) -> Result<(), MemPoolStatus>;

    /// Create a new page for write.
    /// This function will allocate a new page in memory and return a FrameWriteGuard.
    /// In general, this function does not need to write the page to disk.
    /// Disk write will be handled when the page is evicted from the buffer pool.
    /// This function will not guarantee that the returned page is zeroed out but the
    /// page header will be initialized with a correct page id.
    /// The caller must initialize the page content before writing any data to disk.
    fn create_new_page_for_write(
        &self,
        c_id: ContainerId,
    ) -> Result<FrameWriteGuard<'_>, MemPoolStatus>;

    /// Create new pages for write.
    /// This function will allocate multiple new pages in memory and return a list of FrameWriteGuard.
    /// In general, this function does not need to write the page to disk.
    /// Disk write will be handled when the page is evicted from the buffer pool.
    /// This function will return available pages in the memory pool.
    /// It does not guarantee that the returned vector will have the requested number of pages.
    /// This function will not guarantee that the returned pages are zeroed out but the
    /// page headers will be initialized with correct page ids.
    /// The caller must initialize the pages content before writing any data to disk.
    fn create_new_pages_for_write(
        &self,
        c_id: ContainerId,
        num_pages: usize,
    ) -> Result<Vec<FrameWriteGuard<'_>>, MemPoolStatus>;

    /// Check if a page is cached in the memory pool.
    /// This function will return true if the page is in memory, false otherwise.
    /// There are no side effects of calling this function.
    /// That is, the page will not be loaded into memory.
    fn is_in_mem(&self, key: PageFrameId) -> bool;

    /// Get the max page id for a given container
    /// This function will return the max page id for the container
    /// or None if the container is not found.
    fn get_max_page_id(&self, c_id: ContainerId) -> Option<PageId>;

    /// Get the list of page frame keys for a container.
    /// This function will return a list of page frame keys for the container
    /// that are currently in the memory pool.
    fn get_page_ids_in_mem(&self, c_id: ContainerId) -> Vec<PageFrameId>;

    /// Get a page for write.
    /// This function will return a FrameWriteGuard.
    /// This function assumes that a page is already created and either in memory or on disk.
    fn get_page_for_write(&self, key: PageFrameId) -> Result<FrameWriteGuard<'_>, MemPoolStatus>;

    /// Get a page for read.
    /// This function will return a FrameReadGuard.
    /// This function assumes that a page is already created and either in memory or on disk.
    fn get_page_for_read(&self, key: PageFrameId) -> Result<FrameReadGuard<'_>, MemPoolStatus>;

    /// Prefetch page
    /// Load the page into memory so that read access will be faster.
    fn prefetch_page(&self, key: PageFrameId) -> Result<(), MemPoolStatus>;

    /// Persist all the dirty pages to disk.
    /// This function will not deallocate the memory pool.
    /// This does not clear out the frames in the memory pool.
    fn flush_all(&self) -> Result<(), MemPoolStatus>;

    /// Persist all the dirty pages to disk and reset the memory pool.
    /// This function will not deallocate the memory pool but
    /// clears out all the frames in the memory pool.
    /// After calling this function, pages will be read from disk when requested.
    fn flush_all_and_reset(&self) -> Result<(), MemPoolStatus>;

    /// Clear the dirty flags of all the pages in the memory pool.
    /// This function will not deallocate the memory pool.
    /// This does not clear out the frames in the memory pool.
    /// Dirty pages will not be written to disk.
    /// This function is used for experiments to avoid writing pages to disk.
    fn clear_dirty_flags(&self) -> Result<(), MemPoolStatus>;

    /// Reset the memory pool to its initial state.
    /// This function will not deallocate the memory pool
    /// but clears out all the frames.
    /// Additionally, it will remove all the files from the disk
    /// if the memory pool is disk-based.
    fn reset(&self) -> Result<(), MemPoolStatus>;

    /// Tell the memory pool that a page in the frame should be evicted as soon as possible.
    /// This function will not evict the page immediately.
    /// This function is used as a hint to the memory pool to evict the page when possible.
    fn fast_evict(&self, frame_id: u32) -> Result<(), MemPoolStatus>;

    /// Return the runtime statistics of the memory pool.
    fn stats(&self) -> MemoryStats;

    /// Reset the runtime statistics of the memory pool.
    fn reset_stats(&self);
}

/// Page frame id is used to access a page in the buffer pool.
/// It contains not only the page id but also the frame id in the buffer pool.
/// The frame id is used as a hint to access the page in O(1) time.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct PageFrameId {
    p_key: ContainerPageId,
    frame_id: u32, // Frame id in the buffer pool
}

impl PageFrameId {
    pub fn new(c_id: ContainerId, page_id: PageId) -> Self {
        PageFrameId {
            p_key: ContainerPageId::new(c_id, page_id),
            frame_id: u32::MAX,
        }
    }

    pub fn new_with_frame_id(c_id: ContainerId, page_id: PageId, frame_id: u32) -> Self {
        PageFrameId {
            p_key: ContainerPageId::new(c_id, page_id),
            frame_id,
        }
    }

    pub fn p_key(&self) -> ContainerPageId {
        self.p_key
    }

    pub fn frame_id(&self) -> u32 {
        self.frame_id
    }

    pub fn set_frame_id(&mut self, frame_id: u32) {
        self.frame_id = frame_id;
    }

    #[allow(clippy::wrong_self_convention)]
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&self.p_key.c_id.to_be_bytes()); // 2 bytes
        bytes.extend_from_slice(&self.p_key.page_id.to_be_bytes()); // 4 bytes
        bytes.extend_from_slice(&self.frame_id.to_be_bytes()); // 4 bytes
        bytes
    }

    pub fn from_bytes(bytes: &[u8]) -> Self {
        let c_id = u16::from_be_bytes(bytes[0..2].try_into().unwrap());
        let page_id = PageId::from_be_bytes(bytes[2..6].try_into().unwrap());
        let frame_id = u32::from_be_bytes(bytes[6..10].try_into().unwrap());
        PageFrameId {
            p_key: ContainerPageId::new(c_id, page_id),
            frame_id,
        }
    }
}

impl std::fmt::Display for PageFrameId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "({}, f:{})", self.p_key, self.frame_id)
    }
}
