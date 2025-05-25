use std::sync::atomic::{AtomicU32, Ordering};

/// Structure to hold file statistics
/// This structure is used to keep track of the number of reads and writes
/// that are done on a file.
pub struct FileStats {
    pub buffered_read_count: AtomicU32,
    pub buffered_write_count: AtomicU32,
    pub direct_read_count: AtomicU32,
    pub direct_write_count: AtomicU32,
}

impl Clone for FileStats {
    fn clone(&self) -> Self {
        FileStats {
            buffered_read_count: AtomicU32::new(self.buffered_read_count.load(Ordering::Acquire)),
            buffered_write_count: AtomicU32::new(self.buffered_write_count.load(Ordering::Acquire)),
            direct_read_count: AtomicU32::new(self.direct_read_count.load(Ordering::Acquire)),
            direct_write_count: AtomicU32::new(self.direct_write_count.load(Ordering::Acquire)),
        }
    }
}

impl std::fmt::Display for FileStats {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Buffered read count: {}, Buffered write count: {}, Direct read count: {}, Direct write count: {}",
            self.buffered_read_count.load(Ordering::Acquire),
            self.buffered_write_count.load(Ordering::Acquire),
            self.direct_read_count.load(Ordering::Acquire),
            self.direct_write_count.load(Ordering::Acquire),
        )
    }
}

impl Default for FileStats {
    fn default() -> Self {
        Self::new()
    }
}

impl FileStats {
    pub fn new() -> Self {
        FileStats {
            buffered_read_count: AtomicU32::new(0),
            buffered_write_count: AtomicU32::new(0),
            direct_read_count: AtomicU32::new(0),
            direct_write_count: AtomicU32::new(0),
        }
    }

    pub fn read_count(&self) -> u32 {
        self.buffered_read_count.load(Ordering::Acquire)
            + self.direct_read_count.load(Ordering::Acquire)
    }

    pub fn inc_read_count(&self, _direct: bool) {
        #[cfg(feature = "stat")]
        {
            if _direct {
                self.direct_read_count.fetch_add(1, Ordering::AcqRel);
            } else {
                self.buffered_read_count.fetch_add(1, Ordering::AcqRel);
            }
        }
    }

    pub fn write_count(&self) -> u32 {
        self.buffered_write_count.load(Ordering::Acquire)
            + self.direct_write_count.load(Ordering::Acquire)
    }

    pub fn inc_write_count(&self, _direct: bool) {
        #[cfg(feature = "stat")]
        {
            if _direct {
                self.direct_write_count.fetch_add(1, Ordering::AcqRel);
            } else {
                self.buffered_write_count.fetch_add(1, Ordering::AcqRel);
            }
        }
    }

    #[allow(dead_code)]
    pub fn reset(&self) {
        self.buffered_read_count.store(0, Ordering::Release);
        self.buffered_write_count.store(0, Ordering::Release);
        self.direct_read_count.store(0, Ordering::Release);
        self.direct_write_count.store(0, Ordering::Release);
    }
}
