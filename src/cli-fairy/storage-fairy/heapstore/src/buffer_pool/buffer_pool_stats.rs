use std::sync::atomic::{AtomicUsize, Ordering};

/// Statistics kept by the buffer pool.
/// These statistics are used for decision making.
pub(crate) struct BPStats {
    new_page_request: AtomicUsize,
    read_request: AtomicUsize,
    read_request_waiting_for_write: AtomicUsize,
    write_request: AtomicUsize,
}

impl std::fmt::Display for BPStats {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "New Page: {}\nRead Count: {}\nWrite Count: {}",
            self.new_page_request.load(Ordering::Relaxed),
            self.read_request.load(Ordering::Relaxed),
            self.write_request.load(Ordering::Relaxed)
        )
    }
}

impl BPStats {
    pub fn new() -> Self {
        BPStats {
            new_page_request: AtomicUsize::new(0),
            read_request: AtomicUsize::new(0),
            read_request_waiting_for_write: AtomicUsize::new(0),
            write_request: AtomicUsize::new(0),
        }
    }

    pub fn clear(&self) {
        self.new_page_request.store(0, Ordering::Relaxed);
        self.read_request.store(0, Ordering::Relaxed);
        self.read_request_waiting_for_write
            .store(0, Ordering::Relaxed);
        self.write_request.store(0, Ordering::Relaxed);
    }

    pub fn new_page(&self) -> usize {
        self.new_page_request.load(Ordering::Relaxed)
    }

    pub fn inc_new_page(&self) {
        #[cfg(feature = "stat")]
        self.new_page_request.fetch_add(1, Ordering::Relaxed);
    }

    pub fn inc_new_pages(&self, _num_pages: usize) {
        #[cfg(feature = "stat")]
        self.new_page_request
            .fetch_add(_num_pages, Ordering::Relaxed);
    }

    pub fn read_count(&self) -> usize {
        self.read_request.load(Ordering::Relaxed)
    }

    pub fn inc_read_count(&self) {
        #[cfg(feature = "stat")]
        self.read_request.fetch_add(1, Ordering::Relaxed);
    }

    pub fn read_request_waiting_for_write_count(&self) -> usize {
        self.read_request_waiting_for_write.load(Ordering::Relaxed)
    }

    pub fn inc_read_request_waiting_for_write_count(&self) {
        #[cfg(feature = "stat")]
        self.read_request_waiting_for_write
            .fetch_add(1, Ordering::Relaxed);
    }

    pub fn write_count(&self) -> usize {
        self.write_request.load(Ordering::Relaxed)
    }

    pub fn inc_write_count(&self) {
        #[cfg(feature = "stat")]
        self.write_request.fetch_add(1, Ordering::Relaxed);
    }
}
