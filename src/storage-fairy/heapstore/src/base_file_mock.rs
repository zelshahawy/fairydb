use std::sync::atomic::{AtomicUsize, Ordering};

use common::{ids::ContainerId, PAGE_SIZE};

use crate::{base_file::BaseFileTrait, file_stats::FileStats, page::Page};

#[allow(dead_code)]
/// BaseFileMock is a mock implementation of the BaseFileTrait.
pub struct BaseFileMock {
    num_pages: AtomicUsize,
    c_id: ContainerId,
    stats: FileStats,
    mock_page: Page,
    direct: bool,
}

pub const SPIN_TIME_READ_MICRO_SEC: f64 = 2.94; // 340K IOPS
pub const SPIN_TIME_WRITE_MICRO_SEC: f64 = 3.64; // 275K IOPS

impl BaseFileMock {
    #[allow(dead_code)]
    pub fn new<P: AsRef<std::path::Path>>(
        _db_dir: P,
        c_id: ContainerId,
    ) -> Result<Self, std::io::Error> {
        let mut mock_page = Page::new_empty();
        // fill the page with c_id as u8
        let data: [u8; PAGE_SIZE] = [c_id as u8; PAGE_SIZE];
        mock_page.data.copy_from_slice(&data);
        Ok(BaseFileMock {
            num_pages: AtomicUsize::new(0),
            c_id,
            stats: FileStats::new(),
            mock_page,
            direct: true,
        })
    }
}

impl BaseFileTrait for BaseFileMock {
    fn num_pages(&self) -> usize {
        self.num_pages.load(Ordering::Relaxed)
    }

    fn get_stats(&self) -> FileStats {
        self.stats.clone()
    }

    fn prefetch_page(&self, _page_id: u32) -> Result<(), std::io::Error> {
        Ok(())
    }

    fn read_page(&self, page_id: u32, page: &mut Page) -> Result<(), std::io::Error> {
        self.stats.inc_read_count(self.direct);
        // Simulate a delay for the read operation
        std::thread::sleep(std::time::Duration::from_nanos(
            (SPIN_TIME_READ_MICRO_SEC * 1000.0) as u64,
        ));
        page.data.copy_from_slice(&self.mock_page.data);
        page.set_page_id(page_id);
        Ok(())
    }

    fn write_page(&self, page_id: u32, _page: &Page) -> Result<(), std::io::Error> {
        self.stats.inc_write_count(self.direct);
        // Atomic maximum to ensure thread safety
        self.num_pages
            .fetch_max(page_id as usize, Ordering::Relaxed);
        std::thread::sleep(std::time::Duration::from_nanos(
            (SPIN_TIME_WRITE_MICRO_SEC * 1000.0) as u64,
        ));
        Ok(())
    }

    fn flush(&self) -> Result<(), std::io::Error> {
        Ok(())
    }
}
