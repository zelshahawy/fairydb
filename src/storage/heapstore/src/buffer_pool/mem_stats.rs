use crate::buffer_pool::mem_pool_trait::MemPoolStatus;
use common::ids::ContainerId;
use std::collections::BTreeMap;

impl std::fmt::Display for MemPoolStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            MemPoolStatus::BaseFileNotFound => write!(f, "[MP] File manager not found"),
            MemPoolStatus::BaseFileError(s) => s.fmt(f),
            MemPoolStatus::PageNotFound => write!(f, "[MP] Page not found"),
            MemPoolStatus::FrameReadLatchGrantFailed => {
                write!(f, "[MP] Frame read latch grant failed")
            }
            MemPoolStatus::FrameWriteLatchGrantFailed => {
                write!(f, "[MP] Frame write latch grant failed")
            }
            MemPoolStatus::CannotEvictPage => {
                write!(f, "[MP] All frames are latched and cannot evict page")
            }
        }
    }
}

pub struct MemoryStats {
    // Buffer pool stats
    pub bp_num_frames_in_mem: usize,
    pub bp_new_page: usize,        // Total number of new pages created (BP)
    pub bp_read_frame: usize,      // Total number of frames requested for read (BP)
    pub bp_read_frame_wait: usize, // Total number of frames requested for read but had to wait (BP)
    pub bp_write_frame: usize,     // Total number of frames requested for write (BP)
    pub bp_num_frames_per_container: BTreeMap<ContainerId, i64>, // Number of pages of each container in BP

    // Disk stats
    pub disk_created: usize, // Total number of pages created (DISK)
    pub disk_read: usize,    // Total number of pages read (DISK)
    pub disk_write: usize,   // Total number of pages written (DISK)
    pub disk_io_per_container: BTreeMap<ContainerId, (i64, i64, i64)>, // Number of pages created, read, and written for each container
}

impl Default for MemoryStats {
    fn default() -> Self {
        Self::new()
    }
}

impl MemoryStats {
    pub fn new() -> Self {
        MemoryStats {
            bp_num_frames_in_mem: 0,
            bp_new_page: 0,
            bp_read_frame: 0,
            bp_read_frame_wait: 0,
            bp_write_frame: 0,
            bp_num_frames_per_container: BTreeMap::new(),
            disk_created: 0,
            disk_read: 0,
            disk_write: 0,
            disk_io_per_container: BTreeMap::new(),
        }
    }

    pub fn diff(&self, previous: &MemoryStats) -> MemoryStats {
        assert_eq!(self.bp_num_frames_in_mem, previous.bp_num_frames_in_mem);
        MemoryStats {
            bp_num_frames_in_mem: self.bp_num_frames_in_mem,
            bp_new_page: self.bp_new_page - previous.bp_new_page,
            bp_read_frame: self.bp_read_frame - previous.bp_read_frame,
            bp_read_frame_wait: self.bp_read_frame_wait - previous.bp_read_frame_wait,
            bp_write_frame: self.bp_write_frame - previous.bp_write_frame,
            bp_num_frames_per_container: self
                .bp_num_frames_per_container
                .iter()
                .map(|(k, v)| {
                    let prev = previous.bp_num_frames_per_container.get(k).unwrap_or(&0);
                    (*k, v - prev)
                })
                .collect(),
            disk_created: self.disk_created - previous.disk_created,
            disk_read: self.disk_read - previous.disk_read,
            disk_write: self.disk_write - previous.disk_write,
            disk_io_per_container: self
                .disk_io_per_container
                .iter()
                .map(|(k, v)| {
                    let prev = previous.disk_io_per_container.get(k).unwrap_or(&(0, 0, 0));
                    (*k, (v.0 - prev.0, v.1 - prev.1, v.2 - prev.2))
                })
                .collect(),
        }
    }
}

impl std::fmt::Display for MemoryStats {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "Buffer pool stats:")?;
        writeln!(
            f,
            "  Number of frames in memory: {}",
            self.bp_num_frames_in_mem
        )?;
        writeln!(f, "  Number of new pages created: {}", self.bp_new_page)?;
        writeln!(
            f,
            "  Number of frames requested for read: {}",
            self.bp_read_frame
        )?;
        writeln!(
            f,
            "  Number of frames requested for read but had to wait: {}",
            self.bp_read_frame_wait
        )?;
        writeln!(
            f,
            "  Number of frames requested for write: {}",
            self.bp_write_frame
        )?;
        writeln!(f, "  Number of frames for each container:")?;
        for (c_id, num_pages) in &self.bp_num_frames_per_container {
            writeln!(f, "    {}: {}", c_id, num_pages)?;
        }
        writeln!(f, "Disk stats:")?;
        writeln!(f, "  Number of pages created: {}", self.disk_created)?;
        writeln!(f, "  Number of pages read: {}", self.disk_read)?;
        writeln!(f, "  Number of pages written: {}", self.disk_write)?;
        writeln!(f, "  Number of pages read and written for each container:")?;
        for (c_id, (num_created, num_read, num_write)) in &self.disk_io_per_container {
            writeln!(
                f,
                "    {}: created={}, read={}, written={}",
                c_id, num_created, num_read, num_write
            )?;
        }
        Ok(())
    }
}
