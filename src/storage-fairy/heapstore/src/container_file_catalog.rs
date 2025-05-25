#[cfg(not(feature = "mock"))]
use crate::base_file::BaseFile;
use crate::base_file::BaseFileTrait;
#[cfg(feature = "mock")]
use crate::base_file_mock::BaseFileMock as BaseFile;
use crate::buffer_pool::mem_pool_trait::MemPoolStatus;
use crate::file_stats::FileStats;
use crate::page::Page;
use common::ids::{AtomicPageId, ContainerId, PageId};
use dashmap::DashMap;
use std::fs::create_dir_all;
use std::path::{Path, PathBuf};
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};

/// A wrapper struct for the container base file.
/// It contains the page count and a flag to indicate if the container is temporary.
pub struct Container {
    page_count: AtomicPageId,
    is_temp: AtomicBool,
    base_file: BaseFile,
}

impl Container {
    pub fn new(base_file: BaseFile) -> Self {
        Container {
            page_count: AtomicPageId::new(base_file.num_pages().try_into().unwrap()),
            is_temp: AtomicBool::new(false),
            base_file,
        }
    }

    pub fn new_temp(base_file: BaseFile) -> Self {
        Container {
            page_count: AtomicPageId::new(base_file.num_pages().try_into().unwrap()),
            is_temp: AtomicBool::new(true),
            base_file,
        }
    }

    pub fn set_temp(&self, is_temp: bool) {
        self.is_temp.store(is_temp, Ordering::Relaxed);
    }

    pub fn is_temp(&self) -> bool {
        self.is_temp.load(Ordering::Relaxed)
    }

    pub fn num_pages(&self) -> PageId {
        self.page_count.load(Ordering::Relaxed)
    }

    pub fn num_pages_in_disk(&self) -> PageId {
        self.base_file.num_pages().try_into().unwrap()
    }

    pub fn inc_page_count(&self, count: usize) -> PageId {
        self.page_count
            .fetch_add(count as PageId, Ordering::Relaxed)
    }

    pub fn get_stats(&self) -> FileStats {
        self.base_file.get_stats()
    }

    pub fn read_page(&self, page_id: PageId, page: &mut Page) -> Result<(), std::io::Error> {
        self.base_file.read_page(page_id, page)
    }

    pub fn write_page(&self, page_id: PageId, page: &Page) -> Result<(), std::io::Error> {
        if !self.is_temp() {
            // Does not write to the file if the container is temporary.
            self.base_file.write_page(page_id, page)
        } else {
            Ok(())
        }
    }

    pub fn flush(&self) -> Result<(), std::io::Error> {
        self.base_file.flush()
    }
}

/// ContainerFileCatalog is a catalog of containers. It is used to manage the containers
/// and their corresponding files.
/// It also determines if the collection is temporary or not.
pub struct ContainerFileCatalog {
    remove_dir_on_drop: bool,
    base_dir: PathBuf,
    /// A concurrent/thread-safe map of container ids to their corresponding containers.
    containers: DashMap<ContainerId, Arc<Container>>, // c_id -> Container
}

impl ContainerFileCatalog {
    /// Directory structure
    /// * base_dir
    ///    * db_dir
    ///      * container_file
    ///
    /// A call to new will create the base_dir if it does not exist.
    /// The db_dir and container_file are lazily created when a BaseFile is requested.
    /// If remove_dir_on_drop is true, then the base_dir is removed when the ContainerManager is dropped.
    pub fn new<P: AsRef<Path>>(
        base_dir: P,
        remove_dir_on_drop: bool,
    ) -> Result<Self, std::io::Error> {
        trace!("Creating/Reading containers in {:?}", &base_dir.as_ref());
        // Identify all the directories. A directory corresponds to a database.
        // A file in the directory corresponds to a container.
        // Create a BaseFile for each file and store it in the container.
        // If base_dir does not exist, then create it.
        create_dir_all(&base_dir)?;

        let containers = DashMap::new();
        for entry in std::fs::read_dir(&base_dir).unwrap() {
            let entry = entry.unwrap();
            let file_path = entry.path();
            if file_path.is_file() {
                let c_id = file_path
                    .file_name()
                    .unwrap()
                    .to_str()
                    .unwrap()
                    .parse()
                    .unwrap();
                let fm = BaseFile::new(&base_dir, c_id).unwrap();
                containers.insert(c_id, Arc::new(Container::new(fm)));
            }
        }

        Ok(ContainerFileCatalog {
            remove_dir_on_drop,
            base_dir: base_dir.as_ref().to_path_buf(),
            containers,
        })
    }

    pub fn container_ids(&self) -> Vec<ContainerId> {
        self.containers.iter().map(|c| *c.key()).collect()
    }

    pub fn remove_dir_on_drop(&self) -> bool {
        self.remove_dir_on_drop
    }

    // Return the file manager for the given container key with a counter for the number of pages.
    pub fn get_container(&self, c_id: ContainerId) -> Arc<Container> {
        let container = self.containers.entry(c_id).or_insert_with(|| {
            let db_path = &self.base_dir;
            let fm = BaseFile::new(db_path, c_id).unwrap();
            Arc::new(Container::new(fm))
        });
        container.value().clone()
    }

    pub fn get_container_page_count(&self, c_id: ContainerId) -> Option<PageId> {
        Some(self.containers.get(&c_id)?.num_pages())
    }

    pub fn register_container(&self, c_id: ContainerId, is_temp: bool) {
        self.containers.entry(c_id).or_insert_with(|| {
            let db_path = &self.base_dir;
            let fm = BaseFile::new(db_path, c_id).unwrap();
            if is_temp {
                Arc::new(Container::new_temp(fm))
            } else {
                Arc::new(Container::new(fm))
            }
        });
    }

    pub fn get_stats(&self) -> Vec<(ContainerId, (PageId, FileStats))> {
        let mut vec = Vec::new();
        for container in self.containers.iter() {
            let count = container.num_pages();
            let stats = container.get_stats();
            vec.push((*container.key(), (count, stats)));
        }
        vec
    }

    pub fn flush_all(&self) -> Result<(), MemPoolStatus> {
        for container in self.containers.iter() {
            container.flush()?;
        }
        Ok(())
    }

    pub fn remove_all(&self) {
        // Remove all the containers from the map.
        self.containers.clear();
        // Remove all the files from the base directory.
        trace!("Removing containers in {:?}", &self.base_dir);
        for entry in std::fs::read_dir(&self.base_dir).unwrap() {
            let entry = entry.unwrap();
            let file_path = entry.path();
            if file_path.is_file() {
                std::fs::remove_file(file_path).unwrap();
            }
        }
    }

    // Iterate over all the containers
    pub fn iter(&self) -> impl Iterator<Item = (ContainerId, Arc<Container>)> + '_ {
        self.containers
            .iter()
            .map(|c| (*c.key(), c.value().clone()))
    }
}

impl Drop for ContainerFileCatalog {
    fn drop(&mut self) {
        if self.remove_dir_on_drop {
            trace!("Dropping and Removing containers in {:?}", self.base_dir);
            std::fs::remove_dir_all(&self.base_dir).unwrap();
        }
    }
}
