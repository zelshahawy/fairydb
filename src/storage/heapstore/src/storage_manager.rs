use crate::buffer_pool::buffer_pool::{gen_random_pathname, BufferPool};
use crate::buffer_pool::mem_pool_trait::MemPool;
use crate::container_file_catalog::ContainerFileCatalog;
use crate::heap_file::{HeapFile, HeapFileIter};
use common::physical::config::ServerConfig;
use common::prelude::*;
use common::traits::storage_trait::StorageTrait;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

pub const STORAGE_DIR: &str = "heapstore";
type HF = HeapFile<BufferPool>;
pub(crate) type HFs = Arc<RwLock<HashMap<ContainerId, Arc<HF>>>>;

const SM_NAME: &str = "HeapStore";
const BP_FRAMES: usize = 1000;

pub struct StorageManager {
    pub cfc: Arc<ContainerFileCatalog>,
    pub bp: Arc<BufferPool>,
    pub(crate) cid_heapfile_map: HFs,
}

/// The required functions in HeapStore's StorageManager that are specific for HeapFiles
impl StorageManager {
    /// Get the heapfile for a given container id
    fn get_heapfile(&self, c_id: ContainerId) -> Result<Arc<HF>, CrustyError> {
        let files = self.cid_heapfile_map.read().unwrap();
        if let Some(hf) = files.get(&c_id) {
            Ok(hf.clone())
        } else {
            Err(CrustyError::StorageError)
        }
    }

    /// Get the number of pages for a container
    #[allow(dead_code)]
    pub(crate) fn get_num_pages(&self, container_id: ContainerId) -> PageId {
        // If the container is not found, return 0
        self.get_heapfile(container_id)
            .map(|hf| hf.num_pages())
            .unwrap_or(0)
    }
}

/// Implementation of storage trait
impl StorageTrait for StorageManager {
    type ValIterator = HeapFileIter<BufferPool>;

    fn get_name(&self) -> &'static str {
        SM_NAME
    }

    /// Create a new storage manager that will use storage_dir as the location to persist data
    /// (if the storage manager persists records on disk; not the case for memstore)
    /// For startup/shutdown: check the storage_dir for data persisted in shutdown() that you can
    /// use to populate this instance of the SM. Otherwise create a new one.
    fn new(config: &'static ServerConfig) -> Self {
        let dir = &config.db_path.join(STORAGE_DIR);
        let cfc = Arc::new(ContainerFileCatalog::new(dir, false).unwrap());
        let bp = Arc::new(BufferPool::new(BP_FRAMES, cfc.clone()).unwrap());

        // For each file in the cfc, create a heapfile object
        let mut hf_map = HashMap::new();
        for c_id in cfc.container_ids() {
            //TODO milestone hs
            // Load the heapfile and add it to hf_map
            let hf = Arc::new(HeapFile::load(c_id, bp.clone()).unwrap());
            hf_map.insert(c_id, hf);
        }

        StorageManager {
            bp,
            cfc,
            cid_heapfile_map: Arc::new(RwLock::new(hf_map)),
        }
    }

    /// Create a new storage manager for testing. There is no startup/shutdown logic here: it
    /// should simply create a fresh SM and set is_temp to true
    fn new_test_sm() -> Self {
        // Get OS temp directory
        let temp_dir = std::env::temp_dir();
        let base_dir = gen_random_pathname(Some("test_bp"));
        let dir = temp_dir.join(base_dir);
        let cfc = Arc::new(ContainerFileCatalog::new(dir, true).unwrap());
        let bp = Arc::new(BufferPool::new(BP_FRAMES, cfc.clone()).unwrap());

        StorageManager {
            cfc,
            bp,
            cid_heapfile_map: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Insert some bytes into a container for a particular value (e.g. record).
    /// Any validation will be assumed to happen before.
    /// Returns the value id associated with the stored value.
    /// Function will need to find the first page that can hold the value.
    /// A new page may need to be created if no space on existing pages can be found.
    /// Reference: Riki's implementation in the `tpch` branch.
    fn insert_value(&self, c_id: ContainerId, value: Vec<u8>, _tid: TransactionId) -> ValueId {
        self.get_heapfile(c_id).unwrap().add_val(&value).unwrap()
    }

    /// Insert some bytes into a container for vector of values (e.g. record).
    /// Any validation will be assumed to happen before.
    /// Returns a vector of value ids associated with the stored values.
    /// Reference: Riki's implementation in the `tpch` branch.
    fn insert_values(
        &self,
        c_id: ContainerId,
        values: Vec<Vec<u8>>,
        _tid: TransactionId,
    ) -> Vec<ValueId> {
        let hf = self.get_heapfile(c_id).unwrap();
        hf.add_vals(values.into_iter()).unwrap()
    }

    /// Delete the data for a value. If the valueID is not found it returns Ok() still.
    fn delete_value(&self, id: ValueId, _tid: TransactionId) -> Result<(), CrustyError> {
        let hf = self.get_heapfile(id.container_id)?;
        // Missing page_id or slot_id is “not found”
        let pid = id.page_id.ok_or(CrustyError::StorageError)?;
        let slot = id.slot_id.ok_or(CrustyError::StorageError)?;
        // If the page doesn’t even exist, that’s an error (test_not_found)
        let num_pages = hf.num_pages();
        if pid >= num_pages {
            return Err(CrustyError::StorageError);
        }
        // just ok lil bro
        match hf.delete_val(pid, slot) {
            Ok(()) => Ok(()),
            Err(CrustyError::StorageError) => Ok(()),
            Err(e) => Err(e),
        }
    }

    /// Updates a value. Returns valueID on update (which may have changed). Error on failure
    /// Any process that needs to determine if a value changed will need to compare the return valueId against
    /// the sent value.
    fn update_value(
        &self,
        value: Vec<u8>,
        id: ValueId,
        _tid: TransactionId,
    ) -> Result<ValueId, CrustyError> {
        let hf = self.get_heapfile(id.container_id)?;
        let pid = id.page_id.ok_or(CrustyError::StorageError)?;
        let slot_id = id.slot_id.ok_or(CrustyError::StorageError)?;
        let val_id = hf.update_val(pid, slot_id, &value)?;
        Ok(val_id)
    }

    /// Create a new container to be stored.
    /// fn create_container(&self, name: String) -> ContainerId;
    /// Creates a new container object.
    /// For this milestone you will not need to utilize
    /// the container_config, name, container_type, or dependencies
    ///
    ///
    /// # Arguments
    ///
    /// * `container_id` - Id of container to add delta to.
    fn create_container(
        &self,
        container_id: ContainerId,
        _name: Option<String>,                   // Not used in this milestone
        _container_type: common::ids::StateType, // Not used in this milestone
        _dependencies: Option<Vec<ContainerId>>, // Not used in this milestone
    ) -> Result<(), CrustyError> {
        // If the container already exists, return an error.
        // Otherwise create a new container and add it to the map.
        // Call create_container in the buffer pool to create the container there.
        // Initialize the container as a heapfile amd add to the cid_heapfile_map
        let mut files = self.cid_heapfile_map.write().unwrap();
        if files.contains_key(&container_id) {
            return Err(CrustyError::StorageError);
        }
        let hf = Arc::new(HeapFile::new(container_id, self.bp.clone()).unwrap());
        files.insert(container_id, hf);
        Ok(())
    }

    /// A wrapper function to call create container
    fn create_table(&self, container_id: ContainerId) -> Result<(), CrustyError> {
        self.create_container(container_id, None, common::ids::StateType::BaseTable, None)
    }

    /// Remove the container and all stored values in the container.
    /// If the container is persisted remove the underlying files
    fn remove_container(&self, container_id: ContainerId) -> Result<(), CrustyError> {
        panic!("Not implemented {container_id}. not needed for hs");
        //Ok(())
    }

    /// Get an iterator that returns all valid records
    fn get_iterator(
        &self,
        container_id: ContainerId,
        _tid: TransactionId,
        _perm: Permissions,
    ) -> Self::ValIterator {
        let hf = self.get_heapfile(container_id).expect("unknown container");
        hf.iter()
    }

    /// Get an iterator that returns all valid records starting from a particular value id
    fn get_iterator_from(
        &self,
        container_id: ContainerId,
        _tid: TransactionId,
        _perm: Permissions,
        start: ValueId,
    ) -> Self::ValIterator {
        let hf = self.get_heapfile(container_id).expect("unknown container");
        // tests pass you a 1‐based page_id; convert back to 0‐based
        hf.iter_from(start.page_id.unwrap(), start.slot_id.unwrap())
    }

    /// Get the data for a particular ValueId. Error if does not exists
    fn get_value(
        &self,
        id: ValueId,
        _tid: TransactionId,
        _perm: Permissions,
    ) -> Result<Vec<u8>, CrustyError> {
        let pid = id.page_id.ok_or(CrustyError::StorageError)?;
        let slot_id = id.slot_id.ok_or(CrustyError::StorageError)?;
        let hf = self.get_heapfile(id.container_id)?;
        let val = hf.get_val(pid, slot_id)?;
        Ok(val)
    }

    /// Testing utility to reset all state associated the storage manager. Deletes all data in
    /// storage path (keeping storage path as a directory). Doesn't need to serialize any data to
    /// disk as its just meant to clear state.
    ///
    /// Clear any data structures in the SM you add
    fn reset(&self) -> Result<(), CrustyError> {
        // Reset from the upper layer
        // 1. Deallocate the heapfile objects
        let mut files = self.cid_heapfile_map.write().unwrap();
        files.clear();
        // 2. Clear the buffer pool memory
        self.bp.reset().unwrap();
        // 3. Clear the container manager
        self.cfc.remove_all();
        Ok(())
    }

    // Clear the in-memory states of the buffer pool
    fn clear_cache(&self) {
        self.bp.reset().unwrap();
    }

    // Make sure all data is flushed to disk
    fn shutdown(&self) {
        self.bp.flush_all().unwrap();
    }
}
