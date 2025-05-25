use common::physical::config::ServerConfig;
use common::prelude::*;
use common::traits::storage_trait::StorageTrait;

use std::collections::HashMap;
use std::fs::{self, OpenOptions};
use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock};

pub const STORAGE_DIR: &str = "memstore";
const SM_NAME: &str = "MemStore";

/// This is the basic data structure a container that maps a value ID to bytes
type ContainerMap = Arc<RwLock<HashMap<ValueId, Vec<u8>>>>;

/// The MemStore StorageManager. A map for storing containers, a map for tracking the next insert ID,
/// and where to persist on shutdown/startup
pub struct StorageManager {
    containers: Arc<RwLock<HashMap<ContainerId, ContainerMap>>>,
    last_insert: Arc<RwLock<HashMap<ContainerId, ValueId>>>,
    storage_dir: Option<PathBuf>,
    container_names: Arc<RwLock<HashMap<String, ContainerId>>>,
}

impl Drop for StorageManager {
    fn drop(&mut self) {
        info!("Dropping Storage Manager");
        //self.reset().unwrap_or_else( |_| error!("Error resetting SM"));
    }
}
impl StorageTrait for StorageManager {
    type ValIterator = ValueIterator;

    /// Create a new SM from scratch or create containers from files.
    fn new(config: &'static ServerConfig) -> Self {
        let storage_dir = config.db_path.clone();
        if storage_dir.exists() {
            info!(
                "Initializing memstore::storage_manager from path: {:?}",
                &storage_dir
            );
            StorageManager::load(&storage_dir)
        } else {
            info!(
                "Creating new memstore::storage_manager with path: {:?}",
                &storage_dir
            );
            StorageManager {
                containers: Arc::new(RwLock::new(HashMap::new())),
                last_insert: Arc::new(RwLock::new(HashMap::new())),
                storage_dir: Some(storage_dir.to_path_buf()),
                container_names: Arc::new(RwLock::new(HashMap::new())),
            }
        }
    }

    /// Create a new SM that will not be persisted
    fn new_test_sm() -> Self {
        StorageManager {
            containers: Arc::new(RwLock::new(HashMap::new())),
            last_insert: Arc::new(RwLock::new(HashMap::new())),
            storage_dir: None,
            container_names: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Insert bytes into a container
    fn insert_value(
        &self,
        container_id: ContainerId,
        value: Vec<u8>,
        _tid: TransactionId,
    ) -> ValueId {
        // Get the container
        let mut containers = self.containers.write().unwrap();
        // Find key to insert
        let mut last_insert = self.last_insert.write().unwrap();
        // Get the container map to allow the insert
        let mut vals = containers
            .get_mut(&container_id)
            .expect("Container ID Missing on insert")
            .write()
            .unwrap();
        let next_slot = match last_insert.get(&container_id) {
            None => 0,
            Some(slot) => slot.slot_id.expect("Missing SlotId") + 1,
        };
        //TODO check if exits first in case of mistake
        let rid = ValueId {
            container_id,
            segment_id: None,
            page_id: None,
            slot_id: Some(next_slot),
        };
        debug!(
            "memstore:storage_manager insert key: {:?} value: {:?}",
            &rid, &value
        );
        vals.insert(rid, value);
        last_insert.insert(container_id, rid);
        rid
    }

    /// Insert multiple values
    fn insert_values(
        &self,
        container_id: ContainerId,
        values: Vec<Vec<u8>>,
        tid: TransactionId,
    ) -> Vec<ValueId> {
        let mut ret = Vec::new();
        for x in values {
            ret.push(self.insert_value(container_id, x, tid));
        }
        ret
    }

    /// Remove the value from the container
    fn delete_value(&self, id: ValueId, _tid: TransactionId) -> Result<(), CrustyError> {
        let containers = self.containers.write().unwrap();
        if containers.contains_key(&id.container_id) {
            let mut table_map = containers.get(&id.container_id).unwrap().write().unwrap();
            if table_map.contains_key(&id) {
                table_map.remove(&id);
                Ok(())
            } else {
                //Key not found, no need to delete.
                Ok(())
            }
        } else {
            Err(CrustyError::CrustyError(String::from(
                "File ID not found for recordID",
            )))
        }
    }

    /// Updates a value. Returns record ID on update (which may have changed). Error on failure
    fn update_value(
        &self,
        value: Vec<u8>,
        id: ValueId,
        _tid: TransactionId,
    ) -> Result<ValueId, CrustyError> {
        self.delete_value(id, _tid)?;
        Ok(self.insert_value(id.container_id, value, _tid))
    }

    /// Add a new container
    fn create_container(
        &self,
        container_id: ContainerId,
        name: Option<String>,
        _container_type: StateType,
        dependencies: Option<Vec<ContainerId>>,
    ) -> Result<(), CrustyError> {
        if let Some(c_name) = name {
            let mut map = self.container_names.write().unwrap();
            if let std::collections::hash_map::Entry::Vacant(e) = map.entry(c_name) {
                e.insert(container_id);
            } else {
                return Err(CrustyError::ExecutionError(String::from(
                    "Container with name already exists",
                )));
            }
        }

        if dependencies.is_some() {
            //FIXME add meta tracking
            unimplemented!();
        }
        let mut containers = self.containers.write().unwrap();
        if containers.contains_key(&container_id) {
            debug!(
                "memstore::create_container container_id: {:?} already exists",
                &container_id
            );
            return Ok(());
        }
        debug!(
            "memstore::create_container container_id: {:?} does not exist yet",
            &container_id
        );
        containers.insert(container_id, Arc::new(RwLock::new(HashMap::new())));
        Ok(())
    }

    fn create_table(&self, container_id: ContainerId) -> Result<(), CrustyError> {
        self.create_container(container_id, None, StateType::BaseTable, None)
    }

    /// Remove the container and all stored values in the container.
    /// If the container is persisted remove the underlying files
    fn remove_container(&self, container_id: ContainerId) -> Result<(), CrustyError> {
        let mut containers = self.containers.write().unwrap();
        if !containers.contains_key(&container_id) {
            debug!(
                "memstore::remove_container container_id: {:?} does not exist",
                &container_id
            );
            return Ok(());
        }
        debug!(
            "memstore::remove_container container_id: {:?} exists. dropping",
            &container_id
        );
        containers.remove(&container_id).unwrap();
        Ok(())
    }

    /// Get an iterator for a container
    fn get_iterator(
        &self,
        container_id: ContainerId,
        _tid: TransactionId,
        _perm: Permissions,
    ) -> ValueIterator {
        let table_map = self
            .containers
            .read()
            .unwrap()
            .get(&container_id)
            .unwrap()
            .clone();
        let last_insert = self.last_insert.read().unwrap();
        debug!("memstore::get_iterator container_id: {:?}", &container_id);
        let max = last_insert
            .get(&container_id)
            .unwrap_or(&ValueId::new(container_id))
            .slot_id
            .unwrap_or(0);
        ValueIterator::new(table_map, container_id, max)
    }

    fn get_iterator_from(
        &self,
        _container_id: ContainerId,
        _tid: TransactionId,
        _perm: Permissions,
        _start: ValueId,
    ) -> Self::ValIterator {
        unimplemented!()
    }

    /// Get the bytes for a given value if found
    fn get_value(
        &self,
        id: ValueId,
        _tid: TransactionId,
        _perm: Permissions,
    ) -> Result<Vec<u8>, CrustyError> {
        let containers = self.containers.read().unwrap();
        if containers.contains_key(&id.container_id) {
            let map = containers.get(&id.container_id).unwrap().read().unwrap();
            if map.contains_key(&id) {
                Ok(map.get(&id).unwrap().clone())
            } else {
                Err(CrustyError::ExecutionError(format!(
                    "Record ID not found {:?}",
                    id
                )))
            }
        } else {
            Err(CrustyError::ExecutionError(format!(
                "File ID not found {:?}",
                id
            )))
        }
    }

    fn reset(&self) -> Result<(), CrustyError> {
        let mut containers = self.containers.write().unwrap();
        let mut last_inserts = self.last_insert.write().unwrap();
        let mut container_names = self.container_names.write().unwrap();
        containers.clear();
        last_inserts.clear();
        container_names.clear();
        Ok(())
    }

    fn clear_cache(&self) {
        // No cache here
    }

    fn shutdown(&self) {
        info!("Shutting down and persisting containers");
        if let Some(storage_dir) = &self.storage_dir {
            fs::create_dir_all(storage_dir).expect("Unable to create dir to store SM");
            let containers = self.containers.read().unwrap();
            for (c_id, vals_lock) in containers.iter() {
                let vals = vals_lock.read().unwrap();
                let mut file_path = storage_dir.join(format!("{}", c_id));
                file_path.set_extension("ms");
                let file = std::fs::File::create(file_path).expect("Failed to create file");
                serde_cbor::to_writer(file, &*vals).expect("Failed on persisting container");
            }
        } else {
            info!("Test SM or no path, not persisting");
        }
    }

    fn get_name(&self) -> &'static str {
        SM_NAME
    }
}

impl StorageManager {
    /// Create a Memstore SM from a file path and populate from the files
    fn load(storage_dir: &Path) -> Self {
        let mut container_map = HashMap::new();
        let mut last_ins = HashMap::new();
        // Find the files that end with .ms
        let entries: Vec<fs::DirEntry> = fs::read_dir(storage_dir)
            .unwrap()
            .filter_map(Result::ok)
            .filter(|x| x.path().extension().unwrap() == "ms")
            .collect();
        // populate
        for entry in entries {
            // Open the file
            let file = OpenOptions::new()
                .read(true)
                .open(entry.path())
                .expect("Failed to read file");

            // Create the container be using serde to de-serialize the file
            let container: HashMap<ValueId, Vec<u8>> =
                serde_cbor::from_reader(file).expect("cannot read file");

            // The file name contains the CID
            let cid: ContainerId = entry
                .path()
                .file_stem()
                .unwrap()
                .to_string_lossy()
                .to_string()
                .parse::<ContainerId>()
                .unwrap();
            // Find the max key for the next insert key
            let mut max_val: ValueId = ValueId {
                container_id: cid,
                segment_id: None,
                page_id: None,
                slot_id: Some(0),
            };
            for key in container.keys() {
                if let Some(slot) = key.slot_id {
                    if slot > max_val.slot_id.unwrap() {
                        max_val = *key;
                    }
                }
            }
            container_map.insert(cid, Arc::new(RwLock::new(container)));
            last_ins.insert(cid, max_val);
        }

        StorageManager {
            containers: Arc::new(RwLock::new(container_map)),
            last_insert: Arc::new(RwLock::new(last_ins)),
            storage_dir: Some(storage_dir.to_path_buf()),
            container_names: Arc::new(RwLock::new(HashMap::new())),
        }
    }
}

// The iterator struct
pub struct ValueIterator {
    tracker: ValueId,
    max: u16,
    table_map: ContainerMap,
    current: u16,
}

impl ValueIterator {
    //Create a new iterator for a container
    fn new(table_map: ContainerMap, container_id: ContainerId, max: u16) -> Self {
        debug!("new iterator {:?} max {}", container_id, max);
        let mut tracker = ValueId::new(container_id);
        tracker.slot_id = Some(0);
        ValueIterator {
            tracker,
            max,
            table_map,
            current: 0,
        }
    }
}

impl Iterator for ValueIterator {
    type Item = (Vec<u8>, ValueId);
    fn next(&mut self) -> Option<Self::Item> {
        while self.current <= self.max {
            match self.table_map.read().unwrap().get(&self.tracker) {
                Some(res) => {
                    self.tracker.slot_id = Some(self.tracker.slot_id.unwrap() + 1);
                    self.current += 1;
                    return Some((res.clone(), self.tracker));
                }
                None => {
                    self.tracker.slot_id = Some(self.tracker.slot_id.unwrap() + 1);
                    self.current += 1;
                }
            }
        }
        None
    }
}
