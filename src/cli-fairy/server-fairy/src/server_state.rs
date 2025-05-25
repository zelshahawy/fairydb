use std::collections::{hash_map::Entry, HashMap};
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::RwLock;

use crate::database_state::DatabaseState;

use common::error::c_err;
use common::{CrustyError, QUERY_CACHES_DIR_NAME};

use queryexe::Managers;

const SERVER_STATE_DIR: &str = "server_state";

/// A struct that holds information about
/// which client is connected to which database.
pub struct ServerState {
    /// Path where serialized database files are stored.
    pub server_state_dir: PathBuf,
    // maps database id to DatabaseState
    pub id_to_db: RwLock<HashMap<u64, &'static DatabaseState>>,
    /// active connections indicates what client_id is connected to what db_id
    pub active_connections: RwLock<HashMap<u64, u64>>, //xtx type these chnage to a struct  and update the tids
    pub managers: &'static Managers,
}

impl ServerState {
    pub(crate) fn new(base_dir: &Path, managers: &'static Managers) -> Result<Self, CrustyError> {
        // Create databases
        let server_state_dir = base_dir.join(SERVER_STATE_DIR);
        debug!("Looking for databases in {:?}", server_state_dir);

        let mut db_map = HashMap::new();
        if server_state_dir.exists() {
            debug!("server_state_dir exists");
            //TODO ae filter for directories with naming convention or read from file.
            let dbs = fs::read_dir(&server_state_dir).expect("Unable to read DB storage dir");
            {
                // for each path, create a DatabaseState
                for db in dbs {
                    let db = db.unwrap();
                    let db_path = db.path();
                    info!("Found persisted database {:?}", db_path);
                    let db_box = Box::new(DatabaseState::load(db_path, managers)?);
                    let db_state: &'static DatabaseState = Box::leak(db_box);
                    db_map.insert(db_state.id, db_state);
                }
            }
        } else {
            info!("server_state_dir does not exist. creating it");

            //Create the directory for storing DB data
            fs::create_dir_all(&server_state_dir).expect("Error creating storage directory for DB");
        }

        let server_state = ServerState {
            id_to_db: RwLock::new(db_map),
            active_connections: RwLock::new(HashMap::new()),
            server_state_dir,
            managers,
        };

        Ok(server_state)
    }

    pub fn get_connected_db(&self, client_id: u64) -> Result<&'static DatabaseState, CrustyError> {
        let active_connections = self.active_connections.read().unwrap();
        match active_connections.get(&client_id) {
            Some(db_id) => {
                let id_to_db = self.id_to_db.read().unwrap();
                match id_to_db.get(db_id) {
                    Some(db) => Ok(db),
                    None => Err(CrustyError::CrustyError(format!(
                        "database with id {:?} is not found",
                        db_id
                    ))),
                }
            }
            None => Err(CrustyError::CrustyError(format!(
                "client with id {:?} is not connected to a database",
                client_id
            ))),
        }
    }

    pub fn get_db_names(&self) -> Vec<String> {
        let id_to_db = self.id_to_db.read().unwrap();
        id_to_db.values().map(|db| db.name.clone()).collect()
    }

    /// Reset the server
    pub fn reset(&self) -> Result<(), CrustyError> {
        // Clear out each DB state
        let mut id_to_db = self.id_to_db.write().unwrap();
        for db in id_to_db.values() {
            db.reset()?;
        }
        id_to_db.clear();

        // Reset active connections
        let mut active_connections = self.active_connections.write().unwrap();
        active_connections.clear();

        // clear db state caches
        fs::remove_dir_all(self.server_state_dir.clone())?;
        fs::create_dir_all(self.server_state_dir.clone())?;

        // clear the manager states
        self.managers.reset()?;

        // remove manager persisted data
        fs::remove_dir_all(self.managers.path.clone()).ok(); // try to remove but will fail if base_dir is empty - this is ok
        fs::create_dir_all(self.managers.path.clone())?;

        Ok(())
    }

    pub(crate) fn shutdown(&self) -> Result<(), CrustyError> {
        info!("Shutting down");

        // Shutdown/persist DB state

        if self.managers.config.shutdown_purge {
            self.reset().unwrap();
        } else {
            // prevent others from accessing while persisting
            let id_to_db = self.id_to_db.read().unwrap();

            if self.server_state_dir.exists() {
                debug!("Saving DB state to {:?}", self.server_state_dir);
                // persist each db state for reading upon reboot
                for (id, db_state) in id_to_db.iter() {
                    db_state.query_registrar.reset()?; // no support for tids having across shutdown
                    let mut path = self.server_state_dir.clone();
                    path.push(id.to_string());
                    serde_json::to_writer(
                        fs::File::create(path).expect("error creating file"),
                        &db_state.get_serializable_db_state(),
                    )
                    .map_err(|e| c_err(&format!("failed to serialize: {}", e)))?;
                }
            } else {
                error!("server_state_dir should exist");
                return Err(CrustyError::IOError(
                    "server_state_dir should exist".to_string(),
                ));
            }

            // purge registered queries
            let mut query_registrar_info_path = PathBuf::new();
            query_registrar_info_path.push(&self.managers.config.db_path);
            query_registrar_info_path.push(QUERY_CACHES_DIR_NAME);
            fs::remove_dir_all(query_registrar_info_path).ok();
        }

        // call shutdown on SM to ensure stateful shutdown
        self.managers.shutdown();
        Ok(())
    }

    pub fn create_new_db(&self, name: &str) -> Result<(), CrustyError> {
        let db_id = DatabaseState::get_database_id(name);

        let mut id_to_db = self.id_to_db.write().unwrap();

        match id_to_db.entry(db_id) {
            Entry::Occupied(_) => Err(CrustyError::CrustyError(format!(
                "database with name {:?} already exists",
                name
            ))),
            Entry::Vacant(entry) => {
                let db_state = DatabaseState::new_from_name(name, self.managers).map_err(|e| {
                    CrustyError::CrustyError(format!("Failed to create database state: {}", e))
                })?;
                entry.insert(Box::leak(Box::new(db_state)));
                Ok(())
            }
        }
    }

    pub fn connect_to_db(&self, db_name: &str, client_id: u64) -> Result<(), CrustyError> {
        let db_id = self.get_db_id_from_name(db_name)?;
        let mut active_connections = self.active_connections.write().unwrap();
        active_connections.insert(client_id, db_id);
        Ok(())
    }

    pub fn close_connection(&self, client_id: u64) {
        let mut active_connections = self.active_connections.write().unwrap();
        active_connections.remove(&client_id);
    }

    fn get_db_id_from_name(&self, db_name: &str) -> Result<u64, CrustyError> {
        let map_ref = self.id_to_db.read().unwrap();
        for (db_id, db_state) in map_ref.iter() {
            if db_state.name == db_name {
                return Ok(*db_id);
            }
        }
        Err(CrustyError::CrustyError(String::from("db_name not found!")))
    }
}
