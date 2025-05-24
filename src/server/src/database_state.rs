use std::collections::hash_map::DefaultHasher;
use std::collections::HashMap;
use std::fs;
use std::fs::File;
use std::hash::{Hash, Hasher};
use std::io::{BufReader, Read};
use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock};

use crate::{StorageManager, StorageTrait};
use common::catalog::{Catalog, CatalogRef};
use common::ids::{AtomicTimeStamp, StateMeta};
use common::physical::col_id_generator::{ColIdGenerator, ColIdGeneratorRef};
use common::physical_expr::physical_rel_expr::PhysicalRelExpr;
use common::query::query_registrar::QueryStateRegistrar;
use common::table::TableInfo;
use common::traits::stat_manager_trait::StatManagerTrait;
use common::traits::state_tracker_trait::StateTrackerTrait;
use common::{prelude::*, QUERY_CACHES_DIR_NAME};
use common::{Attribute, QueryResult};
use queryexe::query::get_attr;
use queryexe::Managers;
use sqlparser::ast::ColumnDef;
use sqlparser::ast::TableConstraint;

use crate::sql_parser::{ParserResponse, SQLParser};

#[derive(Serialize)]
pub struct DatabaseState {
    pub id: u64,
    pub name: String,

    pub catalog: CatalogRef,
    pub col_id_gen: ColIdGeneratorRef,

    #[serde(skip)]
    pub managers: &'static Managers,

    // XTX maybe add the string vec here
    // The list of things stored
    container_vec: Arc<RwLock<HashMap<ContainerId, StateMeta>>>,

    // Time for operations based on timing (typically inserts)
    pub atomic_time: AtomicTimeStamp,

    #[serde(skip)]
    // TODO: query registrar state should be persisted (work around physical plan stuff)
    pub query_registrar: QueryStateRegistrar,

    client_tids: RwLock<HashMap<u64, TransactionId>>,
}

/// This is exclusively for loading in serialized data back to DatabaseState. DO NOT USE FOR ANYTHING ELSE
#[derive(Serialize, Deserialize)]
pub struct SerializedDatabaseState {
    pub id: u64,
    pub name: String,
    pub catalog: CatalogRef,
    pub col_id_gen: ColIdGeneratorRef,
    pub container_vec: HashMap<ContainerId, StateMeta>,
}

#[allow(dead_code)]
impl DatabaseState {
    pub fn get_database_id(db_name: &str) -> u64 {
        let mut s = DefaultHasher::new();
        db_name.hash(&mut s);
        s.finish()
    }

    pub fn get_serializable_db_state(&self) -> SerializedDatabaseState {
        let r = self.container_vec.read().unwrap();
        let container_vec = r.clone();
        SerializedDatabaseState {
            id: self.id,
            name: self.name.clone(),
            catalog: self.catalog.clone(),
            col_id_gen: self.col_id_gen.clone(),
            container_vec,
        }
    }

    pub fn create_db(
        base_dir: &Path,
        db_name: &str,
        managers: &'static Managers,
    ) -> Result<Self, CrustyError> {
        let db_path = base_dir.join(db_name);
        if db_path.exists() {
            // this will no longer ever flag because db states are stored in base_dir/server_state.
            // won't change because going to `else` here is completely safe and we don't expect existence anyways (would've been created on db spin-up)
            DatabaseState::load(db_path, managers)
        } else {
            DatabaseState::new_from_name(db_name, managers)
        }
    }

    pub fn new_from_name(db_name: &str, managers: &'static Managers) -> Result<Self, CrustyError> {
        let db_name: String = String::from(db_name);
        let db_id = DatabaseState::get_database_id(&db_name);
        debug!(
            "Creating new DatabaseState; name: {} id: {}",
            db_name, db_id
        );
        let db_state = DatabaseState {
            id: db_id,
            name: db_name,
            catalog: Catalog::new(),
            col_id_gen: ColIdGenerator::new(),
            managers,
            container_vec: Arc::new(RwLock::new(HashMap::new())),
            atomic_time: common::ids::AtomicTimeStamp::new(0),
            query_registrar: QueryStateRegistrar::new(),
            client_tids: RwLock::new(HashMap::new()),
        };
        Ok(db_state)
    }

    pub fn load(filename: PathBuf, managers: &'static Managers) -> Result<Self, CrustyError> {
        let reader = fs::File::open(filename).expect("error opening db state file");
        let partial_db_state_info: SerializedDatabaseState =
            serde_json::from_reader(reader).expect("error reading from json");

        let db_state = DatabaseState {
            id: partial_db_state_info.id,
            name: partial_db_state_info.name,
            catalog: partial_db_state_info.catalog,
            col_id_gen: partial_db_state_info.col_id_gen,
            managers,
            container_vec: Arc::new(RwLock::new(partial_db_state_info.container_vec)),
            atomic_time: common::ids::AtomicTimeStamp::new(0), // I thihk it's fine to reset this?
            client_tids: RwLock::new(HashMap::new()),
            query_registrar: QueryStateRegistrar::default(), // TODO: persist query_registrar state and inherit from partial
        };
        Ok(db_state)
    }

    pub fn get_current_time(&self) -> LogicalTimeStamp {
        self.atomic_time.load(std::sync::atomic::Ordering::SeqCst)
    }

    pub fn get_table_names(&self) -> Result<Vec<String>, CrustyError> {
        let tables = self.catalog.get_table_names();
        Ok(tables)
    }

    pub fn get_registered_query_names(&self) -> Result<String, CrustyError> {
        self.query_registrar.get_registered_query_names()
    }

    /// Load in database.
    ///
    /// # Arguments
    ///
    /// * `db` - Name of database to load in.
    /// * `id` - Thread id to get the lock.
    pub fn load_database_from_file(
        _file: fs::File,
        _storage_manager: &StorageManager,
    ) -> Result<CatalogRef, CrustyError> {
        unimplemented!("Reconstruct the catalog from sm")
    }

    /// Creates a new table.
    ///
    /// # Arguments
    ///
    /// * `name` - Name of the new table.
    /// * `cols` - Table columns.
    pub fn create_table(
        &self,
        table_name: &str,
        columns: &[ColumnDef],
        constraints: &[TableConstraint],
    ) -> Result<QueryResult, CrustyError> {
        // Constraints aren't implemented yet

        let table_id = self.catalog.get_table_id(table_name);
        let pks = match SQLParser::get_pks(columns, constraints) {
            Ok(pks) => pks,
            Err(ParserResponse::SQLConstraintError(s)) => return Err(CrustyError::CrustyError(s)),
            _ => unreachable!(),
        };

        let mut attributes: Vec<Attribute> = Vec::new();
        for col in columns {
            let constraint = if pks.contains(&col.name) {
                common::Constraint::PrimaryKey
            } else {
                common::Constraint::None
            };
            let attr = Attribute {
                name: col.name.value.clone().to_string(),
                dtype: get_attr(&col.data_type)?,
                constraint,
            };
            attributes.push(attr);
        }
        let schema = TableSchema::new(attributes);
        debug!("Creating table with schema: {:?}", schema);

        let table_info = TableInfo::new(table_id, table_name.to_string(), schema.clone());
        self.managers.sm.create_container(
            table_id,
            Some(table_name.to_string()),
            common::ids::StateType::BaseTable,
            None,
        )?;
        let res = self.catalog.add_table(table_info);
        if res.is_none() {
            // TODO: This check should be done in the sm.
            return Err(CrustyError::CrustyError(format!(
                "Table {} already exists",
                table_name
            )));
        }
        self.managers.stats.register_table(table_id, schema)?;

        let qr = QueryResult::MessageOnly(format!("Table {} created", table_name));

        Ok(qr)
    }

    pub fn reset(&self) -> Result<(), CrustyError> {
        self.query_registrar.reset()?;
        // get rid of persisted query registrar info and reset
        let mut query_registrar_info_path = PathBuf::new();
        query_registrar_info_path.push(&self.managers.config.db_path);
        query_registrar_info_path.push(QUERY_CACHES_DIR_NAME);
        query_registrar_info_path.push(self.id.to_string());
        fs::remove_dir_all(query_registrar_info_path).ok();

        let mut containers = self.container_vec.write().unwrap();
        containers.clear();
        drop(containers);
        Ok(())
    }

    // NOT BEING USED RN, REFACTORED INPUTS
    // /// Register a new query.
    // ///
    // /// # Arguments
    // ///
    // /// * `query_name` - Query name to register.
    // /// * `query_plan` - Query plan to register.
    // pub fn register_query(
    //     &self,
    //     query_name: String,
    //     json_path: String,
    //     query_plan: Arc<PhysicalRelExpr>,
    // ) -> Result<(), CrustyError> {
    //     self.query_registrar
    //         .register_query(query_name, json_path, query_plan)
    // }

    /// Register a new query.
    ///
    /// # Arguments
    ///
    /// * `query_name` - Query name to register.
    /// * `query_plan` - Query plan to register.
    // NOTE FOR ME: this isn't even actually used anywhere right now
    pub fn register_query_with_result(
        &self,
        query_name: String,
        sql: String,
        json_path: String,
        query_plan: Arc<PhysicalRelExpr>,
        query_result_path: String,
        query_tid: TransactionId,
    ) -> Result<(), CrustyError> {
        self.query_registrar.register_query_with_result(
            query_name,
            sql,
            json_path,
            query_plan,
            query_result_path,
            query_tid,
        )
    }

    /// Update metadata for beginning to run a registered query.
    ///
    /// # Arguments
    ///
    /// * `query_name` - Name of the query.
    /// * `start_timestamp` - Optional start timestamp.
    /// * `end_timestamp` - End timestamp.
    pub fn begin_query(
        &self,
        query_name: &str,
        start_timestamp: Option<LogicalTimeStamp>,
        end_timestamp: LogicalTimeStamp,
    ) -> Result<Arc<PhysicalRelExpr>, CrustyError> {
        let tm = self.managers.tm;
        self.query_registrar
            .begin_query(query_name, start_timestamp, end_timestamp, tm)
    }

    /// Update metadata at end of a query.
    ///
    /// # Arguments
    ///
    /// * `query_name` - Name of the query.
    pub fn finish_query(&self, query_name: &str) -> Result<(), CrustyError> {
        self.query_registrar.finish_query(query_name)
    }

    /// See if query has been executed in the past from physical plan.
    /// If it has, return the old result. Else, return None.
    ///
    /// # Arguments
    ///
    /// * `sql` - sql string inputted by the user.
    pub fn query_result_from_sql(&self, sql: &String) -> Result<Option<QueryResult>, CrustyError> {
        // get name from sql query
        let query_name = match self.query_registrar.get_query_name_from_sql(sql)? {
            Some(n) => n,
            _ => {
                return Ok(None);
            }
        };

        // check that query result is still up-to-date and purge + return if not
        match self
            .query_registrar
            .get_touched_tables_from_name(&query_name)?
        {
            Some(v) => {
                // get last modified table's tid and compare to tid of stored query result
                // NOTE only looking at the high valid for tables both are same
                let latest_tid_val = v
                    .iter()
                    .map(|cid| self.managers.stats.get_ts(cid).1)
                    .max()
                    .unwrap();
                // verify that this tid is less than query tid
                let query_tid = self
                    .query_registrar
                    .get_query_tid_from_name(&query_name)?
                    .unwrap();
                if query_tid.id() <= latest_tid_val {
                    // purge useless result and return None to run og query
                    self.query_registrar.purge_query_with_name(&query_name)?;
                    return Ok(None);
                }
            }
            _ => {
                return Ok(None);
            }
        }

        // search for query name with same name
        let path = match self
            .query_registrar
            .get_query_result_path_from_name(&query_name)?
        {
            Some(p) => p,
            _ => {
                return Ok(None);
            }
        };

        // get old result from found query by reading file
        let file = File::open(path)?;
        let mut buf_reader = BufReader::new(file);
        let mut contents = String::new();
        buf_reader.read_to_string(&mut contents)?;
        let qr: QueryResult = serde_json::from_str(&contents).unwrap();
        Ok(Some(qr))
    }

    // If the client already has a TID, return it; otherwise, assign a new TID.
    pub fn get_or_assign_tid(&self, client_id: u64) -> TransactionId {
        let mut map = self.client_tids.write().unwrap();
        map.entry(client_id).or_default();
        // unwrap the final value
        *map.get(&client_id).unwrap()
    }

    // Force-assign a new TID for this client (e.g., after commit)
    pub fn assign_new_tid(&self, client_id: u64) -> TransactionId {
        let mut map = self.client_tids.write().unwrap();
        let new_tid = TransactionId::new();
        map.insert(client_id, new_tid);
        new_tid
    }

    // Retrieve the TID if it exists
    pub fn get_tid(&self, client_id: u64) -> Option<TransactionId> {
        let map = self.client_tids.read().unwrap();
        map.get(&client_id).copied()
    }
}
