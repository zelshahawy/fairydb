use std::collections::HashMap;
use std::fs;
use std::sync::{Arc, RwLock};

use crate::ids::{ContainerId, LogicalTimeStamp, TransactionId};
use crate::physical_expr::physical_rel_expr::PhysicalRelExpr;
use crate::traits::transaction_manager_trait::TransactionManagerTrait;
use crate::CrustyError;

pub struct QueryStateRegistrar {
    // maps query name to tuple: (query plan hash, plan)
    pub query_plans: Arc<RwLock<HashMap<String, Arc<PhysicalRelExpr>>>>,
    // unused rn (json path is passed as "")
    query_filenames: Arc<RwLock<HashMap<String, String>>>,
    // unused rn (passing 0)
    query_watermarks: Arc<RwLock<HashMap<String, LogicalTimeStamp>>>,
    // in progress queries
    in_progress_queries: Arc<RwLock<HashMap<String, LogicalTimeStamp>>>,
    // maps sql string to query name (which maps to everything else w other fields)
    sql_to_query_name: Arc<RwLock<HashMap<String, String>>>,
    // maps query name to file containing query result
    query_result_filenames: Arc<RwLock<HashMap<String, String>>>,
    // maps query name to its tid
    query_tids: Arc<RwLock<HashMap<String, TransactionId>>>,
}

impl Default for QueryStateRegistrar {
    fn default() -> Self {
        Self::new()
    }
}

impl QueryStateRegistrar {
    pub fn new() -> Self {
        QueryStateRegistrar {
            query_plans: Arc::new(RwLock::new(HashMap::new())),
            query_filenames: Arc::new(RwLock::new(HashMap::new())),
            query_watermarks: Arc::new(RwLock::new(HashMap::new())),
            in_progress_queries: Arc::new(RwLock::new(HashMap::new())),
            query_result_filenames: Arc::new(RwLock::new(HashMap::new())),
            sql_to_query_name: Arc::new(RwLock::new(HashMap::new())),
            query_tids: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub fn reset(&self) -> Result<(), CrustyError> {
        let mut in_prog = self.in_progress_queries.write().unwrap();
        if !in_prog.is_empty() {
            Err(CrustyError::CrustyError(String::from(
                "Queries are in progress cannot drop/reset",
            )))
        } else {
            let mut plans = self.query_plans.write().unwrap();
            let mut files = self.query_filenames.write().unwrap();
            let mut watermarks = self.query_watermarks.write().unwrap();
            plans.clear();
            drop(plans);
            // remove files pointed to by query result serializations
            for file_path in files.values() {
                if let Err(e) = fs::remove_file(file_path) {
                    println!("Error deleting file {}: {}", file_path, e);
                }
            }
            files.clear();
            drop(files);
            watermarks.clear();
            drop(watermarks);
            in_prog.clear();
            drop(in_prog);
            Ok(())
        }
    }

    /// Register a new query.
    ///
    /// # Arguments
    ///
    /// * `query_name` - Query name to register.
    /// * `query_plan` - Query plan to register.
    pub fn register_query(
        &self,
        query_name: String,
        json_path: String,
        query_plan: Arc<PhysicalRelExpr>,
    ) -> Result<(), CrustyError> {
        self.query_plans
            .write()
            .unwrap()
            // we can clone because the plan's contents being the same is all we care about when registering
            .insert(query_name.clone(), query_plan);
        self.query_filenames
            .write()
            .unwrap()
            .insert(query_name.clone(), json_path);
        self.query_watermarks.write().unwrap().insert(query_name, 0);
        Ok(())
    }

    /// Register a new query.
    ///
    /// # Arguments
    ///
    /// * `query_name` - Query name to register.
    /// * `query_plan` - Query plan to register.
    pub fn register_query_with_result(
        &self,
        query_name: String,
        sql: String,
        json_path: String,
        query_plan: Arc<PhysicalRelExpr>,
        query_result_path: String,
        query_tid: TransactionId,
    ) -> Result<(), CrustyError> {
        self.query_plans
            .write()
            .unwrap()
            // we can clone because the plan's contents being the same is all we care about when registering.
            // we also assume that any passed plan has been optimized and thus hashed, so passing it into our map is all good.
            .insert(query_name.clone(), query_plan);
        self.query_filenames
            .write()
            .unwrap()
            .insert(query_name.clone(), json_path);
        self.query_result_filenames
            .write()
            .unwrap()
            .insert(query_name.clone(), query_result_path);
        self.sql_to_query_name
            .write()
            .unwrap()
            .insert(sql.clone(), query_name.clone());
        self.query_tids
            .write()
            .unwrap()
            .insert(query_name.clone(), query_tid);
        self.query_watermarks.write().unwrap().insert(query_name, 0);
        Ok(())
    }

    /// Begin running a registered query.
    ///
    /// # Arguments
    ///
    /// * `query_name` - Query to run.
    /// * `start_timestamp` - Starting timestamp.
    /// * `end_timestamp` - New query watermark if completed.
    pub fn begin_query(
        &self,
        query_name: &str,
        start_timestamp: Option<LogicalTimeStamp>,
        end_timestamp: LogicalTimeStamp,
        txn_manager: &dyn TransactionManagerTrait,
    ) -> Result<Arc<PhysicalRelExpr>, CrustyError> {
        assert!(start_timestamp.unwrap_or(0) <= end_timestamp);

        // Checks transaction not in progress
        if self
            .in_progress_queries
            .read()
            .unwrap()
            .contains_key(query_name)
        {
            return Err(CrustyError::CrustyError(format!(
                "Query \"{}\" already in progress.",
                query_name
            )));
        }

        {
            // Obtain a write lock on the map of query transaction IDs.
            let mut tids = self.query_tids.write().unwrap();
            // If a previous transaction exists for this query, commit it.
            if let Some(old_tid) = tids.remove(query_name) {
                txn_manager.commit_txn(old_tid)?;
            }
            // Create a new transaction ID using the logical time stamp.
            let new_tid = TransactionId::new();
            tids.insert(query_name.to_string(), new_tid);
        }

        match self.query_plans.read().unwrap().get(query_name) {
            Some(physical_plan) => {
                self.in_progress_queries
                    .write()
                    .unwrap()
                    .insert(query_name.to_string(), end_timestamp);
                Ok(Arc::clone(physical_plan))
            }
            None => Err(CrustyError::CrustyError(format!(
                "Query \"{}\" has not been registered.",
                query_name
            ))),
        }
    }

    /// Finish running a registered query.
    ///
    /// # Arguments
    ///
    /// * `query_name` - Query finished.
    pub fn finish_query(&self, query_name: &str) -> Result<(), CrustyError> {
        if !self
            .in_progress_queries
            .read()
            .unwrap()
            .contains_key(query_name)
        {
            Err(CrustyError::CrustyError(format!(
                "Query \"{}\" is not in progress.",
                query_name
            )))
        } else {
            match self
                .in_progress_queries
                .write()
                .unwrap()
                .remove_entry(query_name)
            {
                Some((_, new_watermark)) => {
                    self.query_watermarks
                        .write()
                        .unwrap()
                        .insert(query_name.to_string(), new_watermark);
                    Ok(())
                }
                None => unreachable!(),
            }
        }
    }

    pub fn get_registered_query_names(&self) -> Result<String, CrustyError> {
        let mut registered_query_names_and_paths = Vec::new();
        for (query_name, json_path) in self.query_filenames.read().unwrap().iter() {
            registered_query_names_and_paths
                .push(format!("{} loaded from {}", query_name, json_path));
        }
        let registered_query_names_and_paths = registered_query_names_and_paths.join("\n");
        if registered_query_names_and_paths.is_empty() {
            Ok(String::from("No registered queries"))
        } else {
            Ok(registered_query_names_and_paths)
        }
    }

    pub fn purge_query_with_name(&self, query_name: &String) -> Result<(), CrustyError> {
        self.query_plans.write().unwrap().remove(query_name);
        self.query_filenames.write().unwrap().remove(query_name);
        self.query_watermarks.write().unwrap().remove(query_name);

        // purge serialized result
        let filename = {
            // can't hold read and write locks so change scope
            let qr_map = self.query_result_filenames.read().unwrap();
            qr_map.get(query_name).unwrap().clone()
        };
        fs::remove_file(filename)?;
        self.query_result_filenames
            .write()
            .unwrap()
            .remove(query_name);

        self.query_tids.write().unwrap().remove(query_name);

        // linear search, could optimize w 2 hashmaps but for now may not b worth cuz move away from string matching
        // will either use bimap or ditch string approach eventually. I'm not convinced we even need it now
        self.sql_to_query_name
            .write()
            .unwrap()
            .retain(|_key, val| val != query_name);
        Ok(())
    }

    pub fn get_query_name_from_sql(&self, sql: &String) -> Result<Option<String>, CrustyError> {
        let sql_to_name_map = self.sql_to_query_name.read().unwrap();
        let name = sql_to_name_map.get(sql);
        match name {
            Some(s) => Ok(Some(s.clone())),
            _ => Ok(None),
        }
    }

    pub fn get_query_result_path_from_name(
        &self,
        query_name: &String,
    ) -> Result<Option<String>, CrustyError> {
        let filenames = self.query_result_filenames.read().unwrap();
        let name = filenames.get(query_name);
        match name {
            Some(s) => Ok(Some(s.clone())),
            _ => Ok(None),
        }
    }

    // get all the tables that the inputted query touches
    pub fn get_touched_tables_from_name(
        &self,
        query_name: &String,
    ) -> Result<Option<Vec<ContainerId>>, CrustyError> {
        let physical_plans = self.query_plans.read().unwrap();
        match physical_plans.get(query_name) {
            Some(pp) => {
                let mut container_ids = Vec::new();
                pp.get_tables_involved(&mut container_ids);
                Ok(Some(container_ids))
            }
            _ => Ok(None),
        }
    }

    pub fn get_query_tid_from_name(
        &self,
        query_name: &String,
    ) -> Result<Option<TransactionId>, CrustyError> {
        let tids = self.query_tids.read().unwrap();
        let tid = tids.get(query_name);
        match tid {
            Some(s) => Ok(Some(*s)),
            _ => Ok(None),
        }
    }

    // // maybe we use physical plans to get matching queries later...
    // pub fn get_query_with_pp(&self, pp: &PhysicalRelExpr) -> Option<String> {
    //     let query_plans = self.query_plans.read().unwrap();
    //     for (query_name, query_plan) in query_plans.iter() {
    //         if pp == query_plan { // comparison isn't implemented - ask about hash (?)
    //             return Some(query_name.clone());
    //         }
    //     }
    //     None
    // }
}
