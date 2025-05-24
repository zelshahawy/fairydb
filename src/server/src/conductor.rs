use std::path::Path;
use std::sync::Arc;

use crate::database_state::DatabaseState;

use crate::sql_parser::{ParserResponse, SQLParser};
use crate::Executor;

use common::error::c_err;
use common::ids::TransactionId;
use common::util::data_reader::CsvReader;

use common::physical_expr::physical_rel_expr::PhysicalRelExpr;
use common::query::rules::Rules;
use common::{CrustyError, QueryResult};

use queryexe::query::planner::physical_plan_to_op_iterator;
use queryexe::query::translate_and_validate::{get_name, Query};
use queryexe::query::Translator;
use queryexe::Managers;
use sqlparser::ast::{SetExpr, Statement};
use std::fs::OpenOptions;

use txn_manager::transactions::Transaction;

#[allow(unused_imports)]
use optimizer::cost::dummy_cost_model::{DummyCost, DummyCostModel};
#[allow(unused_imports)]
use optimizer::mock_optimizer::MockOptimizer;

type ConductorCostModel = DummyCostModel;
type Optimizer = MockOptimizer<ConductorCostModel>;

/// Conductor runs query to the database
pub struct Conductor {
    pub parser: SQLParser,
    pub optimizer: Optimizer,
    pub executor: Executor,
    pub active_txn: Transaction,
}

impl Conductor {
    pub fn new(managers: &'static Managers) -> Result<Self, CrustyError> {
        let parser = SQLParser::new();
        let optimizer = Optimizer::new(ConductorCostModel::new(managers.stats), managers);
        let executor = Executor::new_ref(managers);
        let conductor = Conductor {
            parser,
            optimizer,
            executor,
            active_txn: Transaction::new(),
        };
        Ok(conductor)
    }

    /// implemented so that we maintain a tid consistent w the db_state
    pub fn new_from_tid(
        managers: &'static Managers,
        tid: TransactionId,
    ) -> Result<Self, CrustyError> {
        let parser = SQLParser::new();
        let optimizer = Optimizer::new(ConductorCostModel::new(managers.stats), managers);
        let executor = Executor::new_ref(managers);
        let conductor = Conductor {
            parser,
            optimizer,
            executor,
            active_txn: Transaction::new_from_tid(tid),
        };
        Ok(conductor)
    }

    pub fn run_sql_from_string(
        &mut self,
        sql: String,
        db_state: &'static DatabaseState,
    ) -> Result<QueryResult, CrustyError> {
        debug!("Parsing SQL: {:?}", &sql);
        match SQLParser::parse_sql(sql) {
            ParserResponse::SQL(ast) => self.run_sql(ast, db_state),
            ParserResponse::SQLError(e) => Err(c_err(format!("SQL error: {}", e).as_str())),
            ParserResponse::SQLConstraintError(msg) => {
                Err(c_err(format!("SQL constraint error: {}", msg).as_str()))
            }
            ParserResponse::Err => Err(c_err("Unknown error parsing SQL")),
        }
    }

    pub fn to_logical_plan(
        &self,
        sql: &str,
        db_state: &'static DatabaseState,
    ) -> Result<Query, CrustyError> {
        match SQLParser::parse_sql(sql.to_string()) {
            ParserResponse::SQL(ast) => {
                debug!("Processing SQL: {:?}", sql);
                match ast.first().unwrap() {
                    Statement::Query(qbox) => {
                        debug!("Obtaining Logical Plan from query's AST");
                        let enabled_rules = Arc::new(Rules::default());
                        Translator::from_sql(
                            qbox,
                            &db_state.catalog,
                            &enabled_rules,
                            &db_state.col_id_gen,
                        )
                        .map_err(|e| c_err(format!("{}", e).as_str()))
                    }
                    _ => Err(c_err("Not a query")),
                }
            }
            ParserResponse::SQLError(e) => Err(c_err(format!("SQL error: {}", e).as_str())),
            ParserResponse::SQLConstraintError(msg) => {
                Err(c_err(format!("SQL constraint error: {}", msg).as_str()))
            }
            ParserResponse::Err => Err(c_err("Unknown error parsing SQL")),
        }
    }

    pub fn to_physical_plan(
        &self,
        logical_plan: Query,
        db_state: &'static DatabaseState,
    ) -> Result<PhysicalRelExpr, CrustyError> {
        Ok(self
            .optimizer
            .optimize(&logical_plan, Some(&db_state.query_registrar)))
    }

    pub fn run_physical_plan(
        &mut self,
        physical_plan: PhysicalRelExpr,
        db_state: &'static DatabaseState,
    ) -> Result<QueryResult, CrustyError> {
        let op_iterator = physical_plan_to_op_iterator(
            db_state.managers,
            &db_state.catalog,
            &physical_plan,
            self.active_txn.tid()?,
            db_state.get_current_time(),
        )?;
        // We populate the executor with the state: physical plan, and storage manager ref
        self.executor.configure_query(op_iterator);

        // Finally, execute the query
        self.executor.execute()
    }

    pub fn run_opiterator(
        &mut self,
        op_iterator: Box<dyn queryexe::opiterator::OpIterator>,
    ) -> Result<QueryResult, CrustyError> {
        // We populate the executor with the state: physical plan, and storage manager ref
        self.executor.configure_query(op_iterator);

        // Finally, execute the query
        self.executor.execute()
    }

    fn run_sql(
        &mut self,
        ast: Vec<Statement>,
        db_state: &'static DatabaseState,
    ) -> Result<QueryResult, CrustyError> {
        if ast.is_empty() {
            return Err(c_err("Empty SQL command"));
        }
        match ast.first().unwrap() {
            Statement::CreateTable {
                name: table_name,
                columns,
                constraints,
                ..
            } => {
                debug!("Processing CREATE table: {:?}", table_name);
                debug!("Columns: {:?}", columns);
                db_state.create_table(&get_name(table_name)?, columns, constraints)
            }
            Statement::Query(qbox) => {
                debug!("Processing SQL Query");
                let enabled_rules = Arc::new(Rules::default());
                let lp = Translator::from_sql(
                    qbox,
                    &db_state.catalog,
                    &enabled_rules,
                    &db_state.col_id_gen,
                )
                .unwrap();

                // println!("Optimize start time: {:?}", std::time::Instant::now());

                // inside here, see if any parts of the plan already exist (use hash)
                // we pass the optional query registrar to replace subplans (TODO)
                let pp = self
                    .optimizer
                    .optimize(&lp, Some(&db_state.query_registrar));

                debug!("Optimized plan: {:?}", pp);

                // TESTING - optimizer above will return subset stub for now if it exists
                //   so that we can see what running a physical subplan looks like

                self.run_physical_plan(pp, db_state)
            }
            Statement::Insert {
                table_name,
                columns,
                source,
                ..
            } => {
                debug!(
                    "Inserting table:{} columns: {:?} source: {:?}",
                    table_name, columns, source
                );
                let source = if let Some(source) = source {
                    source
                } else {
                    return Err(c_err("No source for insert"));
                };
                match source.body.as_ref() {
                    SetExpr::Values(values) if columns.is_empty() => {
                        // identify the table id and schema of the table via catalog
                        let table_name = get_name(table_name)?;
                        let table_id = db_state.catalog.get_table_id(&table_name);
                        let table_schema = db_state.catalog.get_table_schema(table_id).unwrap();
                        let count = self.executor.import_tuples(
                            values,
                            &table_name,
                            &table_id,
                            &table_schema,
                            self.active_txn.tid()?,
                        )?;
                        let qr = QueryResult::new_insert_result(count, table_name);
                        Ok(qr)
                    }
                    SetExpr::Values(_) => {
                        Err(c_err(
                            "Inserts with columns specified are not currently supported. Must supply values for the entire table",
                        ))
                    }
                    _ => {
                        Err(c_err(
                            "Inserts via query not currently supported. Must supply values",
                        ))
                    }
                }
            }
            _ => {
                unimplemented!()
            }
        }
    }

    pub fn import_csv(
        &mut self,
        table_name: &str,
        file_path: &Path,
        db_state: &'static DatabaseState,
    ) -> Result<QueryResult, CrustyError> {
        let table_id = db_state.catalog.get_table_id(table_name);
        let table_schema = db_state.catalog.get_table_schema(table_id).unwrap();
        let file = OpenOptions::new().read(true).open(file_path).unwrap();
        let mut csv_reader = CsvReader::new(file, &table_schema, b',', false).unwrap();
        let num_inserts = self
            .executor
            .import_records_from_reader(&mut csv_reader, &table_id, self.active_txn.tid()?)
            .unwrap();
        Ok(QueryResult::new_insert_result(
            num_inserts,
            table_name.to_string(),
        ))
    }
}

// pub struct Conductor {
//     pub parser: SQLParser,
//     pub optimizer: Optimizer,
//     pub executor: Executor,
//     pub active_txn: Transaction,
// }
//
// impl Conductor {
//     pub fn new(
//         parser: SQLParser,
//         optimizer: Optimizer,
//         executor: Executor,
//     ) -> Result<Self, CrustyError> {
//         let conductor = Conductor {
//             parser,
//             optimizer,
//             executor,
//             active_txn: Transaction::new(),
//         };
//         Ok(conductor)
//     }
//
//     /// Processes command entered by the user.
//     ///
//     /// # Arguments
//     ///
//     /// * `cmd` - Command to execute.
//     /// * `client_id` - id of client running command.
//     /// * `server_state` the shared ref to server
//     pub fn run_command(
//         &mut self,
//         command: commands::Command,
//         client_id: u64,
//         server_state: &'static ServerState,
//     ) -> Result<String, CrustyError> {
//         match command {
//             commands::Command::Create(name) => {
//                 info!("Processing COMMAND::Create {:?}", name);
//                 server_state.create_database(name)
//             }
//             commands::Command::Connect(name) => {
//                 // Check exists and load.
//                 // TODO: Figure out about using &str.
//                 info!("Processing COMMAND::Connect {:?}", name);
//                 server_state.connect_to_db(name, client_id)
//             }
//             commands::Command::Import(path_and_name) => {
//                 info!("Processing COMMAND::Import {:?}", path_and_name);
//                 // Get db id.
//                 let (table_name, new_path) = ServerState::parse_name_and_path(&path_and_name);
//                 let (table_id, table_schema) =
//                     self.get_table_id_and_schema(table_name, client_id, server_state)?;
//                 let file = OpenOptions::new().read(true).open(new_path).unwrap();
//                 let mut csv_reader = CsvReader::new(file, &table_schema, b',', false).unwrap();
//                 self.executor
//                     .import_records_from_reader(&mut csv_reader, &table_id, self.active_txn.tid()?)
//                     .unwrap();
//                 Ok(format!("Imported table {}", table_name))
//             }
//             commands::Command::RegisterQuery(name_and_plan_path) => {
//                 // Register a query (as a physical plan) to be executed at a later time (a stored procedure or view)
//                 info!("Processing COMMAND::RegisterQuery {:?}", name_and_plan_path);
//                 server_state.register_query(name_and_plan_path, client_id)
//             }
//             commands::Command::RunQueryFull(args) => {
//                 // Execute a query  that has been registered before
//                 info!("Processing COMMAND::RunQueryFull {:?}", args);
//
//                 // Parse arguments
//                 let mut tokens = args.split_whitespace();
//                 let possible_query_name = tokens.next();
//                 let possible_cache = tokens.next();
//                 let possible_timestamp = tokens.next();
//                 if tokens.next().is_some() {
//                     return Err(CrustyError::CrustyError("Too many arguments".to_string()));
//                 } else if possible_query_name.is_none()
//                     || possible_cache.is_none()
//                     || possible_timestamp.is_none()
//                 {
//                     return Err(CrustyError::CrustyError(format!(
//                         "Missing arguments \"{}\"",
//                         args
//                     )));
//                 }
//                 let query_name = possible_query_name.unwrap();
//                 let _cache: bool = match possible_cache.unwrap().parse() {
//                     Ok(v) => v,
//                     Err(e) => return Err(CrustyError::CrustyError(format!("Bad cache: {}", e))),
//                 };
//                 let timestamp: LogicalTimeStamp = match possible_timestamp.unwrap().parse() {
//                     Ok(ts) => ts,
//                     Err(e) => {
//                         return Err(CrustyError::CrustyError(format!("Bad timestamp: {}", e)))
//                     }
//                 };
//
//                 // Get db id.
//                 let db_id_ref = server_state.active_connections.read().unwrap();
//                 let db_state = match db_id_ref.get(&client_id) {
//                     Some(db_id) => {
//                         let db_ref = server_state.id_to_db.read().unwrap();
//                         *db_ref.get(db_id).unwrap()
//                     }
//                     None => {
//                         return Err(CrustyError::CrustyError(String::from(
//                             "No active DB or DB not found",
//                         )))
//                     }
//                 };
//
//                 // Get query plan.
//                 let query_plan =
//                     server_state.begin_query(query_name, None, timestamp, client_id)?;
//
//                 // Run query.
//                 self.run_query(query_plan, db_state, timestamp)?;
//
//                 // Update metadata after finishing query.
//                 server_state.finish_query(query_name, client_id)?;
//
//                 Ok(format!("Finished running query \"{}\"", query_name))
//             }
//             #[allow(unused_variables)]
//             commands::Command::RunQueryPartial(name_and_range) => todo!(),
//             commands::Command::ConvertQuery(args) => {
//                 // Convert a SQL statement/query into a physical plan. Used for registering queries.
//                 info!("Processing COMMAND::ConvertQuery {:?}", args);
//                 let mut tokens = args.split('|');
//                 let json_file_name = tokens.next();
//                 let sql = tokens.next();
//
//                 if json_file_name.is_none() || sql.is_none() {
//                     return Err(CrustyError::CrustyError(format!(
//                         "Missing arguments should be jsonfile|sql \"{}\"",
//                         args
//                     )));
//                 }
//                 info!("JSON {} SQL {}", json_file_name.unwrap(), sql.unwrap());
//                 let file_name: String = json_file_name.unwrap().split_whitespace().collect();
//
//                 if let ParserResponse::SQL(statements) =
//                     SQLParser::parse_sql(sql.unwrap().to_string())
//                 {
//                     if statements.len() != 1 {
//                         return Err(CrustyError::CrustyError(format!(
//                             "Can only store single SQL statement. Got {}",
//                             statements.len()
//                         )));
//                     }
//
//                     let db_id_ref = server_state.active_connections.read().unwrap();
//                     let db_state = match db_id_ref.get(&client_id) {
//                         Some(db_id) => {
//                             let db_ref = server_state.id_to_db.read().unwrap();
//                             *db_ref.get(db_id).unwrap()
//                         }
//                         None => {
//                             return Err(CrustyError::CrustyError(String::from(
//                                 "No active DB or DB not found",
//                             )))
//                         }
//                     };
//
//                     let statement = statements.get(0).unwrap();
//                     if let Statement::Query(query) = statement {
//                         let db = &db_state.database;
//                         debug!("Obtaining Logical Plan from query's AST");
//                         let logical_plan = TranslateAndValidate::from_sql(query, db)?;
//
//                         debug!("Converting this Logical Plan to a Physical Plan");
//                         let physical_plan = logical_plan_to_physical_plan(logical_plan, db)?;
//
//                         let plan_json = physical_plan.to_json();
//                         let x = serde_json::to_vec(&plan_json).unwrap();
//
//                         let file = OpenOptions::new()
//                             .write(true)
//                             .create_new(true)
//                             .open(file_name);
//                         match file {
//                             Ok(mut file) => match file.write(&x) {
//                                 Ok(_size) => Ok("ok".to_string()),
//                                 Err(e) => Err(CrustyError::IOError(format!("{:?}", e))),
//                             },
//                             Err(e) => Err(CrustyError::CrustyError(e.to_string())),
//                         }
//                     } else {
//                         Err(CrustyError::CrustyError(String::from(
//                             "SQL statement is not a query.",
//                         )))
//                     }
//                 } else {
//                     Err(CrustyError::CrustyError(String::from(
//                         "Can only store valid SQL statement.",
//                     )))
//                 }
//             }
//             commands::Command::ShowTables => {
//                 info!("Processing COMMAND::ShowTables");
//                 let db_id_ref = server_state.active_connections.read().unwrap();
//                 match db_id_ref.get(&client_id) {
//                     Some(db_id) => {
//                         let db_ref = server_state.id_to_db.read().unwrap();
//                         let db_state = db_ref.get(db_id).unwrap();
//
//                         let table_names = db_state.get_table_names()?;
//                         Ok(table_names)
//                     }
//                     None => Ok(String::from("No active DB or DB not found")),
//                 }
//             }
//             commands::Command::ShowQueries => {
//                 info!("Processing COMMAND::ShowQueries");
//                 let db_id_ref = server_state.active_connections.read().unwrap();
//                 match db_id_ref.get(&client_id) {
//                     Some(db_id) => {
//                         let db_ref = server_state.id_to_db.read().unwrap();
//                         let db_state = db_ref.get(db_id).unwrap();
//
//                         let registered_query_names = db_state.get_registered_query_names()?;
//                         Ok(registered_query_names)
//                     }
//                     None => Ok(String::from("No active DB or DB not found")),
//                 }
//             }
//             commands::Command::ShowDatabases => {
//                 info!("Processing COMMAND::ShowDatabases");
//                 let id_map = server_state.id_to_db.read();
//                 let mut names: Vec<String> = Vec::new();
//                 match id_map {
//                     Ok(map) => {
//                         for (id, db) in &*map {
//                             debug!(" id {}", id);
//                             names.push(db.name.clone());
//                         }
//                     }
//                     _ => panic!("Failed to get lock"),
//                 }
//                 if names.is_empty() {
//                     Ok(String::from("No databases found"))
//                 } else {
//                     Ok(names.join(","))
//                 }
//             }
//             commands::Command::Reset => {
//                 info!("Processing COMMAND::Reset");
//                 server_state.reset_database()?;
//                 Ok(String::from("Reset all of the database"))
//             }
//             commands::Command::Generate(args) => {
//                 info!("Processing COMMAND::Generate {:?}", args);
//                 // Parse arguments
//                 let mut tokens = args.split_whitespace();
//                 let possible_csv_filename = tokens.next();
//                 let possible_n = tokens.next();
//                 if tokens.next().is_some() {
//                     return Err(CrustyError::CrustyError("Too many arguments".to_string()));
//                 } else if possible_csv_filename.is_none() || possible_n.is_none() {
//                     return Err(CrustyError::CrustyError(format!(
//                         "Missing arguments \"{}\"",
//                         args
//                     )));
//                 }
//                 let csv_file_name = possible_csv_filename.unwrap();
//                 let n: u64 = match possible_n.unwrap().parse() {
//                     Ok(v) => v,
//                     Err(e) => return Err(CrustyError::CrustyError(format!("Bad n: {}", e))),
//                 };
//                 let tuples = testutil::gen_test_tuples(n);
//                 csv_utils::write_tuples_to_new_csv(csv_file_name.to_string(), tuples)
//             }
//             commands::Command::Test => {
//                 unimplemented!()
//                 /*
//                 let queue = server_state.task_queue.lock().unwrap();
//                 queue.send(Message::Test).unwrap();
//                 Ok(String::from("Test OK"))
//                 */
//             }
//             commands::Command::ExecuteSQL(_sql) => {
//                 panic!("Should never get here");
//             }
//             commands::Command::Shutdown => {
//                 panic!("Received a shutdown. Never should have made it this far.");
//             }
//             commands::Command::CloseConnection => {
//                 panic!("Received a close. Never should have made it this far.");
//             }
//             commands::Command::QuietMode => {
//                 panic!("should not get here with quiet mode");
//             }
//         }
//     }
//
//     pub fn run_sql_from_string(
//         &mut self,
//         sql: &str,
//         db_state: &'static DatabaseState,
//     ) -> Result<QueryResult, CrustyError> {
//         match SQLParser::parse_sql(sql.to_string()) {
//             ParserResponse::SQL(ast) => {
//                 info!("Processing SQL: {:?}", sql);
//                 self.run_sql(ast, db_state)
//             }
//             ParserResponse::SQLError(e) => Err(c_err(format!("SQL error: {}", e).as_str())),
//             ParserResponse::SQLConstraintError(msg) => {
//                 Err(c_err(format!("SQL constraint error: {}", msg).as_str()))
//             }
//             ParserResponse::Err => Err(c_err("Unknown error parsing SQL")),
//         }
//     }
//
//     pub fn to_logical_plan(
//         &self,
//         sql: &str,
//         db_state: &'static DatabaseState,
//     ) -> Result<LogicalPlan, CrustyError> {
//         match SQLParser::parse_sql(sql.to_string()) {
//             ParserResponse::SQL(ast) => {
//                 info!("Processing SQL: {:?}", sql);
//                 let db = &db_state.database;
//                 match ast.first().unwrap() {
//                     Statement::Query(qbox) => {
//                         debug!("Obtaining Logical Plan from query's AST");
//                         TranslateAndValidate::from_sql(qbox, db)
//                     }
//                     _ => Err(c_err("Not a query")),
//                 }
//             }
//             ParserResponse::SQLError(e) => Err(c_err(format!("SQL error: {}", e).as_str())),
//             ParserResponse::SQLConstraintError(msg) => {
//                 Err(c_err(format!("SQL constraint error: {}", msg).as_str()))
//             }
//             ParserResponse::Err => Err(c_err("Unknown error parsing SQL")),
//         }
//     }
//
//     pub fn to_physical_plan(
//         &self,
//         logical_plan: LogicalPlan,
//         db_state: &'static DatabaseState,
//     ) -> Result<PhysicalPlan, CrustyError> {
//         let db = &db_state.database;
//         logical_plan_to_physical_plan(logical_plan, db)
//     }
//
//     pub fn run_physical_plan(
//         &mut self,
//         physical_plan: PhysicalPlan,
//         db_state: &'static DatabaseState,
//     ) -> Result<QueryResult, CrustyError> {
//         let db = &db_state.database;
//         let op_iterator = physical_plan_to_op_iterator(
//             db_state.storage_manager,
//             db_state.transaction_manager,
//             db,
//             &physical_plan,
//             self.active_txn.tid()?,
//             db_state.get_current_time(),
//         )?;
//         // print_tree(&op_iterator.to_json(), 0);
//
//         // We populate the executor with the state: physical plan, and storage manager ref
//         self.executor.configure_query(op_iterator);
//
//         // Finally, execute the query
//         let res_bin = self.executor.execute();
//
//         match res_bin {
//             Ok(qr_bin) => Ok(qr_bin),
//             Err(e) => Err(e),
//         }
//     }
//
//     /// Runs SQL commands depending on the first statement, return the result in binary format.
//     ///
//     /// # Arguments
//     ///
//     /// * `cmd` - Tokenized command into statements.
//     /// * `id` - Thread id for lock management.
//     #[allow(unused_variables)]
//     fn run_sql(
//         &mut self,
//         cmd: Vec<Statement>,
//         db_state: &'static DatabaseState,
//     ) -> Result<QueryResult, CrustyError> {
//         if cmd.is_empty() {
//             Err(CrustyError::CrustyError(String::from("Empty SQL command")))
//         } else {
//             match cmd.first().unwrap() {
//                 Statement::CreateTable {
//                     name: table_name,
//                     columns,
//                     constraints,
//                     ..
//                 } => {
//                     info!("Processing CREATE table: {:?}", table_name);
//                     db_state.create_table(&get_name(table_name)?, columns, constraints)
//                 }
//                 Statement::Query(qbox) => {
//                     debug!("Processing SQL Query");
//                     let db = &db_state.database;
//
//                     debug!("Obtaining Logical Plan from query's AST");
//                     let logical_plan = TranslateAndValidate::from_sql(qbox, db)?;
//                     debug!("Converting this Logical Plan to a Physical Plan");
//                     let physical_plan = logical_plan_to_physical_plan(logical_plan, db)?;
//                     debug!("Physical plan {:?}", physical_plan);
//                     self.run_query(
//                         Arc::new(physical_plan),
//                         db_state,
//                         db_state.get_current_time(),
//                     )
//                 }
//                 Statement::Insert {
//                     table_name,
//                     columns,
//                     source,
//                     ..
//                 } => {
//                     debug!(
//                         "Inserting table:{} columns: {:?} source: {:?}",
//                         table_name, columns, source
//                     );
//                     if let SetExpr::Values(values) = &source.as_ref().body {
//                         if !columns.is_empty() {
//                             Err(CrustyError::CrustyError(String::from(
//                                 "Inserts with columns specified are not currently supported. Must supply values for the entire table",
//                             )))
//                         } else {
//                             let (table_id, extracted_table_name, table_schema) =
//                                 self.get_table_id_name_and_schema(table_name, db_state)?;
//                             let insert_count = self.executor.import_tuples(
//                                 values,
//                                 &extracted_table_name,
//                                 &table_id,
//                                 &table_schema,
//                                 self.active_txn.tid()?,
//                             )?;
//
//                             let qr =
//                                 QueryResult::new_insert_result(insert_count, extracted_table_name);
//                             Ok(qr)
//                         }
//                     } else {
//                         Err(CrustyError::CrustyError(String::from(
//                             "Inserts via query not currently supported. Must supply values",
//                         )))
//                     }
//                 }
//                 Statement::Delete {
//                     table_name,
//                     selection,
//                 } => {
//                     debug!("Deleting table:{} selection: {:?}", table_name, selection);
//                     Err(CrustyError::CrustyError(String::from(
//                         "Delete not currently supported",
//                     )))
//                 }
//                 Statement::Truncate {
//                     table_name,
//                     partitions,
//                 } => {
//                     debug!(
//                         "Truncating table:{} partitions: {:?}",
//                         table_name, partitions
//                     );
//                     Err(CrustyError::CrustyError(String::from(
//                         "Binary truncate not currently supported",
//                     )))
//                 }
//                 Statement::Drop {
//                     object_type,
//                     if_exists,
//                     names,
//                     cascade,
//                     purge,
//                 } => {
//                     unimplemented!()
//                 }
//                 Statement::Update {
//                     table_name,
//                     assignments,
//                     selection,
//                 } => {
//                     unimplemented!();
//                     /*
//                     debug!(
//                         "Updating table:{} \n\nassignments: {:?} selection: {:?}",
//                         table_name, assignments, selection
//                     );
//                     let (table_id, extracted_table_name, table_schema) =
//                         self.get_table_id_name_and_schema(table_name, db_state)?;
//                     let db = &db_state.database;
//                     let logical_plan = TranslateAndValidate::from_update(
//                         table_id,
//                         &extracted_table_name,
//                         assignments,
//                         selection,
//                         db,
//                     )?;
//                     let physical_plan =
//                         self.optimizer
//                             .logical_plan_to_physical_plan(logical_plan, db, false)?;
//                     debug!("Physical plan {:?}", physical_plan);
//                     self.run_query(
//                         Arc::new(physical_plan),
//                         db_state,
//                         db_state.get_current_time(),
//                     )
//                     */
//                 }
//                 Statement::StartTransaction { modes } => {
//                     unimplemented!()
//                 }
//                 Statement::Commit { chain } => {
//                     unimplemented!();
//                 }
//                 Statement::Rollback { chain } => {
//                     unimplemented!()
//                 }
//                 Statement::CreateIndex {
//                     name,
//                     table_name,
//                     columns,
//                     unique,
//                     if_not_exists,
//                 } => {
//                     unimplemented!()
//                 }
//                 Statement::CreateView {
//                     or_replace,
//                     materialized,
//                     name,
//                     columns,
//                     query,
//                     with_options,
//                 } => {
//                     unimplemented!()
//                 }
//                 _ => {
//                     unimplemented!()
//                 }
//             }
//         }
//     }
//
//     /// Runs a given query, return the result in binary format.
//     ///
//     /// # Arguments
//     ///
//     /// * `query` - Query to run.
//     /// * `id` - Thread id for lock management.
//     fn run_query(
//         &mut self,
//         physical_plan: Arc<PhysicalPlan>,
//         db_state: &'static DatabaseState,
//         timestamp: LogicalTimeStamp,
//     ) -> Result<QueryResult, CrustyError> {
//         let db = &db_state.database;
//
//         // Start transaction
//         let txn = Transaction::new();
//
//         debug!("Configuring Storage Manager");
//         let op_iterator = physical_plan_to_op_iterator(
//             db_state.storage_manager,
//             db_state.transaction_manager,
//             db,
//             &physical_plan,
//             txn.tid()?,
//             timestamp,
//         )?;
//         // print_tree(&op_iterator.to_json(), 0);
//
//         // We populate the executor with the state: physical plan, and storage manager ref
//         debug!("Configuring Physical Plan");
//         self.executor.configure_query(op_iterator);
//
//         // Finally, execute the query
//         debug!("Executing query");
//         let res_bin = self.executor.execute();
//
//         match res_bin {
//             Ok(qr_bin) => Ok(qr_bin),
//             Err(e) => Err(e),
//         }
//     }
//
//     /// Utility to get a id, name and schema copy for table_name for a given client
//     fn get_table_id_name_and_schema(
//         &self,
//         table_name: &ObjectName,
//         db_state: &'static DatabaseState,
//     ) -> Result<(ContainerId, String, TableSchema), CrustyError> {
//         if table_name.0.len() != 1 {
//             return Err(CrustyError::CrustyError(
//                 "Insert statement only supports unqualified table names".to_owned(),
//             ));
//         }
//         let extracted_table_name = &table_name.0.get(0).unwrap().value;
//         let table_id = db_state
//             .database
//             .get_table_id(extracted_table_name)
//             .ok_or_else(|| {
//                 CrustyError::CrustyError(format!(
//                     "Import cannot find table id for table {}",
//                     table_name
//                 ))
//             })?;
//         let table_schema = db_state.database.get_table_schema(table_id)?;
//         Ok((table_id, extracted_table_name.to_owned(), table_schema))
//     }
//
//     /// Utility to get a id and schema copy for table_id for a given client
//     fn get_table_id_and_schema(
//         &self,
//         table_name: &str,
//         client_id: u64,
//         server_state: &'static ServerState,
//     ) -> Result<(ContainerId, TableSchema), CrustyError> {
//         let db_id_ref = server_state.active_connections.read().unwrap();
//         let db_state = match db_id_ref.get(&client_id) {
//             Some(db_id) => {
//                 let db_ref = server_state.id_to_db.read().unwrap();
//                 *db_ref.get(db_id).unwrap()
//             }
//             None => {
//                 return Err(CrustyError::CrustyError(String::from(
//                     "No active DB or DB not found",
//                 )))
//             }
//         };
//
//         let table_id = db_state.database.get_table_id(table_name).ok_or_else(|| {
//             CrustyError::CrustyError(format!(
//                 "Import cannot find table id for table {}",
//                 table_name
//             ))
//         })?;
//         let table_schema = db_state.database.get_table_schema(table_id)?;
//         Ok((table_id, table_schema))
//     }
// }
//
