use crate::conductor::Conductor;
use crate::database_state::DatabaseState;
use crate::server_state::ServerState;

use common::commands::{self, Command, CommandWithArgs, DBCommand, Response, SystemCommand};

use common::error::c_err;
use common::QUERY_CACHES_DIR_NAME;
use common::{ids::TransactionId, CrustyError, QueryResult};
use std::fs::{self, File};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::atomic::AtomicBool;
use std::sync::Arc;

pub fn handle_command(
    shutdown_signal: Arc<AtomicBool>,
    quiet_mode: &mut bool,
    command: CommandWithArgs,
    server_state: &'static ServerState,
    client_id: u64,
) -> (bool, Response) {
    info!("Handling command: {:?}", command);

    // if the server has shut down, then send a shutdown response
    if shutdown_signal.load(std::sync::atomic::Ordering::Acquire) {
        return (true, Response::Shutdown(false));
    }

    match command.command {
        // Should this have a TID?
        Command::System(system_command) => handle_system_command(
            shutdown_signal,
            quiet_mode,
            client_id,
            server_state,
            system_command,
            &command.args,
        ),
        Command::DB(database_command) => {
            if let Ok(db) = server_state.get_connected_db(client_id) {
                let tid = db.get_or_assign_tid(client_id);
                handle_database_command(db, database_command, &command.args, tid, client_id)
            } else {
                error!("Client {} is not connected to a database", client_id);
                (
                    false,
                    Response::SystemErr("Not connected to a database".to_string()),
                )
            }
        }
    }
}

pub fn handle_system_command(
    shutdown_signal: Arc<AtomicBool>,
    quiet_mode: &mut bool,
    client_id: u64,
    server_state: &'static ServerState,
    system_command: SystemCommand,
    command_args: &[String],
) -> (bool, Response) {
    let response = run_system_command(
        shutdown_signal,
        quiet_mode,
        client_id,
        server_state,
        system_command,
        command_args,
    );

    match response {
        Ok(response) => response,
        Err(e) => (false, Response::SystemErr(e.to_string())),
    }
}

fn run_system_command(
    shutdown_signal: Arc<AtomicBool>,
    quiet_mode: &mut bool,
    client_id: u64,
    server_state: &'static ServerState,
    system_command: SystemCommand,
    command_args: &[String],
) -> Result<(bool, Response), CrustyError> {
    match system_command {
        SystemCommand::ShowDatabases => {
            let db_names = server_state.get_db_names();
            let response = Response::SystemMsg(format!("Databases: {}", db_names.join(", ")));
            Ok((false, response))
        }
        SystemCommand::Reset => {
            server_state.reset()?;
            let response = Response::SystemMsg("Reset server.".to_string());
            Ok((false, response))
        }
        SystemCommand::Shutdown => {
            shutdown_signal.store(true, std::sync::atomic::Ordering::Release);
            server_state.shutdown()?;
            let response = Response::Shutdown(true);
            Ok((true, response))
        }
        SystemCommand::QuietMode => {
            *quiet_mode = true;
            Ok((false, Response::QuietOk))
        }
        SystemCommand::Test => {
            unimplemented!()
        }
        SystemCommand::Create => {
            let db_name = command_args.first().expect("Database name not provided");
            server_state.create_new_db(db_name)?;
            let response = Response::SystemMsg(format!("Created database {}", db_name));
            Ok((false, response))
        }
        SystemCommand::Connect => {
            let db_name = command_args.first().expect("Database name not provided");
            server_state.connect_to_db(db_name, client_id)?;
            let response = Response::SystemMsg(format!("Connected to database {}", db_name));
            Ok((false, response))
        }
        SystemCommand::CloseConnection => {
            server_state.close_connection(client_id);
            let response = Response::SystemMsg("Closing connection".to_string());
            Ok((false, response))
        }
        SystemCommand::Help => {
            let response = Response::SystemMsg(commands::gen_help_string());
            Ok((false, response))
        }
    }
}

pub fn handle_database_command(
    db: &'static DatabaseState,
    database_command: DBCommand,
    command_args: &[String],
    tid: TransactionId,
    client_id: u64,
) -> (bool, Response) {
    match run_database_command(db, database_command, command_args, tid, client_id) {
        Ok(response) => response,
        Err(e) => (false, Response::QueryExecutionError(e.to_string())),
    }
}

pub fn run_database_command(
    db: &'static DatabaseState,
    database_command: DBCommand,
    command_args: &[String],
    tid: TransactionId,
    client_id: u64,
) -> Result<(bool, Response), CrustyError> {
    match database_command {
        DBCommand::ExecuteSQL => {
            let sql = command_args.first().expect("SQL not provided").to_string();
            let mut conductor = Conductor::new_from_tid(db.managers, tid)?;
            let qr = if let Some(query_result) = db.query_result_from_sql(&sql)? {
                info!("Fetched registered query result");
                query_result
            } else {
                conductor.run_sql_from_string(sql, db)?
            };

            // HACK: until committing is properly implemented, we will manually increment the working tid so that query
            // execution is isolated into one txn (i.e. every user command is one transaction).
            // ask Kathir ab this
            let _new_tid = db.assign_new_tid(client_id);

            Ok((false, Response::QueryResult(qr)))
        }
        DBCommand::ShowTables => {
            let tables = db.get_table_names()?;
            let result = QueryResult::MessageOnly(format!("Tables: {}", tables.join(", ")));
            Ok((false, Response::QueryResult(result)))
        }
        DBCommand::Import => {
            let table_name = command_args.get(1).expect("table_name not provided");
            let file_path_str = command_args.first().expect("file_path not provided");
            let file_path = Path::new(file_path_str);

            let mut conductor = Conductor::new_from_tid(db.managers, tid)?;
            let qr = conductor.import_csv(table_name, file_path, db)?;

            // HACK: until committing is properly implemented, we will manually increment the working tid so that insertion is isolated into one txn
            // ask Kathir ab this
            let _new_tid = db.assign_new_tid(client_id);

            if let QueryResult::Insert {
                inserted,
                table_name,
            } = qr
            {
                let response = Response::SystemMsg(format!(
                    "Imported {} rows into table {}",
                    inserted, table_name
                ));
                Ok((false, response))
            } else {
                Err(c_err("Unexpected query result from import"))
            }
        }
        DBCommand::ShowQueries => {
            unimplemented!()
        }
        DBCommand::RegisterQuery => {
            let query_name = command_args.first().expect("Query name not provided");
            let query = command_args.get(1).expect("Query not provided");
            let mut conductor = Conductor::new_from_tid(db.managers, tid)?;

            let maybe_cached = db.query_result_from_sql(query)?;
            let qr = match maybe_cached {
                Some(_) => {
                    return Ok((
                        false,
                        Response::SystemMsg(
                            "Identical query (matching SQL) has already been registered"
                                .to_string(),
                        ),
                    ));
                }
                None => conductor.run_sql_from_string(query.clone(), db)?,
            };

            let lp = conductor.to_logical_plan(query, db)?;
            let pp = conductor.to_physical_plan(lp, db)?;

            // define path for query result storage - .../query_caches/[db.id]/[query_name]_query_result.json
            let qr_filename = format!("[{}]_query_result.json", query_name);
            let mut file_path = PathBuf::new();
            file_path.push(&db.managers.config.db_path);
            file_path.push(QUERY_CACHES_DIR_NAME);
            file_path.push(db.id.to_string());
            file_path.push(qr_filename);
            fs::create_dir_all(file_path.parent().unwrap())
                .expect("couldn't create parent directory for query result file");
            let file_path = file_path.to_str().unwrap().to_string();

            match File::create_new(&file_path) {
                Ok(mut f) => {
                    db.register_query_with_result(
                        query_name.clone(),
                        query.clone(),
                        "".to_string(),
                        pp.into(),
                        file_path,
                        tid, // should be equal to conductor.active_txn.tid()? now
                    )?;
                    let json = serde_json::to_string(&qr).unwrap();
                    f.write_all(json.as_bytes())?;
                }
                _ => {
                    debug!("File already exists");
                }
            };

            // HACK: until committing is properly implemented, we will manually increment the working tid so that query registering is isolated into one txn and purging doesn't automatically happen.
            // ask Kathir ab this
            let _new_tid = db.assign_new_tid(client_id);

            Ok((false, Response::QueryResult(qr)))
        }
        DBCommand::Generate => {
            let _source = command_args.first().expect("Source not provided");
            let _target = command_args.get(1).expect("Target not provided");
            unimplemented!()
        }
        DBCommand::ConvertQuery => {
            let _target = command_args.first().expect("Target not provided");
            let _query = command_args.get(1).expect("Query not provided");
            unimplemented!()
        }
        DBCommand::RunQueryFull => {
            let _query_name = command_args.first().expect("Query name not provided");
            unimplemented!()
        }
        DBCommand::RunQueryPartial => {
            let _query_name = command_args.first().expect("Query name not provided");

            unimplemented!()
        }
        DBCommand::Commit => {
            // db.managers.tm.commit_txn(tid)?; xtx not implemented
            let new_tid = db.assign_new_tid(client_id);
            let response = Response::SystemMsg(format!(
                "Committed old TID {tid:?}. New TID is {new_tid:?}."
            ));
            Ok((false, response))
        }
    }
}
