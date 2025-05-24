use clap::Parser;
use cli_crusty::{connect_to_kill_server, Client, Response};
use common::error::c_err;
use common::physical::config::{ClientConfig, ServerConfig};
use common::CrustyError;
use log::{error, info};
use server::Server;
use std::fs::{self, File};
use std::io;
use std::io::Cursor;
use std::io::{BufRead, BufReader};
use std::path::{Path, PathBuf};
use std::thread;
use utilities::csv_compare;

/// SQL Logic Test Configuration
#[derive(Parser, Debug)]
#[clap(name = "sql_logic_test")]
pub struct SQLLogicTestConfig {
    /// Directory containing test files
    #[clap(long, default_value = "testdata")]
    pub test_file_dir: PathBuf,

    /// Directory of the server
    #[clap(long, default_value = "server")]
    pub server_dir: PathBuf,

    /// Optional path for the server log file
    #[clap(long)]
    pub server_log_file: Option<PathBuf>,

    /// Optional path for the client log file
    #[clap(long)]
    pub client_log_file: Option<PathBuf>,

    /// List of files to exclude from testing
    #[clap(long, use_value_delimiter = true)]
    pub excluded_files: Vec<String>,
}

pub enum TestExpectation {
    Ok,
    Err,
    ResultMatch(bool, PathBuf),
}

impl std::fmt::Display for TestExpectation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TestExpectation::Ok => write!(f, "Ok"),
            TestExpectation::Err => write!(f, "Err"),
            TestExpectation::ResultMatch(is_ordered, path) => {
                write!(f, "ResultMatch({}, {:?})", is_ordered, path)
            }
        }
    }
}

pub struct SQLLogicTestEntry {
    pub expectation: TestExpectation,
    pub command: String, // Assuming `command` is a simple string. Replace with `Command` if it's a custom type.
}

impl std::fmt::Display for SQLLogicTestEntry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Command: {}, Expectation: {}",
            self.command, self.expectation
        )
    }
}

impl SQLLogicTestEntry {
    pub fn run(&self, client: &mut Client) -> Result<(), CrustyError> {
        let command = BufReader::new(self.command.as_bytes());
        let response = client.send_requests_from_buffer(command)?;
        assert_eq!(response.len(), 1);
        let response = &response[0];
        let success = match &self.expectation {
            TestExpectation::Ok => {
                matches!(
                    response,
                    Response::Ok
                        | Response::SystemMsg(_)
                        | Response::QuietOk
                        | Response::QueryResult(_)
                )
            }
            TestExpectation::Err => {
                matches!(
                    response,
                    Response::SystemErr(_) | Response::QuietErr | Response::QueryExecutionError(_)
                )
            }
            TestExpectation::ResultMatch(is_ordered, file_path) => {
                if let Response::QueryResult(qr) = response {
                    let expected = csv::ReaderBuilder::new()
                        .has_headers(false)
                        .from_path(file_path)
                        .map_err(|e| {
                            CrustyError::CrustyError(format!("Failed to read csv: {}", e))
                        })?;

                    let result = qr
                        .get_tuples()
                        .ok_or(c_err("Response is not a result of a select query"))?;
                    let mut csv_str = Cursor::new(Vec::new());
                    let mut csv_writer = csv::WriterBuilder::new()
                        .has_headers(false)
                        .from_writer(&mut csv_str);

                    for record in result {
                        let record_vec: Vec<_> =
                            record.field_vals().map(ToString::to_string).collect();
                        csv_writer.write_record(&record_vec).map_err(|e| {
                            CrustyError::CrustyError(format!("Failed to write csv: {}", e))
                        })?;
                    }

                    csv_writer.flush()?;
                    drop(csv_writer);

                    let result_reader = csv::ReaderBuilder::new()
                        .has_headers(false)
                        .from_reader(csv_str.get_ref().as_slice());

                    if *is_ordered {
                        csv_compare::csvs_equal_ordered(expected, result_reader, None).map_err(
                            |e| {
                                CrustyError::CrustyError(format!(
                                    "Ordered comparison failed: {}",
                                    e
                                ))
                            },
                        )?
                    } else {
                        csv_compare::csvs_equal_unordered(expected, result_reader, None).map_err(
                            |e| {
                                CrustyError::CrustyError(format!(
                                    "Unordered comparison failed: {}",
                                    e
                                ))
                            },
                        )?
                    }
                } else {
                    Err(CrustyError::CrustyError(format!(
                        "Expected a query result, got {:?}",
                        response
                    )))?
                }
            }
        };
        if success {
            Ok(())
        } else {
            Err(CrustyError::CrustyError(format!(
                "Test failed, expected {}, got {:?}",
                &self.expectation, response
            )))
        }
    }
}

pub fn start_server(config: &'static ServerConfig) -> thread::JoinHandle<()> {
    thread::spawn(move || {
        let mut server = Server::new(config);
        server.run_server();
    })
}

pub fn is_excluded(file_name: &str) -> bool {
    let excluded_files = [
        // "filter1",
        // "filter2",
        // "table_creation",
        // "agg2",
        // "join1",
        // "agg1",
        // "match",
    ];
    excluded_files.iter().any(|f: &&str| f == &file_name)
}

pub fn get_test_files(test_file_dir: &Path) -> Vec<PathBuf> {
    let mut test_files = Vec::new();
    for file in fs::read_dir(test_file_dir).unwrap() {
        // if file is not a directory, run the test
        let file = file.expect("Error reading directory");
        if file.file_type().unwrap().is_dir() {
            continue;
        }
        let file_name = file.file_name().into_string().unwrap();
        if is_excluded(&file_name) {
            continue;
        }
        let test_file_path = Path::new(test_file_dir).join(&file_name);
        test_files.push(test_file_path);
    }
    test_files
}

fn parse_expectation(line: &str) -> io::Result<TestExpectation> {
    let parts: Vec<&str> = line.split_whitespace().collect();

    match parts.first() {
        Some(&"statement") => {
            if let Some(&"ok") = parts.get(1) {
                Ok(TestExpectation::Ok)
            } else if let Some(&"err") = parts.get(1) {
                Ok(TestExpectation::Err)
            } else {
                Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "Invalid expectation type",
                ))
            }
        }
        Some(&"match") | Some(&"omatch") => {
            let path_str = parts
                .get(1)
                .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "Missing path"))?;
            let path = PathBuf::from(path_str);

            if path.extension().and_then(|ext| ext.to_str()) != Some("csv") {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "Path is not a CSV file",
                ));
            }

            let is_ordered = parts[0] == "omatch";
            Ok(TestExpectation::ResultMatch(is_ordered, path))
        }
        _ => Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "Invalid expectation type",
        )),
    }
}

fn parse_file(file_path: &PathBuf) -> io::Result<Vec<SQLLogicTestEntry>> {
    let file = File::open(file_path)?;
    let lines = io::BufReader::new(file).lines();

    // filter out empty lines and comments
    let mut lines = lines.filter(|line| {
        if let Ok(line) = line {
            !line.is_empty() && !line.starts_with('#')
        } else {
            false
        }
    });

    let mut entries = Vec::new();

    // loop over every two lines
    while let Some(expectation_str) = lines.next() {
        let expectation = parse_expectation(&expectation_str?)?;
        if let Some(command) = lines.next() {
            entries.push(SQLLogicTestEntry {
                expectation,
                command: command?,
            });
        } else {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "Missing command",
            ));
        }
    }

    Ok(entries)
}

pub fn run_sqllogictests_from_files(config: SQLLogicTestConfig) -> (Vec<String>, Vec<String>) {
    let mut succeeded = Vec::new();
    let mut failed = Vec::new();

    // create a new server in a different thread
    let server_config = Box::leak(Box::new(ServerConfig::temporary()));
    let server = start_server(server_config);

    // wait for the server to start
    let duration = std::time::Duration::from_secs(1);
    info!("Waiting {}ms for server to start...", duration.as_millis());
    std::thread::sleep(duration);

    let client_config = ClientConfig::default();
    let mut client = Client::new(client_config);

    for file_path in get_test_files(&config.test_file_dir) {
        // First create a test database and connect to it
        let response = client
            .send_requests_from_buffer(Cursor::new(b"\\r testdb"))
            .unwrap();
        assert!(response[0].is_ok());
        let response = client
            .send_requests_from_buffer(Cursor::new(b"\\c testdb"))
            .unwrap();
        assert!(response[0].is_ok());

        let file_name = file_path.file_name().unwrap().to_str().unwrap();
        println!(
            "================ Running test in file: {:?} ================",
            file_name
        );

        // parse the files and check
        let tests = parse_file(&file_path).unwrap();
        let mut error = None;

        // if all the tests in the file pass, then the file passes
        for test_entry in tests {
            println!("test: {}", test_entry);
            if let Err(e) = test_entry.run(&mut client) {
                error!("Error running test: {:?}", e);
                error = Some(e);
                break;
            } else {
                info!("Test passed");
            }
        }

        let response = client
            .send_requests_from_buffer(Cursor::new(b"\\reset"))
            .unwrap();
        assert!(response[0].is_ok());

        if let Some(e) = error {
            println!(
                "================ Error running test in file : {:?}, error: {:?} ================",
                file_name, e
            );
            failed.push(file_name.to_string());
        } else {
            println!(
                "================ Test {} Finished ================",
                file_name
            );
            succeeded.push(file_name.to_string());
        }
    }

    // shutdown the server
    let response = client
        .send_requests_from_buffer(Cursor::new(b"\\shutdown\n"))
        .unwrap();
    assert_eq!(response, vec![Response::Shutdown(true)]);

    info!("Shutting down client...");
    drop(client);
    info!("Client shutdown successfully");

    info!("Waiting for server to shutdown...");
    // Hack: connect a new client to kill the server.
    // Server is blocking on tcp_listener.incoming()
    // so we need to connect to it to unblock it
    // and make it check the shutdown flag.
    connect_to_kill_server(&ClientConfig::default()).unwrap();
    server.join().unwrap();
    info!("Server shutdown successfully");

    (succeeded, failed)
}

/*
#[cfg(test)]
mod test {
    use super::*;
    #[test]
    fn test_parse_sqllogictest() {
        // tests from cockroach
        let test_str1 = "statement ok\nCREATE TABLE kv (k INT PRIMARY KEY, v INT, w INT, s STRING)";
        let expected1 = SQLLogicTest {
            expectation: SQLLogicTestResult::Ok,
            command: Command::ExecuteSQL(String::from(
                "CREATE TABLE kv (k INT PRIMARY KEY, v INT, w INT, s STRING)",
            )),
        };
        assert_eq!(parse_sqllogictest(test_str1).unwrap(), expected1);

        let test_str2 = "statement err\nCREATE TABLE kv (k INT, v INT, w INT, s STRING)";
        let expected2 = SQLLogicTest {
            expectation: SQLLogicTestResult::Err,
            command: Command::ExecuteSQL(String::from(
                "CREATE TABLE kv (k INT, v INT, w INT, s STRING)",
            )),
        };
        assert_eq!(parse_sqllogictest(test_str2).unwrap(), expected2);
        /*
        let test_str3 = "query\nSELECT min(1), max(1), count(1) FROM kv----NULL NULL 0";
        let expected3 = SQLLogicTest {
            expectation: SQLLogicTestResult::Query(QueryResult::new("")),
            command: Command::ExecuteSQL(String::from("SELECT min(1), max(1), count(1) FROM kv")),
        };
        assert_eq!(parse_test(test_str3).unwrap(), expected3);
        */

        // tests using commands
        let test_str4 = "statement ok\n\\dq";
        let expected4 = SQLLogicTest {
            expectation: SQLLogicTestResult::Ok,
            command: Command::ShowQueries,
        };
        assert_eq!(parse_sqllogictest(test_str4).unwrap(), expected4);

        let test_str5 = "statement ok\n\\convert data.csv | select * from test";
        let expected5 = SQLLogicTest {
            expectation: SQLLogicTestResult::Ok,
            command: Command::ConvertQuery(String::from(" data.csv | select * from test")),
        };
        assert_eq!(parse_sqllogictest(test_str5).unwrap(), expected5);

        // tests that should fail
        let test_str6 = "statement ok\n\\run";
        assert!(parse_sqllogictest(test_str6).is_err());

        /*
        let test_str7 = "query\nselect * from test";
        assert!(parse_test(test_str7).is_err());


        let test_str8 = "statement ok\n\\dt----NULL NULL 0";
        assert!(parse_test(test_str8).is_err());
        */
    }

    #[test]
    fn test_parse_filter() {
        let contents = r#"statement ok
        create table test (a int primary key, b int)

        statement ok
        \i csv/data.csv test

        match csv/filter1.csv
        select * from test where test.a = 1

        statement ok
        \reset
        "#;
        let parsed = parse_sqllogictests(contents);
        assert!(parsed.is_ok());
    }

    #[test]
    fn test_omatch_sqllogic() {
        let test_str = "statement ok\nCREATE TABLE test (a int primary key, b int)\n\nstatement ok\n\\i csv/data.csv test\n\nomatch csv/data.csv\nselect * from test";
        let expected = vec![
            SQLLogicTest {
                expectation: SQLLogicTestResult::Ok,
                command: Command::ExecuteSQL(String::from(
                    "CREATE TABLE test (a int primary key, b int)",
                )),
            },
            SQLLogicTest {
                expectation: SQLLogicTestResult::Ok,
                command: Command::Import(String::from("csv/data.csv test")),
            },
            SQLLogicTest {
                expectation: SQLLogicTestResult::ResultMatch(true, PathBuf::from("csv/data.csv")),
                command: Command::ExecuteSQL(String::from("select * from test")),
            },
        ];
        assert_eq!(parse_sqllogictests(test_str).unwrap(), expected)
    }

    #[test]
    fn test_parse_sqllogictests() {
        let tests_str = "statement err\nCREATE TABLE kv (k INT, v INT, w INT, s STRING)\nstatement ok\nCREATE TABLE kv (k INT PRIMARY KEY, v INT, w INT, s STRING)\n# Aggregate functions return NULL if there are no rows.\n";
        let expected = vec![
            SQLLogicTest {
                expectation: SQLLogicTestResult::Err,
                command: Command::ExecuteSQL(String::from(
                    "CREATE TABLE kv (k INT, v INT, w INT, s STRING)",
                )),
            },
            SQLLogicTest {
                expectation: SQLLogicTestResult::Ok,
                command: Command::ExecuteSQL(String::from(
                    "CREATE TABLE kv (k INT PRIMARY KEY, v INT, w INT, s STRING)",
                )),
            },
        ];
        assert_eq!(parse_sqllogictests(tests_str).unwrap(), expected)
    }
    /*
    #[test]
    fn test_run_tests() {
        let test1 = SQLLogicTest {
            expectation: SQLLogicTestResult::Ok,
            command: Command::ExecuteSQL(String::from("CREATE TABLE test (a INT, b INT)")),
        };
        let test2 = SQLLogicTest {
            expectation: SQLLogicTestResult::Err,
            command: Command::ExecuteSQL(String::from("CREATE TABLE test (a INT, b INT)")),
        };
        let test3 = SQLLogicTest {
            expectation: SQLLogicTestResult::Ok,
            command: Command::ExecuteSQL(String::from(
                "CREATE TABLE test (a INT primary key, b INT)",
            )),
        };
        let test4 = SQLLogicTest {
            expectation: SQLLogicTestResult::Err,
            command: Command::ExecuteSQL(String::from(
                "CREATE TABLE test (a INT primary key, b INT)",
            )),
        };

        run_tests(vec![test2.clone(), test3.clone(), test4.clone()]).unwrap();
        assert!(run_tests(vec![test1.clone()]).is_err());
        assert!(run_tests(vec![test3.clone(), test3.clone()]).is_err());
        assert!(run_tests(vec![test4.clone()]).is_err());
    }
    */
}
*/
