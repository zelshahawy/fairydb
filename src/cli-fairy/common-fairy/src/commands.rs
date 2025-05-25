/// CommandTuple is a tuple that contains the command string, short name number of arguments,
/// the Command variant, and a description of the command.
type CommandTuple = (&'static str, u8, Command, &'static str);

/// The list of all possible commands that the server can receive.
/// Any new command must be added here and have the responding variant added to the Command enum.
const COMMANDS: [CommandTuple; 19] = [
    // System commands
    (
        "h",
        0,
        Command::System(SystemCommand::Help),
        "Show this help message",
    ),
    (
        "r",
        1,
        Command::System(SystemCommand::Create),
        "Create a new database",
    ),
    (
        "c",
        1,
        Command::System(SystemCommand::Connect),
        "Connect to a database",
    ),
    (
        "reset",
        0,
        Command::System(SystemCommand::Reset),
        "Reset the server state (clears everything)",
    ),
    (
        "shutdown",
        0,
        Command::System(SystemCommand::Shutdown),
        "Shuts down the server",
    ),
    (
        "close",
        0,
        Command::System(SystemCommand::CloseConnection),
        "Disconnect from the current database",
    ),
    (
        "quiet",
        0,
        Command::System(SystemCommand::QuietMode),
        "Sets the server to quiet mode (for benchmarking)",
    ),
    (
        "l",
        0,
        Command::System(SystemCommand::ShowDatabases),
        "Show the current databases",
    ),
    (
        "t",
        0,
        Command::System(SystemCommand::Test),
        "A no-op command for testing",
    ),
    // Database commands
    (
        "sql",
        1,
        Command::DB(DBCommand::ExecuteSQL),
        "Execute a SQL command (no need for '\\sql')",
    ),
    (
        "dt",
        0,
        Command::DB(DBCommand::ShowTables),
        "Show all tables in the current database",
    ),
    (
        "dq",
        0,
        Command::DB(DBCommand::ShowQueries),
        "Show all registered queries in the current database",
    ),
    (
        "register",
        2,
        Command::DB(DBCommand::RegisterQuery),
        "Register a query for future use (name, query)",
    ),
    (
        "runFull",
        1,
        Command::DB(DBCommand::RunQueryFull),
        "Run a registered query to a specific timestamp",
    ),
    (
        "runPartial",
        1,
        Command::DB(DBCommand::RunQueryPartial),
        "Run a registered query for the diffs of a timestamp range",
    ),
    (
        "convert",
        2,
        Command::DB(DBCommand::ConvertQuery),
        "Convert a SQL query to a JSON plan (path, query)",
    ),
    (
        "generate",
        2,
        Command::DB(DBCommand::Generate),
        "Generates a CSV file from a specified source (source target)",
    ),
    (
        "i",
        2,
        Command::DB(DBCommand::Import),
        "Import a CSV file into a specified table (path, table name)",
    ),
    (
        "commit",
        0,
        Command::DB(DBCommand::Commit),
        "Commits the current transaction",
    ),
];

/// Enum for system commands related to server state.
/// If the command needs a string that follows the command, the second field is the string.
#[derive(Debug, PartialEq, Eq, Serialize, Deserialize, Clone)]
pub enum SystemCommand {
    /// Create a new database.
    Create,
    /// Connect to an existing database.
    Connect,
    /// Resets the server state
    Reset,
    /// Shuts down the server.
    Shutdown,
    /// Closes the current database connection.
    CloseConnection,
    /// Sets the server to quiet mode.
    QuietMode,
    /// Lists all databases in the system.
    ShowDatabases,
    /// Test or diagnostic command.
    Test,
    /// Help command.
    Help,
}

// impl std::fmt::Display for SystemCommand {
//     fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
//         match self {
//             SystemCommand::Create => write!(f, "Create"),
//             SystemCommand::Connect => write!(f, "Connect"),
//             SystemCommand::Reset => write!(f, "Reset"),
//             SystemCommand::Shutdown => write!(f, "Shutdown"),
//             SystemCommand::CloseConnection => write!(f, "CloseConnection"),
//             SystemCommand::QuietMode => write!(f, "QuietMode"),
//             SystemCommand::ShowDatabases => write!(f, "ShowDatabases"),
//             SystemCommand::Test => write!(f, "Test"),
//             SystemCommand::Help => write!(f, "Help"),
//         }
//     }
// }

/// Enum for database commands related to data manipulation and querying applied to database state.
#[derive(Debug, PartialEq, Eq, Serialize, Deserialize, Clone)]
pub enum DBCommand {
    /// Execute a specific SQL statement.
    ExecuteSQL,
    /// Register a query for future use.
    RegisterQuery,
    /// Run a registered query to a specific timestamp.
    RunQueryFull,
    /// Run a registered query for the diffs of a timestamp range.
    RunQueryPartial,
    /// Convert a SQL query to a JSON plan.
    ConvertQuery,
    /// Show all tables in the current database.
    ShowTables,
    /// Show all registered queries in the current database.
    ShowQueries,
    /// Generates a CSV file from a specified source.
    Generate,
    /// Import a CSV file into a specified table.
    Import,
    /// Commit a trasncation
    Commit,
}

// impl std::fmt::Display for DBCommand {
//     fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
//         match self {
//             DBCommand::ExecuteSQL => write!(f, "ExecuteSQL"),
//             DBCommand::RegisterQuery => write!(f, "RegisterQuery"),
//             DBCommand::RunQueryFull => write!(f, "RunQueryFull({})"),
//             DBCommand::RunQueryPartial(s) => write!(f, "RunQueryPartial({})"),
//             DBCommand::ConvertQuery(s,_) => write!(f, "ConvertQuery({})"),
//             DBCommand::ShowTables => write!(f, "ShowTables"),
//             DBCommand::ShowQueries => write!(f, "ShowQueries"),
//             DBCommand::Generate(s,_) => write!(f, "Generate({})"),
//             DBCommand::Import(s, p) => write!(f, "Import({}, {:?})"),
//         }
//     }
// }

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize, Clone)]
pub enum Command {
    System(SystemCommand),
    DB(DBCommand),
}

// impl std::fmt::Display for Command {
//     fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
//         match self {
//             Command::System(s) => write!(f, "{}", s),
//             Command::DB(s) => write!(f, "{}", s),
//         }
//     }
// }

#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub struct CommandWithArgs {
    pub command: Command,
    pub args: Vec<String>,
}

/// Types of acceptable commands.
#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Response {
    Ok,
    SystemMsg(String),
    SystemErr(String),
    QueryResult(crate::QueryResult),
    QueryExecutionError(String),
    Shutdown(bool), // true if the request for shutdown comes from the client
    QuietOk,
    QuietErr,
}

impl Response {
    pub fn is_ok(&self) -> bool {
        match self {
            Response::Ok => true,
            Response::SystemMsg(_) => true,
            Response::SystemErr(_) => false,
            Response::QueryResult(_) => true,
            Response::QueryExecutionError(_) => false,
            Response::Shutdown(_) => true,
            Response::QuietOk => true,
            Response::QuietErr => false,
        }
    }
}

pub fn parse_command(mut cmd: String) -> Option<CommandWithArgs> {
    if cmd.ends_with('\n') {
        cmd.pop();
        if cmd.ends_with('\r') {
            cmd.pop();
        }
    }

    // Handle regular SQL commands not prefixed with '\'
    if !cmd.starts_with('\\') {
        return Some(CommandWithArgs {
            command: Command::DB(DBCommand::ExecuteSQL),
            args: vec![cmd],
        });
    }
    cmd.remove(0); // remove \

    for command in COMMANDS.iter() {
        if cmd.starts_with(command.0) {
            // We can have a command with a shared prefix. For example, '\r' and '\reset'.
            // We need to check if the command is the same as the prefix or if it is a different command.
            if (cmd.len() > command.0.len()) && (cmd.chars().nth(command.0.len()).unwrap() != ' ') {
                continue;
            }
            // We have a command match
            if command.1 == 0 {
                // There are no arguments
                return Some(CommandWithArgs {
                    command: command.2.clone(),
                    args: vec![],
                });
            } else {
                let cmd = cmd.strip_prefix(command.0).unwrap().trim_start();
                let limit = command.1 as usize;
                // Split the string, limit the number of splits, and collect into a Vec<String>
                let result: Vec<String> = cmd
                    .split(' ')
                    .take(limit) // Take only the first 'limit' elements
                    .map(str::to_string) // Convert &str to String
                    .collect();

                // Append the remaining parts as a single string
                let remaining = cmd.split(' ').skip(limit).collect::<Vec<&str>>().join(" ");
                let mut args = result;

                if !remaining.is_empty() {
                    args[limit - 1] = format!("{} {}", args[limit - 1], remaining);
                }
                return Some(CommandWithArgs {
                    command: command.2.clone(),
                    args,
                });
            }
        }
    }
    None
}

pub fn gen_help_string() -> String {
    let mut help = String::from("Commands:\n");
    for command in COMMANDS.iter() {
        let args = match command.1 {
            0 => "",
            1 => " <arg>",
            _ => " <arg1> <arg2>",
        };
        help.push_str(&format!("\\{}{}: {}\n", command.0, args, command.3));
    }
    help
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_create() {
        let create: String = String::from("\\r name");
        assert_eq!(
            CommandWithArgs {
                command: Command::System(SystemCommand::Create),
                args: vec!["name".to_string()]
            },
            parse_command(create).unwrap()
        );
    }

    #[test]
    fn test_connect() {
        let connect: String = String::from("\\c name");
        assert_eq!(
            CommandWithArgs {
                command: Command::System(SystemCommand::Connect),
                args: vec!["name".to_string()]
            },
            parse_command(connect).unwrap()
        );
    }

    #[test]
    fn test_import() {
        let import: String = String::from("\\i path name");
        assert_eq!(
            CommandWithArgs {
                command: Command::DB(DBCommand::Import),
                args: vec!["path".to_string(), "name".to_string()]
            },
            parse_command(import).unwrap()
        );
    }

    #[test]
    fn test_reset() {
        let reset: String = String::from("\\reset\n");
        assert_eq!(
            CommandWithArgs {
                command: Command::System(SystemCommand::Reset),
                args: vec![]
            },
            parse_command(reset).unwrap()
        );
    }

    #[test]
    fn test_show_tables() {
        let show_tables: String = String::from("\\dt\n");
        assert_eq!(
            CommandWithArgs {
                command: Command::DB(DBCommand::ShowTables),
                args: vec![]
            },
            parse_command(show_tables).unwrap()
        );
    }

    #[test]
    fn test_bad_command() {
        let bad_command: String = String::from("\\bad\n");
        assert_eq!(None, parse_command(bad_command));
    }

    #[test]
    fn test_sql_command() {
        let sql_command: String = String::from("SELECT * FROM table");
        assert_eq!(
            CommandWithArgs {
                command: Command::DB(DBCommand::ExecuteSQL),
                args: vec![sql_command.clone()]
            },
            parse_command(sql_command).unwrap()
        );
    }

    #[test]
    fn test_reg_sql_command() {
        let sql_command: String = String::from("\\register newq SELECT * FROM table");
        assert_eq!(
            CommandWithArgs {
                command: Command::DB(DBCommand::RegisterQuery),
                args: vec!["newq".to_string(), "SELECT * FROM table".to_string()]
            },
            parse_command(sql_command).unwrap()
        );
    }
}
