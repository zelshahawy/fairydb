# FairyDB
FairyDB is a SQL‑compliant, high‑performance OLTP storage engine written in Rust.
It provides a lightweight, heap‑file based storage layer with a buffer‑pool backed page allocator designed for low‑latency
transactional workloads.

## Usage

Make sure you have Rust > 1.81.0. Updating the rust toolkit is pretty easy, just do:

```bash
$ rustup update
```

You can then check the version by doing:

```bash
$ rustc --version
```

```
$ module load rust
```

You can verify this worked with
```
$ rustc --version
rustc 1.82.0
$ cargo --version
cargo 1.82.0
```

## Building project
To build the entire fairyDB source code, you would run `cargo build`

fairyDB is set up as a workspace and various modules/components of the database are broken into separate packages/crates. To build a specific crate (for example common), you would use the following command `cargo build -p common`. Note if a package/crate depends on another crate (e.g. heapstore depends on common and txn_manager) those crates will be built as part of the process. **Note that for the first milestone you will only have access to common and limited part of heapstore.**


These crates are:
- `cli-fairy` : a command line interface client binary application that can connect and issue commands/queries to a running CrustyDB server.
- `common` : shared data structures or logical components needed by everything in fairyDB. this includes things like tables, errors, logical query plans, ids, some test utilities, etc. This is organized into modules that split out definitions related to the physical layout, shared query execution operations and representations, traits (interfaces), and utilities. Common metadata, structs, typedefs, enums, and errors are all located in the `base' module. 
- `index`:  for managing indexes. This is a work in progress and not fully implemented. 
- `optimizer` : a crate for query optimization.
- `queryexe` : responsible for executing queries. This contains the operator implementations as well as the execution code for a volcano style execution engine.
- `server` : the binary crate for running a fairyDB server. This connects all modules (outside a client) together.
- `storage`: the storage managers for the database. This includes multiple implementations and a buffer pool. Only one storage manager can be defined/used at a time The two main storage managers used in the project are:
  - `heapstore` : a storage manager for storing data in heap files. milestone `hs` is exclusively in this crate.
  - `memstore` : a poorly written storage manager that keeps everything in memory. it will persist data to files using serde on shutdown, and use these files to recreate the database state at shutdown
- `txn_manager` : a near empty crate for an optional milestone to implement transactions. the use a `transaction` is embedded in many other crates, but can be safely ignored for the given milestones. There is also the use of a logical timestamp throughout many components. You can safely ignore this.
- `utilities` : utilities for performance benchmarks that will be used by an optional milestone

There are two other projects outside of fairydb workspace that we will use later `e2e-benchmarks` and `e2e-tests`. These are used for end-to-end testing (eg sending SQL to the server and getting a response).

## To use the application after building, you should first run the server bin and then the cli bin. 

When running the CLI: I highly recommend doing the following:

    - \r mydb # which creates a database. You can change mydb with any name you like.

    - \c mydb # connects to the database you just made.

    - CREATE TABLE users (id INTEGER PRIMARY KEY, name VARCHAR(255)); # Creates a dummy table for your database

    - INSERT INTO users VALUES (1, 'alice'); # insert dummy values

    - SELECT * FROM users; # retrieve all values

You could find a list of all commands down this file.

## Tests

Most crates have tests that can be run using cargo `cargo test`. Like building you can run tests for a single crate `cargo test -p common`. Note that tests will build/compile code in the tests modules, so you may encounter build errors here that do not show up in a regular build.


### Running an ignored test
Some longer tests are set to be ignored by default. To run them: `cargo test -- --ignored`

## Logging

fairyDB uses the [env_logger](https://docs.rs/env_logger/0.8.2/env_logger/) crate for logging messages. Per the docs on the log crate:
```
The basic use of the log crate is through the five logging macros: error!, warn!, info!, debug! and trace! 
where error! represents the highest-priority log messages and trace! the lowest. 
The log messages are filtered by configuring the log level to exclude messages with a lower priority. 
Each of these macros accept format strings similarly to println!.
```

The logging level is set by an environmental variable, `RUST_LOG`. The easiest way to set the level is when running a cargo command you set the logging level in the same command. EG : `RUST_LOG=debug cargo run --bin server`. However, when running unit tests the logging/output is suppressed and the logger is not initialized. So if you want to use logging for a test you must:
- Make sure the test in question calls `init()` which is defined in `common::testutils` that initializes the logger. It can safely be called multiple times.
- Tell cargo to not capture the output. For example, setting the level to DEBUG: `RUST_LOG=debug cargo test -- --nocapture [opt_test_name]`  **note the -- before --nocapture**

Examples:
```
RUST_LOG=debug cargo run --bin server
RUST_LOG=debug cargo test
RUST_LOG=debug cargo test -- --nocapture [test_name]
```

In addition, the log level can also be controlled programmatically. The log
level is set in the first line of the main() function in the server crate. By
default, this is set to DEBUG. Feel free to change this as you see fit.

### Connecting to a Database

This is the basic process for starting a database and connecting to it via the CLI client.

1. Start a server thread

    ```
    $ cargo run --bin server
    ```

2. Start a client with logging enabled to see output (this is in client.sh)

    ```
    $ RUST_LOG=info cargo run --bin cli-fairy
    ```

For convenience we have provided some shell scripts to run the server and client. The server has a debug and info mode for the logger.

### Client Command

fairyDB emulates psql commands.

Command | Functionality
---------|--------------
`\r [DATABABSE]` | cReates a new database, DATABASE
`\c [DATABASE]` | Connects to DATABASE
`\i [PATH] [TABLE_NAME]` | Imports a csv file at PATH and saves it to TABLE_NAME in 
whatever database the client is currently connected to.
`\l` | List the name of all databases present on the server.
`\dt` | List the name of all tables present on the current database.
`\generate [CSV_NAME] [NUMBER_OF_RECORDS]` | Generate a test CSV for a sample schema.
`\reset` | Calls the reset command. This should delete all data and state for all databases on the server
`\close` | Closes the current client, but leaves the database server running
`\shutdown` |  Shuts down the database server cleanly (allows the DB to gracefully exit)

There are other commands you can ignore for this class (register, runFull, runPartial, convert).

The client also handles basic SQL queries.

## End to End Example

After compiling the database, start a server and a client instance.

To start the fairydb server:

```
$ cargo run --bin server
```

and to start the client:

```
$ cargo run --bin cli-fairy
```

Now, from the client, you can interact with the server. Create a database named
'testdb':

```
[fairydb]>> \r testdb 
```

Then, connect to the newly created database:

```
[fairydb]>> \c testdb
```

At this point, you can create a table 'test' in the 'testdb' database you are
connected to by writing the appropriate SQL command. Let's create a table with 2
Integer columns, which we are going to name 'a' and 'b'.

```
[fairydb]>> CREATE TABLE test (a INT, b INT, primary key (a));
```

At this point the table exists in the database, but it does not contain any data. We include a CSV file in the repository (named 'data.csv') with some sample data you can import into the newly created table. You can do that by doing:

```
[fairydb]>> \i <PATH>/data.csv test
```

Note that you need to replace PATH with the path to the repository where the
data.csv file lives.

After importing the data, you can run basic SQL queries on the table. For
example:

```
[fairydb]>> SELECT a FROM test;
```

or:

```
[fairydb]>> SELECT sum(a), sum(b) FROM test;
```

As you follow through this end to end example, we encourage you to take a look
at the log messages emitted by the server. You can search for those log messages
in the code: that is a great way of understanding the lifecycle of query
execution in fairydb.

### Client Scripts 

The client has an option of running a series of commands/queries from a text file. 
Each command or query must be separated by a ; (even commands that would not give 
a ; after when using the cli tool). To use the script pass `-- -s [script file]` 

We have included a sample script that you would invoke the following way:
```
cargo run -p cli-fairy -- -s test-client-script
```

### Shutdown

Note that shutting down the server is not automatic. You will need to
manually shut down the server by sending a \shutdown command from the client
or pressing Ctrl-C in the client terminal (Ctrl-D will disconnect the client but leave the server running).
This allows for a clean shutdown of the server and the database.

A non-clean shutdown of the server will likely leave the database in an inconsistent state.
You will need to clean the database by removing the `fairy_data` directory
and re-running the server (`rm -rf fairy_data/`). 

## Debugging Rust Programs

Debugging is a crucial skill you should learn (if you don't know yet) in order
to become a more effective software developer. If you write software, your
software will contain bugs. Debugging is the process of finding those bugs,
which is necessary if you want to fix them.

#### CLion

JetBrain's [CLion](https://www.jetbrains.com/clion/) IDE looks to have a solid Rust debugger with the Rust extension.
However CLion is not free, but it does offer academic licenses. [Apply here](https://www.jetbrains.com/community/education/#students)
if you want to access the tool (some restrictions on what you can use the tool for).
[Here are instructions on set up and using](https://blog.jetbrains.com/clion/2019/10/debugging-rust-code-in-clion/)
which worked for me out of the box on Ubuntu (with installing the Rust plugin).
One of our TAs uses CLion to debug Rust on OSX. The link also contains instructions for 
debugging on Windows, but it has not been tested by us.
