extern crate rustyline;
use common::error::c_err;
use common::{CrustyError, QueryResult};
use log::{debug, error, info};
use rustyline::history::FileHistory;

use rustyline::error::ReadlineError;
use rustyline::{DefaultEditor, Editor};

use std::io::{Read, Write};
use std::net::TcpStream;

pub use common::commands::Response;
use common::commands::{self, CommandWithArgs};
use common::physical::config::ClientConfig;

#[allow(dead_code)]
pub struct Client {
    config: ClientConfig,
    stream: TcpStream,
}

pub fn connect_to_kill_server(config: &ClientConfig) -> Result<(), CrustyError> {
    let mut bind_addr = config.host.clone();
    bind_addr.push(':');
    bind_addr.push_str(&config.port);
    let _ = TcpStream::connect(bind_addr)?;
    Ok(())
}

impl Client {
    pub fn new(config: ClientConfig) -> Self {
        let _ = env_logger::builder().try_init();
        let mut bind_addr = config.host.clone();
        bind_addr.push(':');
        bind_addr.push_str(&config.port);
        let stream = TcpStream::connect(bind_addr).unwrap();
        Client { config, stream }
    }

    pub fn run_cli(&mut self) {
        let mut rl = DefaultEditor::new().unwrap();
        if rl.load_history("history.txt").is_err() {
            info!("No previous history.");
        }

        if self.config.script.is_empty() {
            info!("No script provided. Starting CLI...");
        } else {
            info!("Running script: {}", self.config.script);
            let file = std::fs::File::open(&self.config.script).unwrap();
            let _ = self.send_requests_from_buffer(file);
        }

        self.process_cli_loop(&mut rl);

        if rl.save_history("history.txt").is_err() {
            error!("Error saving history.");
        }
    }

    fn process_cli_loop(&mut self, rl: &mut Editor<(), FileHistory>) {
        let prompt: &str = "[fairydb]>>";

        while let Some(line) = self.read_cli_line(rl, prompt) {
            if !line.is_empty() {
                let _ = rl.add_history_entry(&line);
                match self.handle_command(line) {
                    Ok(response) => {
                        if !self.handle_response(response) {
                            info!("Server shutdown");
                            break;
                        }
                    }
                    Err(e) => {
                        error!("Error: {}", e);
                    }
                }
            }
        }
    }

    fn read_cli_line(&self, rl: &mut Editor<(), FileHistory>, prompt: &str) -> Option<String> {
        match rl.readline(prompt) {
            Ok(line) => Some(line),
            Err(ReadlineError::Interrupted) => {
                info!("CTRL-C");

                // gracefully shut down when receiving ctrl-c
                Some(String::from("\\shutdown"))
            }
            Err(ReadlineError::Eof) => {
                info!("CTRL-D");
                None
            }
            Err(err) => {
                error!("Error: {:?}", err);
                None
            }
        }
    }

    pub fn send_requests_from_buffer<T: Read>(
        &mut self,
        mut buffer: T,
    ) -> Result<Vec<Response>, CrustyError> {
        let mut content = String::new();
        match buffer.read_to_string(&mut content) {
            Ok(_) => {
                let commands = self.split_into_commands(&content);
                let mut responses = Vec::new();
                for command in commands {
                    responses.push(self.handle_command(command)?);
                }
                Ok(responses)
            }
            Err(e) => {
                error!("Failed to read buffer: {:?}", e);
                Err(c_err("Failed to read buffer"))
            }
        }
    }

    fn handle_command(&mut self, command: String) -> Result<Response, CrustyError> {
        match commands::parse_command(command.clone()) {
            Some(request) => {
                debug!("Request to send {:?}", request);
                self.send_and_wait(&request)
            }
            None => {
                info!("Invalid request: {}", command);
                Err(c_err("Invalid request"))
            }
        }
    }

    fn split_into_commands(&self, buffer_content: &str) -> Vec<String> {
        buffer_content
            .split(';')
            .map(str::trim)
            .filter(|cmd| !cmd.is_empty())
            .map(|cmd| cmd.replace('\n', " "))
            .collect()
    }

    /// Sends a request to the server and waits for a response.
    fn send_and_wait(&mut self, request: &CommandWithArgs) -> Result<Response, CrustyError> {
        if !self.send_request(request) {
            return Err(c_err("Failed to send request"));
        }

        let response_data = self.receive_response()?;

        if response_data.is_empty() {
            info!("Received empty response. Check server logs.");
            return Ok(Response::QuietOk);
        }

        let response: Response = match serde_cbor::from_slice(&response_data) {
            Ok(resp) => {
                match &resp {
                    // pretty print table results
                    Response::QueryResult(query_result) => {
                        info!("Received response:\n{}", query_result);
                        resp
                    }
                    _ => {
                        info!("Received response: {:?}", &resp);
                        resp
                    }
                }
            }
            Err(e) => {
                error!("Failed to deserialize response: {:?}", e);
                return Err(c_err("Failed to deserialize response"));
            }
        };

        Ok(response)
    }

    fn send_request(&mut self, request: &CommandWithArgs) -> bool {
        let serialized_request = match serde_cbor::to_vec(request) {
            Ok(bytes) => bytes,
            Err(e) => {
                error!("Failed to serialize request: {:?}", e);
                return false;
            }
        };

        if let Err(e) = self.stream.write_all(&serialized_request) {
            error!("Error sending data: {:?}", e);
            false
        } else {
            true
        }
    }

    fn receive_response(&mut self) -> Result<Vec<u8>, CrustyError> {
        // Read the length of the response first
        // TODO xtx magic number again
        let mut length_bytes = [0u8; 8];
        self.stream.read_exact(&mut length_bytes)?;
        let response_length = u64::from_be_bytes(length_bytes) as usize;

        // Now read the response data
        let mut response_data = Vec::with_capacity(response_length);
        while response_data.len() < response_length {
            // TODO xtx magic number need to make a config file
            let mut buffer =
                vec![0; std::cmp::min(1024 * 32, response_length - response_data.len())];
            let size = self.stream.read(&mut buffer)?;
            if size == 0 {
                break; // End of stream
            }
            response_data.extend_from_slice(&buffer[..size]);
        }

        if response_data.len() != response_length {
            return Err(c_err("Incomplete response received"));
        }

        Ok(response_data)
    }

    // Returns true if the server should continue running.
    fn handle_response(&self, response: Response) -> bool {
        match response {
            Response::Shutdown(from_client) => {
                if from_client {
                    info!("Received shutdown due to client");
                } else {
                    info!("Received shutdown due to server");
                }

                // If the request for shutdown comes from the client, send a connection to unblock the server after shutdown
                if from_client {
                    info!("Sending connection to unblock server...");
                    match connect_to_kill_server(&self.config) {
                        Ok(_) => info!("Successfully sent unblock connection"),
                        Err(e) => error!("Failed to send unblock connection: {}", e),
                    }
                }

                false
            }
            Response::Ok => {
                info!("Received OK");
                true
            }
            Response::SystemMsg(msg) => {
                info!("Received SystemMsg: {}", msg);
                true
            }
            Response::SystemErr(msg) => {
                error!("Received SystemErr: {}", msg);
                true
            }
            Response::QueryResult(result) => self.process_query_result(result),
            Response::QueryExecutionError(msg) => {
                error!("Received QueryExecutionError: {}", msg);
                true
            }
            Response::QuietOk => {
                debug!("Received quiet OK");
                true
            }
            Response::QuietErr => {
                debug!("Received quiet Err");
                true
            }
        }
    }

    fn process_query_result(&self, result: QueryResult) -> bool {
        match result {
            QueryResult::MessageOnly(message) => {
                info!("Received Query Result: {}", message);
            }
            QueryResult::Select { .. } => {
                // pretty‐print the table using the Display impl
                println!("{}", result);
            }
            QueryResult::Insert {
                inserted,
                table_name,
            } => {
                let message = format!("Inserted {} rows to table: {}", inserted, table_name);
                info!("Received Query Result: {}", message);
            }
        }
        true
    }
}
