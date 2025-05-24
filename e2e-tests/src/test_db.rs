use std::io::Cursor;

use cli_crusty::{connect_to_kill_server, Client, Response};
use common::physical::config::{ClientConfig, ServerConfig};
use log::info;

use crate::sqllogictest_utils::start_server;

/// Create a test database (`testdb`) and a client to connect to it.
/// The client and the server will be dropped when the `TestDB` struct is dropped.
pub struct TestDB {
    server: Option<std::thread::JoinHandle<()>>,
    client: Option<Client>,
}

impl Default for TestDB {
    fn default() -> Self {
        Self::new()
    }
}

impl TestDB {
    pub fn new() -> Self {
        // create a new server in a different thread
        let server_config = Box::leak(Box::new(ServerConfig::temporary()));
        let server = start_server(server_config);

        // wait for the server to start
        let duration = std::time::Duration::from_secs(1);
        info!("Waiting {}ms for server to start...", duration.as_millis());
        std::thread::sleep(duration);

        let client_config = ClientConfig::default();
        let mut client = Client::new(client_config);

        // First create a test database and connect to it
        let response = client
            .send_requests_from_buffer(Cursor::new(b"\\r testdb"))
            .unwrap();
        assert!(response[0].is_ok());
        let response = client
            .send_requests_from_buffer(Cursor::new(b"\\c testdb"))
            .unwrap();
        assert!(response[0].is_ok());

        Self {
            server: Some(server),
            client: Some(client),
        }
    }

    pub fn send_command(&mut self, command: &[u8]) -> Vec<Response> {
        let response = self
            .client
            .as_mut()
            .unwrap()
            .send_requests_from_buffer(Cursor::new(command))
            .unwrap();
        assert!(response[0].is_ok());
        response
    }
}

impl Drop for TestDB {
    fn drop(&mut self) {
        let response = self
            .client
            .as_mut()
            .unwrap()
            .send_requests_from_buffer(Cursor::new(b"\\reset"))
            .unwrap();
        assert!(response[0].is_ok());

        // shutdown the server
        let response = self
            .client
            .as_mut()
            .unwrap()
            .send_requests_from_buffer(Cursor::new(b"\\shutdown\n"))
            .unwrap();
        assert_eq!(response, vec![Response::Shutdown(true)]);

        info!("Shutting down client...");
        drop(self.client.take().unwrap());
        info!("Client shutdown successfully");

        info!("Waiting for server to shutdown...");
        // Hack: connect a new client to kill the server.
        // Server is blocking on tcp_listener.incoming()
        // so we need to connect to it to unblock it
        // and make it check the shutdown flag.
        connect_to_kill_server(&ClientConfig::default()).unwrap();
        drop(self.server.take().unwrap());
        info!("Server shutdown successfully");
    }
}
