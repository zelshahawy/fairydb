use common::physical::config::ServerConfig;
use server::Server;

/// Entry point for server.
///
/// Waits for user connections and creates a new thread for each connection.
fn main() {
    let config = ServerConfig::from_command_line();
    let mut server = Server::new(Box::leak(Box::new(config)));
    server.run_server();
}
