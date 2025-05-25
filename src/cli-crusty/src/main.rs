use cli_crusty::Client;
use common::physical::config::ClientConfig;

fn main() {
    let config = ClientConfig::resolved();
    let mut client = Client::new(config);
    client.run_cli();
}
