use cli_crusty::Client;
use common::physical::config::ClientConfig;

fn main() {
    let config = ClientConfig::resolved();
    print!("{} {}", config.host, config.port);
    let mut client = Client::new(config);
    client.run_cli();
}
