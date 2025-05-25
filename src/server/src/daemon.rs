use crate::server_state::ServerState;
use std::thread;
use std::time::Duration;

#[allow(dead_code)]
pub(crate) struct Daemon {
    _server_state: &'static ServerState,
    pub(crate) _thread: Option<thread::JoinHandle<()>>,
}

#[allow(dead_code)]
impl Daemon {
    pub(crate) fn new(server_state: &'static ServerState, sleep_sec: u64) -> Self {
        // This should be async or moved into the workers
        let thread = std::thread::spawn(move || loop {
            debug!("Daemon doing stuff");

            thread::sleep(Duration::new(sleep_sec, 0));
        });

        Daemon {
            _server_state: server_state,
            _thread: Some(thread),
        }
    }
}
