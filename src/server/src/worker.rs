use crate::server_state::ServerState;
use std::sync::mpsc;
use std::sync::Arc;
use std::sync::Mutex;
use std::thread;
use std::time::Duration;

// Modelled on https://doc.rust-lang.org/book/ch20-03-graceful-shutdown-and-cleanup.html

type Job = Box<dyn FnOnce() + Send + 'static>;

#[allow(dead_code)]
pub enum Message {
    _NewJob(Job),
    Test,
    Terminate,
}

#[allow(dead_code)]
pub(crate) struct Worker {
    pub(crate) id: usize,
    pub(crate) thread: Option<thread::JoinHandle<()>>,
    _server_state: &'static ServerState,
}

#[allow(dead_code)]
impl Worker {
    pub(crate) fn new(
        id: usize,
        receiver: Arc<Mutex<mpsc::Receiver<Message>>>,
        server_state: &'static ServerState,
    ) -> Worker {
        info!("creating worker {}", id);
        let thread = thread::spawn(move || loop {
            let message = receiver.lock().unwrap().recv().unwrap();

            match message {
                Message::_NewJob(job) => {
                    debug!("Worker {} got a job; executing.", id);

                    job();
                }
                Message::Terminate => {
                    info!("Worker {} was told to terminate.", id);

                    break;
                }
                Message::Test => {
                    info!("Worker {} is testing.", id);
                    thread::sleep(Duration::new(7, 0));
                    info!("Worker {} is done testing.", id);
                }
            }
        });

        Worker {
            id,
            thread: Some(thread),
            _server_state: server_state,
        }
    }
}
