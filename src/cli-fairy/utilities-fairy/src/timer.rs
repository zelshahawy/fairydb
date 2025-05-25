use chrono::{DateTime, Utc};

pub struct Timer {
    start_time: DateTime<Utc>,
    name: String,
}

impl Timer {
    pub fn new(name: &str) -> Self {
        Timer {
            start_time: Utc::now(),
            name: name.to_string(),
        }
    }
}

impl Drop for Timer {
    fn drop(&mut self) {
        let elapsed = Utc::now() - self.start_time;
        // duration in ms
        let elapsed = elapsed.num_milliseconds();
        println!("Timer {}, elapsed {:?}ms", self.name, elapsed);
    }
}
