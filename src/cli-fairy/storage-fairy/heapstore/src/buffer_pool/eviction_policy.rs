use std::cell::RefCell;

use rand::rngs::SmallRng;
use rand::{RngCore, SeedableRng};

use super::buffer_frame::BufferFrame;

// Thread-local `SmallRng` state.
thread_local! {
    static THREAD_RNG_KEY: RefCell<SmallRng> = RefCell::new(SmallRng::from_os_rng());
}

/// A handle to the thread-local `SmallRng`—similar to `rand::ThreadRng`.
#[derive(Debug, Clone)]
pub struct SmallThreadRng;

impl RngCore for SmallThreadRng {
    fn next_u32(&mut self) -> u32 {
        THREAD_RNG_KEY.with(|rng_cell| rng_cell.borrow_mut().next_u32())
    }

    fn next_u64(&mut self) -> u64 {
        THREAD_RNG_KEY.with(|rng_cell| rng_cell.borrow_mut().next_u64())
    }

    fn fill_bytes(&mut self, dest: &mut [u8]) {
        THREAD_RNG_KEY.with(|rng_cell| rng_cell.borrow_mut().fill_bytes(dest))
    }
}

// Structures implementing this trait are used to determine which buffer frame to evict.
// It must ensure that multiple threads can safely update the internal states concurrently.
pub trait EvictionPolicy: Send + Sync {
    fn new() -> Self;
    /// Returns the eviction score of the buffer frame.
    /// The lower the score, the more likely the buffer frame is to be evicted.
    fn score(&self, frame: &BufferFrame) -> u64
    where
        Self: Sized;
    fn update(&self);
    fn reset(&self);
}

pub struct DummyEvictionPolicy; // Used for in-memory pool
impl EvictionPolicy for DummyEvictionPolicy {
    #[inline]
    fn new() -> Self {
        DummyEvictionPolicy
    }

    #[inline]
    fn score(&self, _frame: &BufferFrame) -> u64 {
        0
    }

    #[inline]
    fn update(&self) {}

    #[inline]
    fn reset(&self) {}
}
