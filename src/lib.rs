#![doc = include_str!("../README.md")]

mod executor;
mod mpsc;
mod mpsc2;
mod preempt;
mod yield_once;

mod join;
mod queue;
mod stats;
mod task;

pub use executor::{yield_maybe, Executor, ExecutorBuilder};
pub use join::{JoinError, JoinHandle};
pub use queue::{Queue, QueueKey};
