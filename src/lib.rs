#![doc = include_str!("../README.md")]

mod executor;
mod yield_once;

mod join;
mod queue;
pub mod scheduler;
mod stats;
mod task;

pub use executor::{yield_maybe, Executor, ExecutorBuilder};
pub use join::{JoinError, JoinHandle};
pub use queue::{Queue, QueueKey};
