mod arrival_fifo;
mod las;
mod qlas;
mod runnable_fifo;
pub use arrival_fifo::ArrivalFifo;
pub use las::LAS;
pub use qlas::QLAS;
pub use runnable_fifo::RunnableFifo;

use crate::queue::TaskId;
use std::time::Instant;

/// Per-queue scheduler: chooses *which task* to run within the queue.
pub trait Scheduler {
    fn push(&mut self, id: TaskId, group: u64, at: Instant);
    fn pop(&mut self) -> Option<TaskId>;
    fn clear_task_state(&mut self, id: TaskId, group: u64);
    fn clear_group_state(&mut self, group: u64);
    fn is_runnable(&self) -> bool;
    fn observe(&mut self, id: TaskId, group: u64, start: Instant, end: Instant, ready: bool);
}
