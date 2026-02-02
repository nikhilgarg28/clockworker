pub type TaskId = usize;

pub trait QueueKey: Eq + Sized + Copy + Send + Sync + std::fmt::Debug + 'static {}
impl<K> QueueKey for K where K: Eq + Sized + Copy + Send + Sync + std::fmt::Debug + 'static {}

pub struct Queue<K: QueueKey> {
    id: K,
    share: u64,
}
impl<K: QueueKey> Queue<K> {
    pub fn new(id: K, share: u64) -> Self {
        Self { id, share }
    }
    pub fn id(&self) -> K {
        self.id
    }
    pub fn share(&self) -> u64 {
        self.share
    }
}

use crate::mpsc::Mpsc;
use futures_util::task::AtomicWaker;
use std::cell::Cell;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::Waker;

/// Sentinel value for empty LIFO slot.
const LIFO_EMPTY: usize = usize::MAX;

/// Task queue that encapsulates MPSC and LIFO slot optimization.
///
/// This struct manages:
/// - A pure MPSC queue (no length, no waker)
/// - A LIFO slot for cache locality (most recently enqueued task)
/// - Length tracking (total items in LIFO + MPSC)
/// - Waker management (single wake point)
pub struct TaskQueue {
    mpsc: Mpsc<TaskId>,
    lifo_slot: AtomicUsize,
    lifo_counter: Cell<usize>,
    len: AtomicUsize,
    waker: AtomicWaker,
    lifo_skip_interval: usize,
    enable_lifo: bool,
}

impl TaskQueue {
    /// Creates a new TaskQueue.
    ///
    /// - `enable_lifo`: If true, use LIFO slot optimization. If false, pure FIFO.
    /// - `lifo_skip_interval`: Skip LIFO every N pops to maintain fairness (only used if `enable_lifo` is true).
    pub fn new(enable_lifo: bool, lifo_skip_interval: usize) -> Self {
        Self {
            mpsc: Mpsc::new(),
            lifo_slot: AtomicUsize::new(LIFO_EMPTY),
            lifo_counter: Cell::new(0),
            len: AtomicUsize::new(0),
            waker: AtomicWaker::new(),
            lifo_skip_interval,
            enable_lifo,
        }
    }

    /// Enqueues a task.
    ///
    /// If LIFO is enabled:
    /// - Swaps the task into the LIFO slot
    /// - If LIFO was empty, increments length and wakes if queue was empty
    /// - If LIFO had a task, pushes the old task to MPSC (length stays same)
    ///
    /// If LIFO is disabled:
    /// - Pushes directly to MPSC
    /// - Increments length and wakes if queue was empty
    pub fn push(&self, task_id: TaskId) {
        if self.enable_lifo {
            // Try LIFO slot
            let old_id = self.lifo_slot.swap(task_id, Ordering::AcqRel);
            if old_id != LIFO_EMPTY {
                // LIFO had a task - push old one to MPSC
                self.mpsc.push(old_id);
            }
        } else {
            // LIFO disabled - just push to MPSC
            self.mpsc.push(task_id);
        }
        // Wake if queue was empty (transition from empty to non-empty)
        // This ensures the executor is notified when work becomes available
        if self.len.fetch_add(1, Ordering::AcqRel) == 0 {
            self.waker.wake();
        }
    }

    /// Pops a task from the queue.
    ///
    /// If LIFO is enabled and counter allows:
    /// - Tries LIFO first, falls back to MPSC if LIFO is empty
    /// - Skips LIFO every `lifo_skip_interval` pops for fairness
    ///
    /// If LIFO is disabled:
    /// - Pops directly from MPSC
    ///
    /// Decrements length when a task is popped.
    pub fn pop(&self) -> Option<TaskId> {
        let result = self.pop_from_lifo().or_else(|| self.mpsc.pop());
        if result.is_some() {
            self.len.fetch_sub(1, Ordering::Release);
        }
        result
    }

    fn pop_from_lifo(&self) -> Option<TaskId> {
        if !self.enable_lifo {
            return None;
        }
        let counter = self.lifo_counter.get();
        // Increment counter first, then check if we should skip LIFO
        // This way counter 0 uses LIFO, counter 16 skips LIFO, etc.
        let counter = counter + 1;
        self.lifo_counter.set(counter);
        let use_lifo = (counter % self.lifo_skip_interval) != 0;
        if use_lifo {
            let lifo_id = self.lifo_slot.swap(LIFO_EMPTY, Ordering::AcqRel);
            if lifo_id != LIFO_EMPTY {
                return Some(lifo_id);
            }
        }
        None
    }

    /// Checks if the queue is empty.
    ///
    /// Fast path: just checks the length counter.
    pub fn is_empty(&self) -> bool {
        self.len.load(Ordering::Acquire) == 0
    }

    /// Drains the LIFO slot, moving any task to the back of MPSC.
    ///
    /// Called at the start of a timeslice to ensure only tasks woken
    /// during the current execution benefit from LIFO cache locality.
    /// Tasks that have been sitting in LIFO across timeslices have
    /// already lost their cache benefit due to context switching.
    pub fn drain_lifo_to_mpsc(&self) {
        if self.enable_lifo {
            let lifo_id = self.lifo_slot.swap(LIFO_EMPTY, Ordering::AcqRel);
            if lifo_id != LIFO_EMPTY {
                // Move LIFO task to MPSC (back of queue)
                self.mpsc.push(lifo_id);
                // Length stays the same (1 in LIFO -> 1 in MPSC)
                // No wake needed - queue is already runnable (we're about to run it)
            }
        }
    }

    /// Registers a waker to be notified when tasks are enqueued.
    ///
    /// Single registration point for the TaskQueue waker.
    pub fn register_waker(&self, waker: &Waker) {
        self.waker.register(waker);
    }
}

unsafe impl Send for TaskQueue {}
unsafe impl Sync for TaskQueue {}

impl std::fmt::Debug for TaskQueue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TaskQueue")
            .field("lifo_skip_interval", &self.lifo_skip_interval)
            .field("enable_lifo", &self.enable_lifo)
            .field("len", &self.len.load(Ordering::Acquire))
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::thread;

    #[test]
    fn test_task_queue_enqueue_empty_lifo() {
        let queue = TaskQueue::new(true, 16);
        assert!(queue.is_empty());

        queue.push(1);
        assert!(!queue.is_empty());
    }

    #[test]
    fn test_task_queue_enqueue_non_empty_lifo() {
        let queue = TaskQueue::new(true, 16);
        queue.push(1);
        queue.push(2);

        // First task (1) should be in LIFO, second (2) should push 1 to MPSC
        // Length should still be 2
        assert!(!queue.is_empty());
    }

    #[test]
    fn test_task_queue_pop_lifo() {
        let queue = TaskQueue::new(true, 16);
        queue.push(1);
        queue.push(2);

        // First pop should get 2 (LIFO)
        assert_eq!(queue.pop(), Some(2));
        // Second pop should get 1 (from MPSC)
        assert_eq!(queue.pop(), Some(1));
        assert_eq!(queue.pop(), None);
        assert!(queue.is_empty());
    }

    #[test]
    fn test_task_queue_pop_skip_interval() {
        let queue = TaskQueue::new(true, 4); // Skip every 4th pop

        // Fill LIFO and MPSC
        queue.push(10); // Goes to LIFO
        queue.push(20); // Pushes 10 to MPSC, 20 to LIFO
        queue.push(30); // Pushes 20 to MPSC, 30 to LIFO
        queue.push(40); // Pushes 30 to MPSC, 40 to LIFO
                        // State: LIFO=40, MPSC=[10,20,30]

        // Counter starts at 0
        // Pop 1: counter becomes 1, 1 % 4 != 0, use LIFO -> 40
        assert_eq!(queue.pop(), Some(40));
        // State: LIFO=empty, MPSC=[10,20,30]

        // Pop 2: counter becomes 2, 2 % 4 != 0, try LIFO (empty), fall back to MPSC -> 10
        assert_eq!(queue.pop(), Some(10));
        // State: LIFO=empty, MPSC=[20,30]

        // Pop 3: counter becomes 3, 3 % 4 != 0, try LIFO (empty), fall back to MPSC -> 20
        assert_eq!(queue.pop(), Some(20));
        // State: LIFO=empty, MPSC=[30]

        // Pop 4: counter becomes 4, 4 % 4 == 0, skip LIFO, go to MPSC -> 30
        assert_eq!(queue.pop(), Some(30));

        // Add more items to test skip when LIFO is non-empty
        queue.push(50); // Goes to LIFO
        queue.push(60); // Pushes 50 to MPSC, 60 to LIFO
                        // State: LIFO=60, MPSC=[50]

        // Pop 5: counter becomes 5, 5 % 4 != 0, use LIFO -> 60
        assert_eq!(queue.pop(), Some(60));
        // Pop 6: counter becomes 6, 6 % 4 != 0, try LIFO (empty), fall back to MPSC -> 50
        assert_eq!(queue.pop(), Some(50));
    }

    #[test]
    fn test_task_queue_lifo_disabled() {
        let queue = TaskQueue::new(false, 16);
        queue.push(1);
        queue.push(2);
        queue.push(3);

        // Should be pure FIFO
        assert_eq!(queue.pop(), Some(1));
        assert_eq!(queue.pop(), Some(2));
        assert_eq!(queue.pop(), Some(3));
        assert_eq!(queue.pop(), None);
    }

    #[test]
    fn test_task_queue_is_empty() {
        let queue = TaskQueue::new(true, 16);
        assert!(queue.is_empty());

        queue.push(1);
        assert!(!queue.is_empty());

        queue.pop();
        assert!(queue.is_empty());
    }

    #[test]
    fn test_task_queue_concurrent_enqueue() {
        let queue = Arc::new(TaskQueue::new(true, 16));
        let num_threads = 8;
        let items_per_thread = 1000;

        let handles: Vec<_> = (0..num_threads)
            .map(|t| {
                let queue = Arc::clone(&queue);
                thread::spawn(move || {
                    for i in 0..items_per_thread {
                        queue.push(t * items_per_thread + i);
                    }
                })
            })
            .collect();

        for h in handles {
            h.join().unwrap();
        }

        // Pop all items and verify we got all of them
        let mut count = 0;
        while queue.pop().is_some() {
            count += 1;
        }
        assert_eq!(count, num_threads * items_per_thread);
        assert!(queue.is_empty());
    }

    #[test]
    fn test_task_queue_length_accuracy() {
        let queue = TaskQueue::new(true, 16);

        // Enqueue multiple items
        for i in 1..=10 {
            queue.push(i);
        }

        // Pop some items
        for _ in 0..5 {
            assert!(queue.pop().is_some());
        }

        // Should still have 5 items
        let mut remaining = 0;
        while queue.pop().is_some() {
            remaining += 1;
        }
        assert_eq!(remaining, 5);
        assert!(queue.is_empty());
    }
}
