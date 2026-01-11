use crate::{
    join::{JoinHandle, JoinState},
    queue::{Queue, Scheduler, TaskId},
    task::TaskHeader,
    yield_once::yield_once,
};
use flume::{Receiver, Sender};
use futures::task::waker;
use slab::Slab;
use static_assertions::assert_not_impl_any;
use std::{
    cell::Cell,
    cell::RefCell,
    collections::HashSet,
    future::Future,
    pin::Pin,
    rc::Rc,
    sync::atomic::{AtomicU64, Ordering},
    sync::Arc,
    task::{Context, Poll},
    time::{Duration, Instant},
};
thread_local! {
    static YIELD_MAYBE_DEADLINE: Cell<Option<Instant>> = Cell::new(None);
}

fn set_yield_maybe_deadline(deadline: Instant) {
    YIELD_MAYBE_DEADLINE.with(|cell| cell.set(Some(deadline)));
}

/// Wraps a user given future to make it cancelable
/// This future only returns () - when the underlying future completes,
/// the result is published to the JoinState, which wrapped by Join Handle
/// can be awaited by the user.
struct CancelableFuture<T, F> {
    header: Arc<TaskHeader>, // has `cancelled: AtomicBool`
    join: Arc<JoinState<T>>,
    fut: Pin<Box<F>>,
}

impl<T, F> CancelableFuture<T, F>
where
    F: Future<Output = T> + 'static,
{
    pub fn new(header: Arc<TaskHeader>, join: Arc<JoinState<T>>, fut: F) -> Self {
        Self {
            header,
            join,
            fut: Box::pin(fut),
        }
    }
}

impl<T, F> Future for CancelableFuture<T, F>
where
    F: Future<Output = T> + 'static,
{
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        // If already completed (maybe abort() completed join immediately), stop.
        if self.join.is_done() {
            return Poll::Ready(());
        }

        // Cancellation intent is owned by the task header.
        if self.header.is_cancelled() {
            self.join.try_complete_cancelled();
            return Poll::Ready(());
        }

        match self.fut.as_mut().poll(cx) {
            Poll::Ready(out) => {
                self.join.try_complete_ok(out);
                Poll::Ready(())
            }
            Poll::Pending => Poll::Pending,
        }
    }
}

/// Local (executor-thread-only) task record containing the !Send future.
struct TaskRecord {
    header: Arc<TaskHeader>,
    fut: Pin<Box<dyn Future<Output = ()> + 'static>>, // !Send ok
}

/// Global per-queue state maintained by the executor (vruntime/shares).
struct QueueState {
    vruntime: u128, // total CPU time consumed (in nanoseconds)
    share: u64,
    scheduler: Box<dyn Scheduler>,
}

impl QueueState {
    fn new(queue: Queue) -> Self {
        Self {
            vruntime: 0,
            share: queue.share(),
            scheduler: queue.scheduler(),
        }
    }
}
/// The priority executor: single-thread polling + class vruntime selection.
pub struct Executor {
    ingress_tx: Sender<TaskId>,
    ingress_rx: Receiver<TaskId>,

    tasks: RefCell<Slab<TaskRecord>>,
    queues: RefCell<Vec<QueueState>>,
    qids: RefCell<Vec<u8>>,

    sched_latency: Duration,
    min_slice: Duration,
    driver_yield: Duration,
    min_vruntime: std::cell::Cell<u128>,
    num_driver_yields: AtomicU64,
}
assert_not_impl_any!(Executor: Send, Sync);

impl Executor {
    /// Create an executor with N classes, each with a weight (share).
    pub fn new(queues: Vec<Queue>) -> Result<Rc<Self>, String> {
        // verify that all queues have unique ids
        let queue_ids = queues.iter().map(|q| q.id()).collect::<HashSet<_>>();
        if queue_ids.len() != queues.len() {
            return Err("All queues must have unique ids".to_string());
        }
        // no share can be 0
        if queues.iter().any(|q| q.share() == 0) {
            return Err("All queues must have a share > 0".to_string());
        }

        let (tx, rx) = flume::unbounded::<TaskId>();

        let qids = queues.iter().map(|q| q.id() as u8).collect::<Vec<_>>();
        let queues = queues
            .into_iter()
            .map(|q| QueueState::new(q))
            .collect::<Vec<_>>();

        Ok(Rc::new(Self {
            ingress_tx: tx,
            ingress_rx: rx,
            tasks: RefCell::new(Slab::new()),
            queues: RefCell::new(queues),
            qids: RefCell::new(qids),
            // TODO: make these configurable
            sched_latency: Duration::from_millis(2),
            min_slice: Duration::from_micros(100),
            driver_yield: Duration::from_micros(500),
            min_vruntime: std::cell::Cell::new(0),
            num_driver_yields: AtomicU64::new(0),
        }))
    }

    /// Spawn onto a class (queue). Returns a JoinHandle that detaches on drop.
    /// Spawn can only be called from the executor thread.
    pub fn spawn<ID, T, F>(self: &Rc<Self>, qid: ID, fut: F) -> Result<JoinHandle<T>, String>
    where
        T: 'static,
        F: Future<Output = T> + 'static, // !Send ok
        ID: Into<u8>,
    {
        let join = Arc::new(JoinState::<T>::new());

        let mut tasks = self.tasks.borrow_mut();
        let qid = qid.into();
        let Some(_) = self.qids.borrow().iter().position(|q| *q == qid) else {
            return Err(format!("Queue not found for id: {}", qid));
        };
        let entry = tasks.vacant_entry();
        let id = entry.key();
        let header = Arc::new(TaskHeader::new(id, qid, self.ingress_tx.clone()));

        // Wrap user future to publish result into JoinState.
        let wrapped = CancelableFuture::new(header.clone(), join.clone(), fut);

        entry.insert(TaskRecord {
            header: header.clone(),
            fut: Box::pin(wrapped),
        });

        // Enqueue initially.
        header.enqueue();

        Ok(JoinHandle::new(header, join))
    }

    /// Drain ingress notifications and route runnable tasks into their class policies.
    fn drain_ingress_into_classes(&self) {
        while let Ok(id) = self.ingress_rx.try_recv() {
            self.enqueue_task(id);
        }
    }

    fn enqueue_task(&self, id: TaskId) {
        let tasks = self.tasks.borrow();
        let Some(task) = tasks.get(id) else {
            return;
        };
        let qid = task.header.qid();
        let Some(idx) = self.qids.borrow().iter().position(|q| *q == qid) else {
            unreachable!("Queue not found for id: {}", qid);
        };
        let mut queues = self.queues.borrow_mut();
        let queue = &mut queues[idx];
        let is_runnable = queue.scheduler.is_runnable();
        queue.scheduler.push(id);
        // this queue just became runnable, so update its vruntime
        if !is_runnable && queue.scheduler.is_runnable() {
            queue.vruntime = queue.vruntime.max(self.min_vruntime.get());
        }
    }

    /// Pick the next runnable class by deadline among classes that have
    /// runnable tasks. Deadline is vruntime + sched_latency / num_runnable,
    /// so higher weight classes
    /// have lower deadline for the same CPU time, making them preferred.
    fn pick_next_class(&self) -> Option<(usize, Duration)> {
        let mut best: Option<(usize, u128)> = None;
        let num_runnable = self
            .queues
            .borrow()
            .iter()
            .filter(|q| q.scheduler.is_runnable())
            .count();
        if num_runnable == 0 {
            return None;
        }
        let request = self.sched_latency.as_nanos() as u128 / num_runnable as u128;
        let request = request.max(self.min_slice.as_nanos() as u128);
        for (idx, q) in self.queues.borrow().iter().enumerate() {
            if !q.scheduler.is_runnable() {
                continue;
            }
            // d_i = vruntime_i + request / share_i
            let deadline = q.vruntime + (request / q.share as u128);
            match best {
                None => best = Some((idx, deadline)),
                Some((_, bv)) if deadline < bv => best = Some((idx, deadline)),
                _ => {}
            }
        }
        best.map(|(i, _)| (i, Duration::from_nanos(request as u64)))
    }

    /// Charge elapsed CPU time to a class.
    /// We track total CPU time in nanoseconds and compute vruntime on-the-fly
    /// when selecting (total_cpu_nanos / weight), avoiding rounding issues.
    fn charge_class(&self, qidx: usize, elapsed: Duration) {
        let mut queues = self.queues.borrow_mut();
        let queue = &mut queues[qidx];
        // ceil of (elapsed / share)
        let incr = (elapsed.as_nanos() + queue.share as u128 - 1) / (queue.share as u128);
        queue.vruntime += incr;
    }
    fn update_min_vruntime(&self, including: u128) {
        let min_vruntime = self
            .queues
            .borrow()
            .iter()
            .filter(|q| q.scheduler.is_runnable())
            .map(|q| q.vruntime)
            .chain(Some(including))
            .min();
        let min_vruntime = min_vruntime.unwrap();
        // update executor's min_vruntime
        let prev_min_vruntime = self.min_vruntime.get();
        self.min_vruntime.set(prev_min_vruntime.max(min_vruntime));
    }

    /// Run the executor loop forever.
    ///
    /// Panic behavior: if any task panics while being polled, the executor panics (propagates).
    pub async fn run(&self) -> () {
        loop {
            // Always ingest wakeups first.
            self.drain_ingress_into_classes();

            // If nothing runnable, park by awaiting one ingress item.
            if self.pick_next_class().is_none() {
                // park self until new item is enqueued
                match self.ingress_rx.recv_async().await {
                    Ok(id) => {
                        self.enqueue_task(id);
                        continue;
                    }
                    Err(_) => {
                        // sender side dropped + no pending items => we're done
                        break;
                    }
                }
            }
            // Choose class, then choose task within class.
            let (qidx, timeslice) = self.pick_next_class().expect("checked runnable");
            let timeslice = timeslice.min(self.driver_yield);
            let class_end = Instant::now() + timeslice;
            'timeslice: loop {
                set_yield_maybe_deadline(class_end);
                let mut maybe_task = {
                    let mut queues = self.queues.borrow_mut();
                    let queue = &mut queues[qidx];
                    queue.scheduler.pop()
                };

                // Skip dead/stale tasks if policy had tombstones or late notifications.
                let id = loop {
                    let Some(id) = maybe_task else {
                        // Class became empty; re-loop.
                        break None;
                    };
                    let tasks = self.tasks.borrow();
                    let Some(task) = tasks.get(id) else {
                        // Stale id; pick again from same class.
                        drop(tasks);
                        maybe_task = self
                            .queues
                            .borrow_mut()
                            .get_mut(qidx)
                            .unwrap()
                            .scheduler
                            .pop();
                        continue;
                    };
                    if task.header.is_done() {
                        // this is spurious task - we have already done cleanup
                        // before so nothing to do here
                        drop(tasks);
                        let mut queues = self.queues.borrow_mut();
                        let queue = &mut queues[qidx];
                        maybe_task = queue.scheduler.pop();
                        continue;
                    }
                    break Some(id);
                };

                let Some(id) = id else {
                    break 'timeslice;
                };

                // Poll the task once.
                let start = Instant::now();
                let mut tasks = self.tasks.borrow_mut();
                let Some(task) = tasks.get_mut(id) else {
                    continue 'timeslice;
                };

                // Clear queued before polling so a wake during poll can enqueue again.
                task.header.set_queued(false);

                let w = waker(task.header.clone());
                let mut cx = Context::from_waker(&w);

                // NOTE: default abort-on-panic is achieved by *not* catching unwind here.
                let poll = task.fut.as_mut().poll(&mut cx);

                let end = Instant::now();
                let elapsed = end.saturating_duration_since(start);
                self.charge_class(qidx, elapsed);

                match poll {
                    Poll::Ready(()) => {
                        task.header.set_done();
                        tasks.remove(id);
                        let mut queues = self.queues.borrow_mut();
                        let queue = &mut queues[qidx];
                        queue.scheduler.clear_state(id);
                        queue.scheduler.record(id, start, end, true);
                    }
                    // Task is still running, nothing to do.
                    Poll::Pending => {
                        let mut queues = self.queues.borrow_mut();
                        let queue = &mut queues[qidx];
                        queue.scheduler.record(id, start, end, false);
                    }
                };
                if end > class_end {
                    break 'timeslice;
                }
            }
            // compute new min vruntime - note that qid may not be runnable
            // anymore but we do want to include it in the computation
            let new_vruntime = self.queues.borrow()[qidx].vruntime;
            self.update_min_vruntime(new_vruntime);

            // Give the underlying runtime a chance to run its drivers.
            yield_once().await;
            self.num_driver_yields.fetch_add(1, Ordering::Relaxed);
        }
    }
}

pub async fn yield_maybe() {
    let should_yield = YIELD_MAYBE_DEADLINE.with(|d| {
        if let Some(dl) = d.get() {
            Instant::now() >= dl
        } else {
            false
        }
    });
    if should_yield {
        // clear so we don't yield repeatedly in a tight loop
        YIELD_MAYBE_DEADLINE.with(|d| d.set(None));
        yield_once().await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::join::JoinError;
    use crate::queue::FifoQueue;
    use crate::queue::Scheduler;
    use crate::yield_once::yield_once;
    use std::sync::atomic::AtomicBool;
    use std::sync::atomic::{AtomicU32, Ordering};
    use std::sync::{Arc, Mutex};
    use tokio::task::LocalSet;
    use tokio::time::{sleep, timeout, Duration};

    #[tokio::test]
    async fn test_basic_task_completion() {
        let local = LocalSet::new();
        local
            .run_until(async {
                let executor =
                    Executor::new(vec![Queue::new(0, 1, Box::new(FifoQueue::new()))]).unwrap();
                let counter = Arc::new(AtomicU32::new(0));

                let counter_clone = counter.clone();
                let handle = executor.spawn(0, async move {
                    counter_clone.fetch_add(1, Ordering::Relaxed);
                });

                // Run executor in background
                let executor_clone = executor.clone();
                local.spawn_local(async move {
                    executor_clone.run().await;
                });

                // Wait for task to complete
                let result = timeout(Duration::from_millis(100), handle.unwrap()).await;
                assert!(result.is_ok(), "Task should complete");
                assert_eq!(counter.load(Ordering::Relaxed), 1);
            })
            .await;
    }

    #[tokio::test]
    async fn test_join_handle_returns_result() {
        let local = LocalSet::new();
        local
            .run_until(async {
                let executor =
                    Executor::new(vec![Queue::new(0, 1, Box::new(FifoQueue::new()))]).unwrap();

                let handle = executor.spawn(0, async move { 42 });

                let executor_clone = executor.clone();
                local.spawn_local(async move {
                    executor_clone.run().await;
                });

                let result = timeout(Duration::from_millis(100), handle.unwrap()).await;
                assert!(result.is_ok(), "JoinHandle should complete");
                let join_result = result.unwrap();
                assert_eq!(join_result, Ok(42));
            })
            .await;
    }

    #[tokio::test]
    async fn test_join_handle_abort() {
        let local = LocalSet::new();
        local
            .run_until(async {
                let executor =
                    Executor::new(vec![Queue::new(0, 1, Box::new(FifoQueue::new()))]).unwrap();
                let started = Arc::new(AtomicBool::new(false));
                let completed = Arc::new(AtomicBool::new(false));

                let started_clone = started.clone();
                let completed_clone = completed.clone();
                let handle = executor
                    .spawn(0, async move {
                        started_clone.store(true, Ordering::Relaxed);
                        // Task that runs for a while
                        for _ in 0..100 {
                            sleep(Duration::from_millis(10)).await;
                        }
                        completed_clone.store(true, Ordering::Relaxed);
                    })
                    .unwrap();

                let executor_clone = executor.clone();
                local.spawn_local(async move {
                    executor_clone.run().await;
                });

                // Give executor time to start
                sleep(Duration::from_millis(10)).await;

                // Wait a bit for task to start
                sleep(Duration::from_millis(50)).await;
                assert!(started.load(Ordering::Relaxed), "Task should have started");

                // Abort the task
                handle.abort();

                // Wait for abort to be processed
                let result = timeout(Duration::from_millis(500), handle).await;
                assert!(result.is_ok(), "JoinHandle should complete after abort");
                let join_result = result.unwrap();
                assert!(matches!(join_result, Err(JoinError::Cancelled)));

                // Give a bit more time and verify task didn't complete
                sleep(Duration::from_millis(100)).await;
                assert!(
                    !completed.load(Ordering::Relaxed),
                    "Task should not have completed"
                );
            })
            .await;
    }

    #[tokio::test]
    async fn test_vruntime_scheduling() {
        let local = LocalSet::new();
        local
            .run_until(async {
                let executor = Executor::new(vec![
                    Queue::new(0, 8, Box::new(FifoQueue::new())),
                    Queue::new(1, 1, Box::new(FifoQueue::new())),
                ])
                .unwrap();
                let high = Arc::new(AtomicU32::new(0));
                let low = Arc::new(AtomicU32::new(0));

                let high_clone = high.clone();
                let low_clone = low.clone();

                let executor_clone = executor.clone();
                local.spawn_local(async move {
                    executor_clone.run().await;
                });
                // Spawn tasks that run indefinitely with some work per iteration.
                // Note: We use yield_once() instead of sleep() because sleep() makes tasks
                // pending (not runnable), so they can't compete for CPU, thus
                // giving low weight class access to the CPU when high weight
                // class is not runnable.
                let handle1 = executor.spawn(0, async move {
                    loop {
                        for _ in 0..100_000 {
                            high_clone.fetch_add(1, Ordering::Relaxed);
                        }
                        yield_once().await;
                    }
                });
                let handle2 = executor.spawn(1, async move {
                    loop {
                        for _ in 0..100_000 {
                            low_clone.fetch_add(1, Ordering::Relaxed);
                        }
                        yield_once().await;
                    }
                });
                sleep(Duration::from_millis(100)).await;
                let high_count = high.load(Ordering::Relaxed);
                let low_count = low.load(Ordering::Relaxed);
                // High weight class should get more CPU time (roughly 8x)
                assert!(
                    low_count * 4 < high_count && high_count < low_count * 12,
                    "High weight class should get significantly more CPU time. High: {}, Low: {}",
                    high_count,
                    low_count
                );
                handle1.unwrap().abort();
                handle2.unwrap().abort();
            })
            .await;
    }

    #[tokio::test]
    async fn test_policy_fifo_ordering() {
        let local = LocalSet::new();
        local
            .run_until(async {
                let executor =
                    Executor::new(vec![Queue::new(0, 1, Box::new(FifoQueue::new()))]).unwrap();
                let execution_order = Arc::new(Mutex::new(Vec::new()));

                // Spawn multiple tasks that should execute in FIFO order
                for i in 0..5 {
                    let order_clone = execution_order.clone();
                    let _handle = executor.spawn(0, async move {
                        order_clone.lock().unwrap().push(i);
                    });
                }

                let executor_clone = executor.clone();
                local.spawn_local(async move {
                    executor_clone.run().await;
                });

                // Wait for all tasks to complete
                sleep(Duration::from_millis(200)).await;

                let order = execution_order.lock().unwrap();
                // Tasks should execute in FIFO order (0, 1, 2, 3, 4)
                assert_eq!(order.len(), 5, "All tasks should have executed");
                assert_eq!(
                    *order,
                    vec![0, 1, 2, 3, 4],
                    "Tasks should execute in FIFO order"
                );
            })
            .await;
    }

    #[tokio::test]
    async fn test_multiple_tasks_same_class() {
        let local = LocalSet::new();
        local
            .run_until(async {
                let executor =
                    Executor::new(vec![Queue::new(0, 1, Box::new(FifoQueue::new()))]).unwrap();
                let counter = Arc::new(AtomicU32::new(0));

                // Spawn multiple tasks that all increment the counter
                let mut handles = Vec::new();
                for _ in 0..5 {
                    let counter_clone = counter.clone();
                    let handle = executor.spawn(0, async move {
                        counter_clone.fetch_add(1, Ordering::Relaxed);
                    });
                    handles.push(handle);
                }

                let executor_clone = executor.clone();
                local.spawn_local(async move {
                    executor_clone.run().await;
                });

                // Wait for all tasks to complete
                for handle in handles {
                    let result = timeout(Duration::from_millis(100), handle.unwrap()).await;
                    assert!(result.is_ok(), "All tasks should complete");
                }

                assert_eq!(counter.load(Ordering::Relaxed), 5);
            })
            .await;
    }

    #[tokio::test]
    async fn test_task_with_yield() {
        let local = LocalSet::new();
        local
            .run_until(async {
                let executor =
                    Executor::new(vec![Queue::new(0, 1, Box::new(FifoQueue::new()))]).unwrap();
                let counter = Arc::new(AtomicU32::new(0));

                let counter_clone = counter.clone();
                let handle = executor.spawn(0, async move {
                    for _ in 0..3 {
                        counter_clone.fetch_add(1, Ordering::Relaxed);
                        sleep(Duration::from_millis(10)).await;
                    }
                });

                let executor_clone = executor.clone();
                local.spawn_local(async move {
                    executor_clone.run().await;
                });

                // Give executor time to start
                sleep(Duration::from_millis(10)).await;

                let result = timeout(Duration::from_millis(500), handle.unwrap()).await;
                assert!(
                    result.is_ok(),
                    "Task with yields should complete, got {:?}",
                    result
                );
                assert_eq!(counter.load(Ordering::Relaxed), 3);
            })
            .await;
    }

    #[tokio::test]
    async fn test_custom_policy_decision() {
        // Create a custom policy that tracks which tasks are picked
        struct Tracker {
            ids: Vec<TaskId>,
            picked: Arc<Mutex<Vec<(TaskId, bool)>>>,
        }

        impl Scheduler for Tracker {
            fn push(&mut self, id: TaskId) {
                self.ids.push(id);
            }
            fn clear_state(&mut self, _id: TaskId) {}

            fn pop(&mut self) -> Option<TaskId> {
                // pick largest id
                let id = self.ids.iter().max().copied();
                if let Some(id) = id {
                    self.ids.remove(id);
                }
                id
            }

            fn is_runnable(&self) -> bool {
                !self.ids.is_empty()
            }

            fn record(&mut self, id: TaskId, _start: Instant, _end: Instant, ready: bool) {
                self.picked.lock().unwrap().push((id, ready));
            }
        }

        // We can't easily inject a custom policy with the current API,
        // but we can verify that the default FifoPolicy is being used
        // by checking execution order
        let local = LocalSet::new();
        let picked = Arc::new(Mutex::new(Vec::new()));
        local
            .run_until(async {
                let executor = Executor::new(vec![Queue::new(
                    0,
                    1,
                    Box::new(Tracker {
                        ids: Vec::new(),
                        picked: picked.clone(),
                    }),
                )])
                .unwrap();
                // Spawn tasks with different IDs
                for _ in 0..3 {
                    let _handle = executor.spawn(0, async {});
                }

                local.spawn_local(async move {
                    executor.run().await;
                });

                sleep(Duration::from_millis(200)).await;

                let picked = picked.lock().unwrap();
                assert_eq!(picked.len(), 3);
                // verify tasks are in reverse order of their IDs and are all ready
                assert!(picked[0].0 > picked[1].0 && picked[1].0 > picked[2].0);
                assert!(picked.iter().all(|(_, ready)| *ready));
            })
            .await;
    }

    #[tokio::test]
    async fn test_abort_before_task_starts() {
        let local = LocalSet::new();
        local
            .run_until(async {
                let executor =
                    Executor::new(vec![Queue::new(0, 1, Box::new(FifoQueue::new()))]).unwrap();
                let executed = Arc::new(AtomicBool::new(false));

                let executed_clone = executed.clone();
                let handle = executor
                    .spawn(0, async move {
                        executed_clone.store(true, Ordering::Relaxed);
                    })
                    .unwrap();

                // Abort immediately before executor runs
                handle.abort();

                let executor_clone = executor.clone();
                local.spawn_local(async move {
                    executor_clone.run().await;
                });

                // Wait a bit
                sleep(Duration::from_millis(100)).await;

                // Task should not have executed
                assert!(
                    !executed.load(Ordering::Relaxed),
                    "Task should not execute after abort"
                );

                // JoinHandle should return Cancelled
                let result = timeout(Duration::from_millis(50), handle).await;
                assert!(result.is_ok());
                assert!(matches!(result.unwrap(), Err(JoinError::Cancelled)));
            })
            .await;
    }

    #[tokio::test]
    async fn test_enum_queue_ids() {
        enum QueueId {
            High,
            Low,
        }
        impl Into<u8> for QueueId {
            fn into(self) -> u8 {
                match self {
                    QueueId::High => 0,
                    QueueId::Low => 1,
                }
            }
        }
        let local = LocalSet::new();
        local
            .run_until(async {
                let executor = Executor::new(vec![
                    Queue::new(QueueId::High, 1, Box::new(FifoQueue::new())),
                    Queue::new(QueueId::Low, 1, Box::new(FifoQueue::new())),
                ])
                .unwrap();
                let high = Arc::new(AtomicU32::new(0));
                let low = Arc::new(AtomicU32::new(0));

                let high_clone = high.clone();
                let low_clone = low.clone();

                let executor_clone = executor.clone();
                local.spawn_local(async move {
                    executor_clone.run().await;
                });
                let _ = executor.spawn(QueueId::High, async move {
                    high_clone.fetch_add(1, Ordering::Relaxed);
                    yield_once().await;
                });
                let _ = executor.spawn(QueueId::Low, async move {
                    low_clone.fetch_add(1, Ordering::Relaxed);
                    yield_once().await;
                });
                sleep(Duration::from_millis(100)).await;
            })
            .await;
    }

    #[tokio::test]
    async fn test_vruntime_resets() {
        let local = LocalSet::new();
        local
            .run_until(async {
                let executor = Executor::new(vec![
                    Queue::new(0, 1, Box::new(FifoQueue::new())),
                    Queue::new(1, 1, Box::new(FifoQueue::new())),
                ])
                .unwrap();
                let counter = Arc::new(AtomicU32::new(0));
                let counter_clone = counter.clone();
                let _ = executor.spawn(0, async move {
                    for _ in 0..1000 {
                        counter_clone.fetch_add(1, Ordering::Relaxed);
                        yield_once().await;
                    }
                });
                let executor_clone = executor.clone();
                local.spawn_local(async move {
                    executor_clone.run().await;
                });
                sleep(Duration::from_millis(100)).await;
                assert_eq!(counter.load(Ordering::Relaxed), 1000);
                let vruntime1 = executor.queues.borrow()[0].vruntime;
                assert!(vruntime1 > 0);
                // now spawn a task in the second queue
                let counter_clone = counter.clone();
                let _ = executor.spawn(1, async move {
                    counter_clone.fetch_add(1, Ordering::Relaxed);
                });
                sleep(Duration::from_millis(100)).await;
                assert_eq!(counter.load(Ordering::Relaxed), 1001);
                let vruntime2 = executor.queues.borrow()[1].vruntime;
                // even though the second task only ran for a short time
                // its vruntime should have "inherited" the vruntime of the
                // first queue when it started running
                assert!(
                    vruntime2 > vruntime1,
                    "vruntime2 should be greater than vruntime1, got {} and {}",
                    vruntime2,
                    vruntime1
                );
            })
            .await;
    }

    #[tokio::test]
    async fn test_yield_maybe() {
        let local = LocalSet::new();
        local
            .run_until(async {
                let queue = Queue::new(0, 1, Box::new(FifoQueue::new()));
                let executor = Executor::new(vec![queue]).unwrap();
                let executor_clone = executor.clone();
                local.spawn_local(async move {
                    executor_clone.run().await;
                });
                let counter1 = Arc::new(AtomicU32::new(0));
                let counter1_clone = counter1.clone();
                let handle = executor
                    .spawn(0, async move {
                        let mut i = 0;
                        loop {
                            counter1_clone.fetch_add(1, Ordering::Relaxed);
                            if i % 1000 == 0 {
                                yield_maybe().await;
                            }
                            i += 1;
                        }
                    })
                    .unwrap();
                sleep(Duration::from_millis(100)).await;
                let count = counter1.load(Ordering::Relaxed);
                assert!(count > 0);
                let yields = executor.num_driver_yields.load(Ordering::Relaxed);
                assert!(yields > 0);
                // we have yielded at most half the time (in practice much
                // much less)
                assert!(yields < count as u64 / 1000 / 2);
                handle.abort();
            })
            .await;
    }

    // Test with smol runtime
    #[test]
    fn test_smol_runtime() {
        let queue = Queue::new(0, 1, Box::new(FifoQueue::new()));
        let executor = Executor::new(vec![queue]).unwrap();
        let executor_clone = executor.clone();
        let smol_local_ex = smol::LocalExecutor::new();
        let h1 = smol_local_ex.spawn(async move {
            executor_clone.run().await;
        });
        let h2 = smol_local_ex.spawn(async move {
            let handle = executor.spawn(0, async move { 42 }).unwrap();
            handle.await
        });

        let res = smol::future::block_on(smol_local_ex.run(async {
            let res = h2.await;
            drop(h1);
            res
        }));
        assert_eq!(res, Ok(42));
    }

    #[tokio::test]
    async fn test_abort_after_done() {
        let local = LocalSet::new();
        local
            .run_until(async {
                let queue = Queue::new(0, 1, Box::new(FifoQueue::new()));
                let executor = Executor::new(vec![queue]).unwrap();
                let executor_clone = executor.clone();
                local.spawn_local(async move {
                    executor_clone.run().await;
                });
                let counter = Arc::new(AtomicU32::new(0));
                let counter_clone = counter.clone();
                let handle = executor
                    .spawn(0, async move {
                        counter_clone.fetch_add(1, Ordering::Relaxed);
                        42
                    })
                    .unwrap();
                sleep(Duration::from_millis(100)).await;
                assert_eq!(counter.load(Ordering::Relaxed), 1);
                // task is done but abort should still work - no-op
                handle.abort();
                let result = handle.await;
                assert_eq!(result, Ok(42));
            })
            .await;
    }

    // Test with monoio runtime
    #[test]
    fn test_monoio_runtime() {
        use monoio::LegacyDriver;
        let mut rt = monoio::RuntimeBuilder::<LegacyDriver>::new()
            .enable_timer() // Explicitly enable the timer
            .build()
            .unwrap();
        let _ = rt.block_on(async move {
            let queue = Queue::new(0, 1, Box::new(FifoQueue::new()));
            let executor = Executor::new(vec![queue]).unwrap();
            let counter = Arc::new(AtomicU32::new(0));

            let counter_clone = counter.clone();
            let handle = executor
                .spawn(0, async move {
                    counter_clone.fetch_add(1, Ordering::Relaxed);
                    42
                })
                .unwrap();

            // initial value should be 0
            assert_eq!(counter.load(Ordering::Relaxed), 0);
            // Run executor in background
            let executor_clone = executor.clone();
            monoio::spawn(async move {
                executor_clone.run().await;
            });
            monoio::time::sleep(Duration::from_millis(100)).await;
            // task should have completed
            assert_eq!(counter.load(Ordering::Relaxed), 1);
            let result = handle.await;
            assert_eq!(result, Ok(42));
        });
    }
}
