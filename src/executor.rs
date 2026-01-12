use crate::{
    join::{JoinHandle, JoinState},
    queue::{Queue, QueueKey, TaskId},
    scheduler::Scheduler,
    stats::{ExecutorStats, QueueStats},
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
    future::Future,
    hash::{Hash, Hasher},
    num::NonZeroU64,
    pin::Pin,
    rc::Rc,
    sync::atomic::Ordering,
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

fn hash<H: std::hash::Hash>(h: H) -> u64 {
    let mut hasher = ahash::AHasher::default();
    h.hash(&mut hasher);
    hasher.finish()
}

#[derive(Debug)]
pub enum SpawnError<K: QueueKey> {
    ShuttingDown,
    QueueNotFound(K),
    InvalidShare(u64),
}

/// Wraps a user given future to make it cancelable
/// This future only returns () - when the underlying future completes,
/// the result is published to the JoinState, which wrapped by Join Handle
/// can be awaited by the user.
struct CancelableFuture<T, K: QueueKey, F: Future<Output = T> + 'static> {
    header: Arc<TaskHeader<K>>, // has `cancelled: AtomicBool`
    join: Arc<JoinState<T>>,
    fut: Pin<Box<F>>,
}

impl<T, K: QueueKey, F: Future<Output = T> + 'static> CancelableFuture<T, K, F> {
    pub fn new(header: Arc<TaskHeader<K>>, join: Arc<JoinState<T>>, fut: F) -> Self {
        Self {
            header,
            join,
            fut: Box::pin(fut),
        }
    }
}

impl<T, K: QueueKey, F: Future<Output = T> + 'static> Future for CancelableFuture<T, K, F> {
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
struct TaskRecord<K: QueueKey> {
    header: Arc<TaskHeader<K>>,
    fut: Pin<Box<dyn Future<Output = ()> + 'static>>, // !Send ok
}

/// Global per-queue state maintained by the executor (vruntime/shares).
struct QueueState<K: QueueKey> {
    vruntime: u128, // total CPU time consumed (in nanoseconds)
    share: u64,
    scheduler: Box<dyn Scheduler>,
    stats: QueueStats<K>,
}

impl<K: QueueKey> QueueState<K> {
    fn new(queue: Queue<K>) -> Self {
        Self {
            vruntime: 0,
            stats: QueueStats::new(queue.id(), queue.share()),
            share: queue.share(),
            scheduler: queue.scheduler(),
        }
    }
}
/// How the executor should shut down.
#[derive(Clone, Copy, Debug)]
pub enum ShutdownMode {
    /// Stop accepting new tasks; keep running until no tasks remain.
    Drain,
    /// Stop accepting new tasks; keep running until drained or deadline, then force.
    DrainFor(Duration),
    /// Stop accepting new tasks; cancel everything and finish ASAP.
    Force,
}

/// Shared inner state between executor and handles (single-thread; !Send).
#[derive(Debug)]
pub struct ShutdownState {
    requested: Cell<Option<(Instant, ShutdownMode)>>,
    force_initiated: Cell<bool>,
    /// Wakers waiting for shutdown completion.
    waiters: RefCell<Vec<std::task::Waker>>,
    accepting: Cell<bool>,
    /// Set when executor has fully stopped and shutdown is complete.
    stopped: Cell<bool>,
}

impl ShutdownState {
    pub fn new() -> Self {
        Self {
            accepting: Cell::new(true),
            requested: Cell::new(None),
            force_initiated: Cell::new(false),
            waiters: RefCell::new(Vec::new()),
            stopped: Cell::new(false),
        }
    }
    fn request_shutdown(&self, mode: ShutdownMode) {
        if self.requested.get().is_some() {
            return;
        }
        self.requested.set(Some((Instant::now(), mode)));
    }

    // Mark the executor as stopped and wake all waiters.
    fn mark_stopped_and_wake_waiters(&self) {
        self.stopped.set(true);
        for w in self.waiters.borrow_mut().drain(..) {
            w.wake();
        }
    }
    fn requested(&self) -> bool {
        self.requested.get().is_some()
    }
}

/// Await this to learn when shutdown is complete.
/// TODO: should we build this as future on Executor itself?
#[derive(Clone)]
pub struct ShutdownHandle {
    inner: Rc<ShutdownState>,
}

impl Future for ShutdownHandle {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        if self.inner.stopped.get() {
            return Poll::Ready(());
        }
        // Register as waiter
        let mut waiters = self.inner.waiters.borrow_mut();
        // TODO: avoid unbounded growth if polled repeatedly with same waker
        waiters.push(cx.waker().clone());
        Poll::Pending
    }
}
impl ShutdownHandle {
    pub fn new(inner: Rc<ShutdownState>) -> Self {
        Self { inner }
    }
}

pub struct QueueHandle<K: QueueKey> {
    executor: Rc<Executor<K>>,
    qid: K,
    hash: Option<NonZeroU64>,
}
impl<K: QueueKey> QueueHandle<K> {
    pub fn group<H: Hash + std::fmt::Debug>(self: &Self, data: H) -> Self {
        let hash = hash(data);
        // set highest bit of hash to 1
        let hash = hash | 1 << 63;
        Self {
            executor: self.executor.clone(),
            qid: self.qid,
            hash: Some(NonZeroU64::new(hash).unwrap()),
        }
    }
    pub fn spawn<T, F>(self: &Self, fut: F) -> JoinHandle<T, K>
    where
        T: 'static,
        F: Future<Output = T> + 'static, // !Send ok
    {
        let group = match self.hash {
            None => {
                let h = self.executor.next_group_id.get();
                self.executor.next_group_id.set(h + 1);
                // set highest bit of h to 0 to distinguish from user provided groups
                h & !(1 << 63)
            }
            Some(hash) => hash.get() as u64,
        };
        self.executor.spawn_inner(self.qid, group, fut)
    }
}

pub struct ExecutorBuilder<K: QueueKey> {
    options: ExecutorOptions,
    queues: Vec<Queue<K>>,
}
impl<K: QueueKey> ExecutorBuilder<K> {
    pub fn new() -> Self {
        Self {
            options: ExecutorOptions::default(),
            queues: Vec::new(),
        }
    }
    pub fn with_sched_latency(mut self, sched_latency: Duration) -> Self {
        self.options.sched_latency = sched_latency;
        self
    }
    pub fn with_min_slice(mut self, min_slice: Duration) -> Self {
        self.options.min_slice = min_slice;
        self
    }
    pub fn with_driver_yield(mut self, driver_yield: Duration) -> Self {
        self.options.driver_yield = driver_yield;
        self
    }
    pub fn with_queue<S: Scheduler + 'static>(mut self, qid: K, share: u64, scheduler: S) -> Self {
        let scheduler: Box<dyn Scheduler> = Box::new(scheduler);
        let queue = Queue::new(qid, share, scheduler);
        self.queues.push(queue);
        self
    }
    pub fn build(self) -> Result<Rc<Executor<K>>, String> {
        Executor::new(self.options, self.queues)
    }
}

pub struct ExecutorOptions {
    sched_latency: Duration,
    min_slice: Duration,
    driver_yield: Duration,
}
impl Default for ExecutorOptions {
    fn default() -> Self {
        Self {
            sched_latency: Duration::from_millis(2),
            min_slice: Duration::from_micros(100),
            driver_yield: Duration::from_micros(500),
        }
    }
}

/// The priority executor: single-thread polling + class vruntime selection.
pub struct Executor<K: QueueKey> {
    ingress_tx: Sender<TaskId>,
    ingress_rx: Receiver<TaskId>,

    /// Number of live tasks known to the executor.
    live_tasks: std::sync::atomic::AtomicUsize,

    shutdown: Rc<ShutdownState>,

    tasks: RefCell<Slab<TaskRecord<K>>>,
    queues: RefCell<Vec<QueueState<K>>>,
    qids: RefCell<Vec<K>>,

    sched_latency: Duration,
    min_slice: Duration,
    driver_yield: Duration,
    min_vruntime: std::cell::Cell<u128>,
    // stats
    stats: RefCell<ExecutorStats>,

    next_group_id: std::cell::Cell<u64>,
}
assert_not_impl_any!(Executor<u8>: Send, Sync);

impl<K: QueueKey> Executor<K> {
    /// Create an executor with N classes, each with a weight (share).
    pub fn new(options: ExecutorOptions, queues: Vec<Queue<K>>) -> Result<Rc<Self>, String> {
        if queues.is_empty() {
            return Err("Must have at least one queue".to_string());
        }
        // verify that all queues have unique ids
        for i in 0..queues.len() {
            for j in i + 1..queues.len() {
                if queues[i].id() == queues[j].id() {
                    return Err("All queues must have unique ids".to_string());
                }
            }
        }
        // no share can be 0
        if queues.iter().any(|q| q.share() == 0) {
            return Err("All queues must have a share > 0".to_string());
        }

        let (tx, rx) = flume::unbounded::<TaskId>();

        let qids = queues.iter().map(|q| q.id()).collect::<Vec<_>>();
        let queues = queues
            .into_iter()
            .map(|q| QueueState::new(q))
            .collect::<Vec<_>>();

        Ok(Rc::new(Self {
            ingress_tx: tx,
            ingress_rx: rx,
            tasks: RefCell::new(Slab::new()),
            queues: RefCell::new(queues),
            shutdown: Rc::new(ShutdownState::new()),
            qids: RefCell::new(qids),
            live_tasks: std::sync::atomic::AtomicUsize::new(0),
            sched_latency: options.sched_latency,
            min_slice: options.min_slice,
            driver_yield: options.driver_yield,
            min_vruntime: std::cell::Cell::new(0),
            stats: RefCell::new(ExecutorStats::new(Instant::now())),
            next_group_id: std::cell::Cell::new(0),
        }))
    }

    fn should_force_now(&self, now: Instant) -> bool {
        match self.shutdown.requested.get() {
            None => false,
            Some((_, ShutdownMode::Force)) => true,
            Some((asof, ShutdownMode::DrainFor(deadline))) => now - asof >= deadline,
            Some((_, ShutdownMode::Drain)) => false,
        }
    }

    /// Called by executor thread when it wants to force-cancel remaining tasks.
    /// You can implement this using your task table: mark cancelled and enqueue, or just drop tasks.
    fn force_cancel_all_tasks(&self) {
        let tasks = self.tasks.borrow_mut();
        for (_, task) in tasks.iter() {
            task.header.cancel();
            task.header.enqueue();
        }
    }

    /// Get a handle to a queue through which tasks can be spawned
    pub fn queue(self: &Rc<Self>, qid: K) -> Result<QueueHandle<K>, SpawnError<K>> {
        let Some(_) = self.qids.borrow().iter().position(|q| *q == qid) else {
            return Err(SpawnError::QueueNotFound(qid));
        };
        Ok(QueueHandle {
            executor: self.clone(),
            qid,
            hash: None,
        })
    }

    /// Internal method to spawn a task onto a queue.
    fn spawn_inner<T, F>(self: &Rc<Self>, qid: K, group: u64, fut: F) -> JoinHandle<T, K>
    where
        T: 'static,
        F: Future<Output = T> + 'static, // !Send ok
    {
        let qid = qid.into();
        assert!(self.qids.borrow().iter().position(|q| *q == qid).is_some());
        let mut tasks = self.tasks.borrow_mut();
        let entry = tasks.vacant_entry();
        let id = entry.key();
        let header = Arc::new(TaskHeader::new(id, qid, group, self.ingress_tx.clone()));
        let join = Arc::new(JoinState::<T>::new());
        // Wrap user future to publish result into JoinState.
        let wrapped = CancelableFuture::new(header.clone(), join.clone(), fut);

        // if not accepting, don't enqueue, must mark cancelled
        if !self.shutdown.accepting.get() {
            let cancelled = join.try_complete_cancelled();
            assert!(cancelled);
            return JoinHandle::new(header, join);
        }

        entry.insert(TaskRecord {
            header: header.clone(),
            fut: Box::pin(wrapped),
        });
        // increment live tasks
        self.live_tasks.fetch_add(1, Ordering::Relaxed);

        // Enqueue initially.
        header.enqueue();

        JoinHandle::new(header, join)
    }

    /// Drain ingress notifications and route runnable tasks into their class policies.
    fn drain_ingress_into_classes(&self, now: Instant) {
        let mut drained = 0u64;
        while let Ok(id) = self.ingress_rx.try_recv() {
            drained += 1;
            self.enqueue_task(id, now);
        }
        self.stats.borrow_mut().record_wakeups_drained(drained);
    }

    fn enqueue_task(&self, id: TaskId, now: Instant) {
        let tasks = self.tasks.borrow();
        let Some(task) = tasks.get(id) else {
            return;
        };
        let qid = task.header.qid();
        let Some(idx) = self.qids.borrow().iter().position(|q| *q == qid) else {
            unreachable!("Queue not found for id: {:?}", qid);
        };
        let mut queues = self.queues.borrow_mut();
        let queue = &mut queues[idx];
        let was_runnable = queue.scheduler.is_runnable();
        queue.scheduler.push(id, task.header.group(), now);
        let now_runnable = queue.scheduler.is_runnable();
        let became_runnable = !was_runnable && now_runnable;
        // this queue just became runnable, so update its vruntime
        if became_runnable {
            queue.vruntime = queue.vruntime.max(self.min_vruntime.get());
        }
        queue.stats.record_runnable_enqueue(became_runnable, now);
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
        queue.stats.record_poll(elapsed);
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

    /// Get the current executor stats.
    pub fn stats(&self) -> ExecutorStats {
        self.stats.borrow().clone()
    }

    /// Get the current queue stats.
    pub fn qstats(&self) -> Vec<QueueStats<K>> {
        self.queues
            .borrow()
            .iter()
            .map(|q| q.stats.clone())
            .collect()
    }

    /// Run the executor loop forever.
    ///
    /// Panic behavior: if any task panics while being polled, the executor panics (propagates).
    pub async fn run(&self) -> () {
        let mut last_driver_yield_at = Instant::now();

        loop {
            let now = Instant::now();

            // Handle shutdown if needed
            if self.should_force_now(now) && !self.shutdown.force_initiated.get() {
                self.shutdown.force_initiated.set(true);
                self.force_cancel_all_tasks();
            }

            self.stats.borrow_mut().record_loop_iter();
            self.drain_ingress_into_classes(now);

            // Select next queue to run
            let Some((qidx, timeslice)) = self.select_queue() else {
                // Nothing runnable - wait for tasks
                let more = self.wait_for_tasks(now).await;
                if !more {
                    break;
                } else {
                    continue;
                }
            };

            // Execute timeslice
            let timeslice = timeslice.min(self.driver_yield);
            self.run_timeslice(qidx, timeslice);

            // Update executor's min_vruntime
            let new_vruntime = self.queues.borrow()[qidx].vruntime;
            self.update_min_vruntime(new_vruntime);

            // Check shutdown
            if self.shutdown.requested() && self.num_live_tasks() == 0 {
                self.shutdown.mark_stopped_and_wake_waiters();
                break;
            }

            // Yield to driver
            last_driver_yield_at = self.yield_to_driver(last_driver_yield_at).await;
        }
    }

    fn num_live_tasks(&self) -> usize {
        self.live_tasks.load(Ordering::Relaxed)
    }

    /// Wait for new tasks if nothing is runnable.
    /// Returns true if we should continue the loop, false if we should break.
    async fn wait_for_tasks(&self, now: Instant) -> bool {
        let idle_start = Instant::now();
        let recv = self.ingress_rx.recv_async().await;
        let idle_end = Instant::now();
        let idle_duration = idle_end.duration_since(idle_start);
        self.stats.borrow_mut().idle_ns += idle_duration.as_nanos();

        match recv {
            Ok(id) => {
                self.enqueue_task(id, now);
                true // Continue loop
            }
            Err(_) => {
                // Sender dropped + no pending items => we're done
                false // Break loop
            }
        }
    }

    /// Select the next queue to run and measure the decision time.
    fn select_queue(&self) -> Option<(usize, Duration)> {
        let t1 = Instant::now();
        let result = self.pick_next_class();
        let elapsed = Instant::now().duration_since(t1);
        self.stats.borrow_mut().record_schedule_decision(elapsed);
        result
    }

    /// Pop the next valid task from a queue, skipping stale/done tasks.
    fn pop_next_task_from_queue(&self, qidx: usize) -> Option<TaskId> {
        loop {
            let mut queues = self.queues.borrow_mut();
            let queue = &mut queues[qidx];
            queue.stats.record_runnable_dequeue();
            let maybe_id = queue.scheduler.pop();
            drop(queues);

            let Some(id) = maybe_id else {
                return None;
            };

            let tasks = self.tasks.borrow();
            let Some(task) = tasks.get(id) else {
                // Stale id; try again
                continue;
            };

            if task.header.is_done() {
                // Spurious task; try again
                continue;
            }

            return Some(id);
        }
    }

    /// Poll a single task and return whether it completed, the start time, and the elapsed time.
    fn poll_task(&self, id: TaskId, qidx: usize) -> (bool, Instant, Duration) {
        let start = Instant::now();
        let mut tasks = self.tasks.borrow_mut();
        let Some(task) = tasks.get_mut(id) else {
            return (false, start, Duration::ZERO);
        };

        // Clear queued before polling so a wake during poll can enqueue again.
        task.header.set_queued(false);

        let w = waker(task.header.clone());
        let mut cx = Context::from_waker(&w);

        // NOTE: default abort-on-panic is achieved by *not* catching unwind here.
        let poll = task.fut.as_mut().poll(&mut cx);
        drop(tasks);

        let end = Instant::now();
        let elapsed = end.saturating_duration_since(start);
        self.charge_class(qidx, elapsed);

        match poll {
            Poll::Ready(()) => (true, start, elapsed),
            Poll::Pending => (false, start, elapsed),
        }
    }

    /// Complete a task that finished (Ready).
    fn complete_task(&self, id: TaskId, qidx: usize, start: Instant, end: Instant) {
        let mut tasks = self.tasks.borrow_mut();
        let task = tasks.get_mut(id).expect("task should exist");
        let group = task.header.group();
        task.header.set_done();
        tasks.remove(id);

        self.live_tasks.fetch_sub(1, Ordering::Relaxed);

        let mut queues = self.queues.borrow_mut();
        let queue = &mut queues[qidx];
        queue.scheduler.clear_task_state(id, group);
        if group & (1 << 63) == 0 {
            queue.scheduler.clear_group_state(group);
        }
        queue.scheduler.observe(id, group, start, end, true);
    }

    /// Record that a task is still pending.
    fn record_task_pending(&self, id: TaskId, qidx: usize, start: Instant, end: Instant) {
        let mut tasks = self.tasks.borrow_mut();
        let task = tasks.get_mut(id).expect("task should exist");
        let group = task.header.group();
        let mut queues = self.queues.borrow_mut();
        let queue = &mut queues[qidx];
        queue.scheduler.observe(id, group, start, end, false);
    }

    /// Execute tasks from a selected queue until the timeslice is exhausted.
    fn run_timeslice(&self, qidx: usize, timeslice: Duration) {
        let now = Instant::now();
        let until = now + timeslice;
        self.queues.borrow_mut()[qidx]
            .stats
            .record_first_service_after_runnable(now);

        loop {
            set_yield_maybe_deadline(until);

            let Some(id) = self.pop_next_task_from_queue(qidx) else {
                break; // Queue became empty
            };

            let (completed, start, elapsed) = self.poll_task(id, qidx);
            let end = start + elapsed;

            if completed {
                self.complete_task(id, qidx, start, end);
            } else {
                self.record_task_pending(id, qidx, start, end);
            }

            if end > until {
                self.stats.borrow_mut().record_poll(elapsed, true);
                let mut queues = self.queues.borrow_mut();
                queues[qidx].stats.record_slice_overrun();
                queues[qidx].stats.record_slice_exhausted();
                break;
            }
        }
    }

    /// Yield to the driver and record stats.
    async fn yield_to_driver(&self, last_yield: Instant) -> Instant {
        let now = Instant::now();
        let since_last = now - last_yield;
        yield_once().await;
        let after_yield = Instant::now();
        let in_driver = after_yield.duration_since(now);
        self.stats
            .borrow_mut()
            .record_driver_yield(since_last, in_driver);
        after_yield
    }

    pub fn shutdown(&self, mode: ShutdownMode) -> ShutdownHandle {
        self.shutdown.accepting.set(false);
        self.shutdown.request_shutdown(mode);
        ShutdownHandle::new(self.shutdown.clone())
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
    use crate::scheduler::RunnableFifo;
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
                let executor = ExecutorBuilder::new()
                    .with_queue(0, 1, RunnableFifo::new())
                    .build()
                    .unwrap();
                let counter = Arc::new(AtomicU32::new(0));

                let counter_clone = counter.clone();
                let queue = executor.queue(0).unwrap();
                let handle = queue.spawn(async move {
                    counter_clone.fetch_add(1, Ordering::Relaxed);
                });

                // Run executor in background
                let executor_clone = executor.clone();
                local.spawn_local(async move {
                    executor_clone.run().await;
                });

                // Wait for task to complete
                let result = timeout(Duration::from_millis(100), handle).await;
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
                let executor = ExecutorBuilder::new()
                    .with_queue(0, 1, RunnableFifo::new())
                    .build()
                    .unwrap();

                let queue = executor.queue(0).unwrap();
                let handle = queue.spawn(async move { 42 });

                let executor_clone = executor.clone();
                local.spawn_local(async move {
                    executor_clone.run().await;
                });

                let result = timeout(Duration::from_millis(100), handle).await;
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
                let executor = ExecutorBuilder::new()
                    .with_queue(0, 1, RunnableFifo::new())
                    .build()
                    .unwrap();
                let started = Arc::new(AtomicBool::new(false));
                let completed = Arc::new(AtomicBool::new(false));

                let started_clone = started.clone();
                let completed_clone = completed.clone();
                let queue = executor.queue(0).unwrap();
                let handle = queue.spawn(async move {
                    started_clone.store(true, Ordering::Relaxed);
                    // Task that runs for a while
                    for _ in 0..100 {
                        sleep(Duration::from_millis(10)).await;
                    }
                    completed_clone.store(true, Ordering::Relaxed);
                });

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
                let opts = ExecutorOptions::default();
                let executor = Executor::new(
                    opts,
                    vec![
                        Queue::new(0, 8, Box::new(RunnableFifo::new())),
                        Queue::new(1, 1, Box::new(RunnableFifo::new())),
                    ],
                )
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
                let queue1 = executor.queue(0).unwrap();
                let handle1 = queue1.spawn(async move {
                    loop {
                        for _ in 0..100_000 {
                            high_clone.fetch_add(1, Ordering::Relaxed);
                        }
                        yield_once().await;
                    }
                });
                let queue2 = executor.queue(1).unwrap();
                let handle2 = queue2.spawn(async move {
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
                handle1.abort();
                handle2.abort();
            })
            .await;
    }

    #[tokio::test]
    async fn test_policy_fifo_ordering() {
        let local = LocalSet::new();
        local
            .run_until(async {
                let opts = ExecutorOptions::default();
                let executor =
                    Executor::new(opts, vec![Queue::new(0, 1, Box::new(RunnableFifo::new()))])
                        .unwrap();
                let queue = executor.queue(0).unwrap();
                let execution_order = Arc::new(Mutex::new(Vec::new()));

                // Spawn multiple tasks that should execute in FIFO order
                for i in 0..5 {
                    let order_clone = execution_order.clone();
                    let _handle = queue.spawn(async move {
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
                let opts = ExecutorOptions::default();
                let executor =
                    Executor::new(opts, vec![Queue::new(0, 1, Box::new(RunnableFifo::new()))])
                        .unwrap();
                let queue = executor.queue(0).unwrap();
                let counter = Arc::new(AtomicU32::new(0));

                // Spawn multiple tasks that all increment the counter
                let mut handles = Vec::new();
                for _ in 0..5 {
                    let counter_clone = counter.clone();
                    let handle = queue.spawn(async move {
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
                    let result = timeout(Duration::from_millis(100), handle).await;
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
                let opts = ExecutorOptions::default();
                let executor =
                    Executor::new(opts, vec![Queue::new(0, 1, Box::new(RunnableFifo::new()))])
                        .unwrap();
                let queue = executor.queue(0).unwrap();
                let counter = Arc::new(AtomicU32::new(0));

                let counter_clone = counter.clone();
                let handle = queue.spawn(async move {
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

                let result = timeout(Duration::from_millis(500), handle).await;
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
            enqueued: Arc<Mutex<Vec<(u64, TaskId)>>>,
            picked: Arc<Mutex<Vec<(TaskId, bool)>>>,
        }

        impl Scheduler for Tracker {
            fn push(&mut self, id: TaskId, group: u64, _at: Instant) {
                self.ids.push(id);
                self.enqueued.lock().unwrap().push((group, id));
            }
            fn clear_task_state(&mut self, _id: TaskId, _group: u64) {}
            fn clear_group_state(&mut self, _group: u64) {}

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

            fn observe(
                &mut self,
                id: TaskId,
                _gid: u64,
                _start: Instant,
                _end: Instant,
                ready: bool,
            ) {
                self.picked.lock().unwrap().push((id, ready));
            }
        }

        // We can't easily inject a custom policy with the current API,
        // but we can verify that the default FifoPolicy is being used
        // by checking execution order
        let local = LocalSet::new();
        let enqueued = Arc::new(Mutex::new(Vec::new()));
        let picked = Arc::new(Mutex::new(Vec::new()));
        local
            .run_until(async {
                let opts = ExecutorOptions::default();
                let executor = Executor::new(
                    opts,
                    vec![Queue::new(
                        0,
                        1,
                        Box::new(Tracker {
                            ids: Vec::new(),
                            picked: picked.clone(),
                            enqueued: enqueued.clone(),
                        }),
                    )],
                )
                .unwrap();
                let queue = executor.queue(0).unwrap();
                // Spawn tasks with different IDs
                for i in 0..3 {
                    let _handle = queue.group(i % 2).spawn(async {});
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
                // also verify first and third tasks are in the same group but
                // second task is in a different group
                let enqueued = enqueued.lock().unwrap();
                assert_eq!(enqueued.len(), 3);
                assert_eq!(enqueued[0].0, enqueued[2].0);
                assert_ne!(enqueued[0].0, enqueued[1].0);
            })
            .await;
    }

    #[tokio::test]
    async fn test_abort_before_task_starts() {
        let local = LocalSet::new();
        local
            .run_until(async {
                let opts = ExecutorOptions::default();
                let executor =
                    Executor::new(opts, vec![Queue::new(0, 1, Box::new(RunnableFifo::new()))])
                        .unwrap();
                let queue = executor.queue(0).unwrap();
                let executed = Arc::new(AtomicBool::new(false));

                let executed_clone = executed.clone();
                let handle = queue.spawn(async move {
                    executed_clone.store(true, Ordering::Relaxed);
                });

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
        #[derive(Debug, PartialEq, Eq, Hash, Copy, Clone)]
        enum QueueId {
            High,
            Low,
        }
        let local = LocalSet::new();
        local
            .run_until(async {
                let opts = ExecutorOptions::default();
                let executor = Executor::new(
                    opts,
                    vec![
                        Queue::new(QueueId::High, 1, Box::new(RunnableFifo::new())),
                        Queue::new(QueueId::Low, 1, Box::new(RunnableFifo::new())),
                    ],
                )
                .unwrap();
                let high = Arc::new(AtomicU32::new(0));
                let low = Arc::new(AtomicU32::new(0));

                let high_clone = high.clone();
                let low_clone = low.clone();

                let executor_clone = executor.clone();
                local.spawn_local(async move {
                    executor_clone.run().await;
                });
                let q1 = executor.queue(QueueId::High).unwrap();
                let _ = q1.spawn(async move {
                    high_clone.fetch_add(1, Ordering::Relaxed);
                    yield_once().await;
                });
                let q2 = executor.queue(QueueId::Low).unwrap();
                let _ = q2.spawn(async move {
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
                let opts = ExecutorOptions::default();
                let executor = Executor::new(
                    opts,
                    vec![
                        Queue::new(0, 1, Box::new(RunnableFifo::new())),
                        Queue::new(1, 1, Box::new(RunnableFifo::new())),
                    ],
                )
                .unwrap();
                let counter = Arc::new(AtomicU32::new(0));
                let counter_clone = counter.clone();
                let q1 = executor.queue(0).unwrap();
                let _ = q1.spawn(async move {
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
                let q2 = executor.queue(1).unwrap();
                let _ = q2.spawn(async move {
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
                let opts = ExecutorOptions::default();
                let executor =
                    Executor::new(opts, vec![Queue::new(0, 1, Box::new(RunnableFifo::new()))])
                        .unwrap();
                let queue = executor.queue(0).unwrap();
                let executor_clone = executor.clone();
                local.spawn_local(async move {
                    executor_clone.run().await;
                });
                let counter1 = Arc::new(AtomicU32::new(0));
                let counter1_clone = counter1.clone();
                let handle = queue.spawn(async move {
                    let mut i = 0;
                    loop {
                        counter1_clone.fetch_add(1, Ordering::Relaxed);
                        if i % 1000 == 0 {
                            yield_maybe().await;
                        }
                        i += 1;
                    }
                });
                sleep(Duration::from_millis(100)).await;
                let count = counter1.load(Ordering::Relaxed);
                assert!(count > 0);
                let yields = executor.stats.borrow().driver_yields;
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
        let queue = Queue::new(0, 1, Box::new(RunnableFifo::new()));
        let opts = ExecutorOptions::default();
        let executor = Executor::new(opts, vec![queue]).unwrap();
        let executor_clone = executor.clone();
        let smol_local_ex = smol::LocalExecutor::new();
        let h1 = smol_local_ex.spawn(async move {
            executor_clone.run().await;
        });
        let h2 = smol_local_ex.spawn(async move {
            let queue = executor.queue(0).unwrap();
            let handle = queue.spawn(async move { 42 });
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
                let opts = ExecutorOptions::default();
                let executor =
                    Executor::new(opts, vec![Queue::new(0, 1, Box::new(RunnableFifo::new()))])
                        .unwrap();
                let executor_clone = executor.clone();
                local.spawn_local(async move {
                    executor_clone.run().await;
                });
                let counter = Arc::new(AtomicU32::new(0));
                let counter_clone = counter.clone();
                let queue = executor.queue(0).unwrap();
                let handle = queue.spawn(async move {
                    counter_clone.fetch_add(1, Ordering::Relaxed);
                    42
                });
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
            let opts = ExecutorOptions::default();
            let queue = Queue::new(0, 1, Box::new(RunnableFifo::new()));
            let executor = Executor::new(opts, vec![queue]).unwrap();
            let counter = Arc::new(AtomicU32::new(0));

            let counter_clone = counter.clone();
            let queue = executor.queue(0).unwrap();
            let handle = queue.spawn(async move {
                counter_clone.fetch_add(1, Ordering::Relaxed);
                42
            });

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

    #[tokio::test]
    async fn test_force_shutdown() {
        // verify happy path - shutdown returns a handle that is awaitable
        // also spawns aren't allowed after shutdown
        // and when using force shutdown, all tasks are cancelled
        let local = LocalSet::new();
        local
            .run_until(async {
                let opts = ExecutorOptions::default();
                let executor =
                    Executor::new(opts, vec![Queue::new(0, 1, Box::new(RunnableFifo::new()))])
                        .unwrap();
                let executor_clone = executor.clone();
                local.spawn_local(async move {
                    executor_clone.run().await;
                });
                let counter = Arc::new(AtomicU32::new(0));
                let counter_clone = counter.clone();
                // spawn a task that runs forever
                let queue = executor.queue(0).unwrap();
                let task = queue.spawn(async move {
                    loop {
                        counter_clone.fetch_add(1, Ordering::Relaxed);
                        sleep(Duration::from_millis(100)).await;
                    }
                });
                // sleep a bit to let the task start
                sleep(Duration::from_millis(100)).await;
                assert!(counter.load(Ordering::Relaxed) > 0);
                assert_eq!(executor.num_live_tasks(), 1);
                // shutdown with drain mode
                let shutdown_handle = executor.shutdown(ShutdownMode::Force);
                // can't spawn after shutdown
                let queue = executor.queue(0).unwrap();
                let result = queue.spawn(async move { 42 });
                // this should be cancelled immediately
                let result = timeout(Duration::from_millis(1), result).await;
                assert!(result.is_ok());
                assert!(matches!(result.unwrap(), Err(JoinError::Cancelled)));
                // await shutdown handle
                shutdown_handle.await;
                // all tasks should be cancelled
                assert_eq!(executor.num_live_tasks(), 0);
                // and await on task works
                let result = task.await;
                assert!(matches!(result, Err(JoinError::Cancelled)));
            })
            .await;
    }

    #[tokio::test]
    async fn test_drain_timeout() {
        let local = LocalSet::new();
        local
            .run_until(async {
                let opts = ExecutorOptions::default();
                let executor =
                    Executor::new(opts, vec![Queue::new(0, 1, Box::new(RunnableFifo::new()))])
                        .unwrap();
                let executor_clone = executor.clone();
                local.spawn_local(async move {
                    executor_clone.run().await;
                });
                let counter = Arc::new(AtomicU32::new(0));
                let counter_clone = counter.clone();
                // spawn a task that runs forever
                let queue = executor.queue(0).unwrap();
                let task = queue.spawn(async move {
                    loop {
                        counter_clone.fetch_add(1, Ordering::Relaxed);
                        sleep(Duration::from_millis(100)).await;
                    }
                });
                // sleep a bit to let the task start
                sleep(Duration::from_millis(100)).await;
                let count = counter.load(Ordering::Relaxed);
                assert!(count > 0);
                assert_eq!(executor.num_live_tasks(), 1);
                // shutdown with drain mode
                let shutdown_handle =
                    executor.shutdown(ShutdownMode::DrainFor(Duration::from_secs(1)));
                // can't spawn after shutdown
                let queue = executor.queue(0).unwrap();
                let result = queue.spawn(async move { 42 });
                // join handle should be cancelled immediately
                let result = timeout(Duration::from_millis(1), result).await;
                assert!(result.is_ok());
                assert!(matches!(result.unwrap(), Err(JoinError::Cancelled)));
                // sleep for 100 ms - task should still be running
                sleep(Duration::from_millis(100)).await;
                assert!(counter.load(Ordering::Relaxed) > count);
                assert_eq!(executor.num_live_tasks(), 1);
                // if we try to await shutdown or task handle with timeout,
                // it should timeout
                let result = timeout(Duration::from_millis(100), shutdown_handle.clone()).await;
                assert!(result.is_err());
                let result = timeout(Duration::from_millis(100), task.clone()).await;
                assert!(result.is_err());
                // but it should be done within 1 second
                let result = timeout(Duration::from_secs(1), shutdown_handle).await;
                assert!(result.is_ok());
                assert_eq!(executor.num_live_tasks(), 0);
                // and task should be cancelled
                let result = timeout(Duration::from_millis(1), task).await;
                let result = result.unwrap();
                assert_eq!(result, Err(JoinError::Cancelled));
            })
            .await;
    }

    #[tokio::test]
    async fn test_drain_shutdown() {
        let local = LocalSet::new();
        local
            .run_until(async {
                let opts = ExecutorOptions::default();
                let executor =
                    Executor::new(opts, vec![Queue::new(0, 1, Box::new(RunnableFifo::new()))])
                        .unwrap();
                let executor_clone = executor.clone();
                local.spawn_local(async move {
                    executor_clone.run().await;
                });
                let counter = Arc::new(AtomicU32::new(0));
                let counter_clone = counter.clone();
                // spawn a task that runs forever
                let queue = executor.queue(0).unwrap();
                let task = queue.spawn(async move {
                    counter_clone.fetch_add(1, Ordering::Relaxed);
                    sleep(Duration::from_millis(100)).await;
                    counter_clone.fetch_add(1, Ordering::Relaxed);
                    sleep(Duration::from_millis(100)).await;
                    counter_clone.fetch_add(1, Ordering::Relaxed);
                    42
                });
                // wait just a bit to let the task start
                sleep(Duration::from_millis(10)).await;
                // verify task has started
                assert_eq!(executor.num_live_tasks(), 1);
                assert!(counter.load(Ordering::Relaxed) == 1);
                // now initiate shutdown
                let shutdown_handle = executor.shutdown(ShutdownMode::Drain);
                // await shutdown handle
                let result = timeout(Duration::from_secs(1), shutdown_handle.clone()).await;
                assert!(result.is_ok());
                // all tasks should be cancelled
                assert_eq!(executor.num_live_tasks(), 0);
                // more importantly though the task should have completed
                assert_eq!(counter.load(Ordering::Relaxed), 3);
                let result = timeout(Duration::from_millis(1), task).await;
                let result = result.unwrap();
                assert_eq!(result, Ok(42));
            })
            .await;
    }

    #[test]
    fn test_bad_executor_creation() {
        // can't create executor with 0 shares
        let result = Executor::new(
            ExecutorOptions::default(),
            vec![Queue::new(0, 0, Box::new(RunnableFifo::new()))],
        );
        assert!(result.is_err());
        // can't create executor with duplicate queue IDs
        let result = Executor::new(
            ExecutorOptions::default(),
            vec![
                Queue::new(0, 1, Box::new(RunnableFifo::new())),
                Queue::new(0, 1, Box::new(RunnableFifo::new())),
            ],
        );
        assert!(result.is_err());
        // can't create executor with 0 queues
        let result = Executor::<u8>::new(ExecutorOptions::default(), vec![]);
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_panic_crashes_executor() {
        let local = LocalSet::new();
        local
            .run_until(async {
                let opts = ExecutorOptions::default();
                let executor = Executor::<u8>::new(
                    opts,
                    vec![Queue::new(0, 1, Box::new(RunnableFifo::new()))],
                )
                .unwrap();
                let queue = executor.queue(0).unwrap();
                let handle = tokio::task::spawn_local(async move {
                    executor.run().await;
                });
                let _ = queue.spawn(async {
                    panic!("test");
                });
                let result = handle.await;
                assert!(result.is_err());
                assert!(result.unwrap_err().is_panic());
            })
            .await;
    }
}
