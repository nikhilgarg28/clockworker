use crate::{
    join::{JoinHandle, JoinState},
    mpsc::Mpsc,
    preempt::PreemptState,
    queue::{Queue, QueueKey, TaskId},
    stats::{ExecutorStats, QueueStats},
    task::TaskHeader,
    yield_once::yield_once,
};
use futures::FutureExt;
use futures_util::task::AtomicWaker;
use slab::Slab;
use static_assertions::assert_not_impl_any;
use std::sync::atomic::AtomicBool;
use std::{
    any::Any,
    cell::Cell,
    cell::RefCell,
    future::Future,
    mem,
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
    catch_panics: bool,
}

impl<T, K: QueueKey, F: Future<Output = T> + 'static> CancelableFuture<T, K, F> {
    pub fn new(
        header: Arc<TaskHeader<K>>,
        join: Arc<JoinState<T>>,
        fut: F,
        catch_panics: bool,
    ) -> Self {
        Self {
            header,
            join,
            fut: Box::pin(fut),
            catch_panics,
        }
    }

    fn convert_panic_to_error(
        panic_payload: Box<dyn Any + Send>,
    ) -> Box<dyn std::error::Error + Send> {
        // Try to extract a meaningful error message from the panic
        match panic_payload.downcast::<String>() {
            Ok(msg) => Box::new(std::io::Error::new(
                std::io::ErrorKind::Other,
                format!("Task panicked: {}", msg),
            )) as Box<dyn std::error::Error + Send>,
            Err(payload) => match payload.downcast::<&'static str>() {
                Ok(msg) => Box::new(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    format!("Task panicked: {}", msg),
                )) as Box<dyn std::error::Error + Send>,
                Err(_) => Box::new(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    "Task panicked with unknown payload",
                )) as Box<dyn std::error::Error + Send>,
            },
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

        // Poll with optional panic handling
        let poll_result = if self.catch_panics {
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| self.fut.as_mut().poll(cx)))
        } else {
            Ok(self.fut.as_mut().poll(cx))
        };

        match poll_result {
            Ok(Poll::Ready(out)) => {
                self.join.try_complete_ok(out);
                Poll::Ready(())
            }
            Ok(Poll::Pending) => Poll::Pending,
            Err(panic_payload) => {
                // Convert panic to JoinError::Panic
                let panic_err = Self::convert_panic_to_error(panic_payload);
                self.join
                    .try_complete_err(crate::join::JoinError::Panic(panic_err));
                Poll::Ready(())
            }
        }
    }
}

/// Wrapper to create a waker that sets a flag and wakes an AtomicWaker when woken.
/// Used by `run_until` to detect when the `until` future's dependencies become ready.
struct UntilWakerWrapper {
    woken: Arc<std::sync::atomic::AtomicBool>,
    idle_waker: Arc<futures_util::task::AtomicWaker>,
}

impl futures_util::task::ArcWake for UntilWakerWrapper {
    fn wake_by_ref(arc_self: &Arc<Self>) {
        arc_self.woken.store(true, Ordering::Release);
        arc_self.idle_waker.wake();
    }
}

/// Local (executor-thread-only) task record containing the !Send future.
struct TaskRecord<K: QueueKey> {
    header: Arc<TaskHeader<K>>,
    waker: std::task::Waker,
    fut: Pin<Box<dyn Future<Output = ()> + 'static>>, // !Send ok - type-erased CancelableFuture
}

/// Global per-queue state maintained by the executor (vruntime/shares).
struct QueueState<K: QueueKey> {
    vruntime: u128, // total CPU time consumed (in nanoseconds)
    share: u64,
    mpsc: Arc<Mpsc<TaskId>>,
    stats: QueueStats<K>,
}

impl<K: QueueKey> QueueState<K> {
    fn new(queue: Queue<K>, mpsc: Arc<Mpsc<TaskId>>) -> Self {
        Self {
            vruntime: 0,
            stats: QueueStats::new(queue.id(), queue.share()),
            share: queue.share(),
            mpsc,
        }
    }
}

pub struct QueueHandle<K: QueueKey> {
    executor: Rc<Executor<K>>,
    qid: K,
}
impl<K: QueueKey> QueueHandle<K> {
    pub fn spawn<T, F>(self: &Self, fut: F) -> JoinHandle<T, K>
    where
        T: 'static,
        F: Future<Output = T> + 'static, // !Send ok
    {
        self.executor.spawn_inner(self.qid, fut)
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

    /// Add a queue with FIFO scheduling.
    pub fn with_queue(mut self, qid: K, share: u64) -> Self {
        let queue = Queue::new(qid, share);
        self.queues.push(queue);
        self
    }
    pub fn with_panic_on_task_panic(mut self, panic_on_task_panic: bool) -> Self {
        self.options.panic_on_task_panic = panic_on_task_panic;
        self
    }
    /// Set the maximum number of task polls before yielding to the driver.
    /// This ensures I/O-heavy workloads don't starve the reactor.
    /// Default is 61.
    pub fn with_max_polls_per_yield(mut self, max_polls: u32) -> Self {
        self.options.max_polls_per_yield = max_polls;
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
    panic_on_task_panic: bool,
    /// Maximum number of task polls before yielding to the driver.
    /// This ensures I/O-heavy workloads don't starve the reactor.
    max_polls_per_yield: u32,
}
impl Default for ExecutorOptions {
    fn default() -> Self {
        Self {
            sched_latency: Duration::from_millis(2),
            min_slice: Duration::from_micros(100),
            driver_yield: Duration::from_micros(500),
            panic_on_task_panic: true,
            max_polls_per_yield: 61, // same as tokio
        }
    }
}

/// The priority executor: single-thread polling + class vruntime selection.
pub struct Executor<K: QueueKey> {
    options: ExecutorOptions,
    queue_mpscs: Vec<Arc<Mpsc<TaskId>>>,
    is_runnable: RefCell<Vec<bool>>, // true iff ith queue is runnable

    tasks: RefCell<Slab<TaskRecord<K>>>,
    queues: RefCell<Vec<QueueState<K>>>,
    qids: RefCell<Vec<K>>,

    min_vruntime: std::cell::Cell<u128>,

    /// Shared preemption state - allows wakers to signal when a higher-priority
    /// queue has tasks, enabling early timeslice termination.
    preempt_state: Arc<PreemptState>,

    // stats
    stats: RefCell<ExecutorStats>,
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

        // Create one mpsc channel per queue
        let num_queues = queues.len();
        if num_queues > 256 {
            return Err("Cannot have more than 256 queues (preemption mask limit)".to_string());
        }

        // Create shared preemption state
        let preempt_state = Arc::new(PreemptState::new());

        let queue_mpscs: Vec<Arc<Mpsc<TaskId>>> =
            (0..num_queues).map(|_| Arc::new(Mpsc::new())).collect();

        let qids = queues.iter().map(|q| q.id()).collect::<Vec<_>>();
        let queues: Vec<QueueState<K>> = queues
            .into_iter()
            .enumerate()
            .map(|(idx, q)| QueueState::new(q, queue_mpscs[idx].clone()))
            .collect();

        Ok(Rc::new(Self {
            queue_mpscs,
            is_runnable: RefCell::new(vec![false; num_queues]),
            tasks: RefCell::new(Slab::new()),
            queues: RefCell::new(queues),
            qids: RefCell::new(qids),
            options,
            min_vruntime: std::cell::Cell::new(0),
            preempt_state,
            stats: RefCell::new(ExecutorStats::new(Instant::now())),
        }))
    }

    /// Get a handle to a queue through which tasks can be spawned
    pub fn queue(self: &Rc<Self>, qid: K) -> Result<QueueHandle<K>, SpawnError<K>> {
        let Some(_) = self.qids.borrow().iter().position(|q| *q == qid) else {
            return Err(SpawnError::QueueNotFound(qid));
        };
        Ok(QueueHandle {
            executor: self.clone(),
            qid,
        })
    }

    /// Internal method to spawn a task onto a queue.
    fn spawn_inner<T, F>(self: &Rc<Self>, qid: K, fut: F) -> JoinHandle<T, K>
    where
        T: 'static,
        F: Future<Output = T> + 'static, // !Send ok
    {
        let qid = qid.into();
        let qidx = self
            .qids
            .borrow()
            .iter()
            .position(|q| *q == qid)
            .expect("queue should exist");
        let mut tasks = self.tasks.borrow_mut();
        let entry = tasks.vacant_entry();
        let id = entry.key();
        let header = Arc::new(TaskHeader::new(
            id,
            qid,
            qidx,
            self.queue_mpscs[qidx].clone(),
            self.preempt_state.clone(),
        ));
        let join = Arc::new(JoinState::<T>::new());
        // Wrap user future to publish result into JoinState.
        // catch_panics = !panic_on_task_panic (if executor panics on task panic, we don't catch)
        let catch_panics = !self.options.panic_on_task_panic;
        let wrapped = CancelableFuture::new(header.clone(), join.clone(), fut, catch_panics);

        let waker = futures::task::waker(header.clone());

        entry.insert(TaskRecord {
            header: header.clone(),
            waker,
            fut: Box::pin(wrapped),
        });

        // Enqueue initially.
        header.enqueue();

        JoinHandle::new(header, join)
    }

    /// Pick the next runnable class by deadline among classes that have
    /// runnable tasks. Deadline is vruntime + sched_latency / num_runnable,
    /// so higher weight classes have lower deadline for the same CPU time,
    /// making them preferred.
    ///
    /// Returns (selected_idx, timeslice, selected_deadline, num_runnable) or None if no queues are runnable.
    fn pick_next_class(&self) -> Option<(usize, Duration, u128, usize)> {
        let mut best: Option<(usize, u128)> = None;
        let mut runnable = None;
        let mut num_runnable = 0;
        let mut is_runnable = self.is_runnable.borrow_mut();
        for (idx, q) in self.queues.borrow_mut().iter_mut().enumerate() {
            let was_runnable = is_runnable[idx];
            is_runnable[idx] = !q.mpsc.is_empty();
            if !was_runnable && is_runnable[idx] {
                // wasn't runnable before, but is now - inherit vruntime
                q.vruntime = q.vruntime.max(self.min_vruntime.get());
            }
            if is_runnable[idx] {
                num_runnable += 1;
                runnable = Some(idx);
            }
        }
        if num_runnable == 0 {
            return None;
        }
        let request = self.options.sched_latency.as_nanos() as u128 / num_runnable as u128;
        let request = request.max(self.options.min_slice.as_nanos() as u128);

        if num_runnable == 1 {
            let selected_idx = runnable.unwrap();
            let queues = self.queues.borrow();
            let selected_deadline =
                queues[selected_idx].vruntime + (request / queues[selected_idx].share as u128);
            return Some((
                selected_idx,
                Duration::from_nanos(request as u64),
                selected_deadline,
                num_runnable,
            ));
        }

        // Multiple runnable queues - find the best one
        for (idx, q) in self.queues.borrow().iter().enumerate() {
            if q.mpsc.is_empty() {
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

        let (selected_idx, selected_deadline) = best.unwrap();
        Some((
            selected_idx,
            Duration::from_nanos(request as u64),
            selected_deadline,
            num_runnable,
        ))
    }

    /// Compute and update the preemption mask based on the selected queue.
    /// Empty queues that would have higher priority than the selected queue
    /// are marked in the mask so their wakers can trigger preemption.
    fn update_preempt_mask(&self, selected_deadline: u128, num_runnable: usize) {
        let is_runnable = self.is_runnable.borrow();
        let queues = self.queues.borrow();

        // Calculate hypothetical request if one more queue becomes runnable
        let hypothetical_request =
            self.options.sched_latency.as_nanos() as u128 / (num_runnable + 1) as u128;
        let hypothetical_request =
            hypothetical_request.max(self.options.min_slice.as_nanos() as u128);
        let min_vruntime = self.min_vruntime.get();

        // Find empty queues that would preempt if they got a task
        let preempting = (0..queues.len()).filter(|&idx| {
            if is_runnable[idx] {
                return false; // Already runnable, skip
            }
            // Hypothetical deadline: inherits min_vruntime
            let hypothetical_deadline =
                min_vruntime + (hypothetical_request / queues[idx].share as u128);
            hypothetical_deadline < selected_deadline
        });
        self.preempt_state.update_mask(preempting);
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
            .filter(|q| !q.mpsc.is_empty())
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

    /// Run the executor loop until the given future completes.
    ///
    /// Panic behavior: if any task panics while being polled, the executor
    /// panics (propagates) unless executor has been configured to catch panics
    /// with `with_panic_on_task_panic(false)`.
    ///
    /// The executor will continue running tasks until `until` completes, then
    /// returns. When the executor stops, pending tasks remain pending and will
    /// resume if `run_until` is called again (Tokio-like behavior).
    pub async fn run_until<F: Future>(&self, until: F) -> F::Output {
        let mut until_pinned = std::pin::pin!(until.fuse());

        // Flag that gets set when until's waker is called
        let until_woken = Arc::new(AtomicBool::new(false));
        // Waker to wake the idle wait loop when until_woken is set
        let idle_waker = Arc::new(AtomicWaker::new());
        // Create a waker that sets the flag and wakes idle_waker
        let until_waker = self.create_until_waker(until_woken.clone(), idle_waker.clone());

        let mut last_driver_yield_at = Instant::now();
        let mut iter = 0u64;

        // Initial poll to register our waker
        {
            let mut cx = Context::from_waker(&until_waker);
            if let Poll::Ready(result) = until_pinned.as_mut().poll(&mut cx) {
                return result;
            }
        }

        loop {
            iter += 1;
            let enable_stats = iter % 128 == 0;
            self.stats.borrow_mut().record_loop_iter();

            // Only poll until if it was woken (its dependencies became ready)
            if until_woken.swap(false, Ordering::AcqRel) {
                let mut cx = Context::from_waker(&until_waker);
                if let Poll::Ready(result) = until_pinned.as_mut().poll(&mut cx) {
                    return result;
                }
            }

            // Select next queue to run
            let Some((qidx, timeslice)) = self.select_queue(enable_stats) else {
                // Nothing runnable - wait for work or until_woken signal
                self.wait_for_work_or_signal(&until_woken, &idle_waker)
                    .await;
                continue;
            };

            // Execute timeslice
            let timeslice = timeslice.min(self.options.driver_yield);
            let end = self.run_timeslice(qidx, timeslice, enable_stats);

            // Update executor's min_vruntime
            let new_vruntime = self.queues.borrow()[qidx].vruntime;
            self.update_min_vruntime(new_vruntime);

            // Yield to driver
            last_driver_yield_at = self.yield_to_driver(last_driver_yield_at, end).await;
        }
    }

    /// Create a waker that sets `until_woken` and wakes `idle_waker` when called.
    fn create_until_waker(
        &self,
        until_woken: Arc<std::sync::atomic::AtomicBool>,
        idle_waker: Arc<futures_util::task::AtomicWaker>,
    ) -> std::task::Waker {
        let wrapper = Arc::new(UntilWakerWrapper {
            woken: until_woken,
            idle_waker,
        });
        futures::task::waker(wrapper)
    }

    /// Wait for either a queue to receive a task or `until_woken` to be set.
    /// This is a single poll_fn that registers on all wakers directly, avoiding allocations.
    async fn wait_for_work_or_signal(
        &self,
        until_woken: &Arc<AtomicBool>,
        idle_waker: &Arc<AtomicWaker>,
    ) {
        use futures_util::future::poll_fn;

        poll_fn(|cx| {
            // Check if until was woken
            if until_woken.load(Ordering::Acquire) {
                return Poll::Ready(());
            }

            // Check if any queue has items
            for mpsc in &self.queue_mpscs {
                if !mpsc.is_empty() {
                    return Poll::Ready(());
                }
            }

            // Register our waker with idle_waker (for until_woken signal)
            idle_waker.register(cx.waker());

            // Register our waker with each queue's MPSC
            for mpsc in &self.queue_mpscs {
                mpsc.register_waker(cx.waker());
            }

            // Re-check after registration to avoid missed wakeups
            if until_woken.load(Ordering::Acquire) {
                return Poll::Ready(());
            }
            for mpsc in &self.queue_mpscs {
                if !mpsc.is_empty() {
                    return Poll::Ready(());
                }
            }

            Poll::Pending
        })
        .await
    }

    fn num_live_tasks(&self) -> usize {
        self.queue_mpscs.iter().map(|mpsc| mpsc.len()).sum()
    }

    /// Select the next queue to run and measure the decision time.
    /// Clears the preempt flag when a new timeslice is selected.
    fn select_queue(&self, enable_stats: bool) -> Option<(usize, Duration)> {
        let start = if enable_stats {
            Some(Instant::now())
        } else {
            None
        };

        let Some((selected_idx, timeslice, selected_deadline, num_runnable)) =
            self.pick_next_class()
        else {
            // No runnable queues, clear preempt mask
            self.preempt_state.update_mask(std::iter::empty());
            return None;
        };

        // Clear preempt flag when starting a new timeslice
        self.preempt_state.clear_preempt();

        // Compute which empty queues would preempt the selected queue
        self.update_preempt_mask(selected_deadline, num_runnable);

        if let Some(start) = start {
            let elapsed = Instant::now().duration_since(start);
            self.stats.borrow_mut().record_schedule_decision(elapsed);
        }

        Some((selected_idx, timeslice))
    }

    /// Pop the next valid task from a queue, skipping stale/done tasks.
    /// Returns task id
    fn pop_next_task_from_queue(&self, qidx: usize) -> Option<TaskId> {
        loop {
            let mut queues = self.queues.borrow_mut();
            let queue = &mut queues[qidx];
            // Check if queue was runnable before pop (to detect when it becomes runnable)
            queue.stats.record_runnable_dequeue();
            let maybe_id = queue.mpsc.pop();

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
    fn poll_task(&self, id: TaskId, qidx: usize, start: Instant) -> (bool, Duration) {
        // Extract the future from the task while keeping the task in the Slab.
        // This allows us to release the borrow before polling, enabling nested spawns.
        let (waker, mut extracted_fut) = {
            let mut tasks = self.tasks.borrow_mut();
            let task = match tasks.get_mut(id) {
                Some(task) => task,
                None => return (false, Duration::ZERO),
            };

            // Clear queued before polling so a wake during poll can enqueue again.
            task.header.set_queued(false);

            // Clone what we need for the context
            let waker = task.waker.clone();

            // Extract the future using a dummy placeholder
            // We use futures::future::ready(()) as a placeholder that immediately resolves
            let placeholder = Box::pin(futures::future::ready(()));
            let extracted_fut = mem::replace(&mut task.fut, placeholder);

            (waker, extracted_fut)
        };
        // Borrow is now released - the task remains in the Slab with the placeholder future

        let mut cx = Context::from_waker(&waker);

        // CancelableFuture handles panics internally, so we can poll directly
        // Now we can poll without holding the tasks borrow, allowing nested spawns
        let poll = extracted_fut.as_mut().poll(&mut cx);

        let end = Instant::now();
        let elapsed = end.saturating_duration_since(start);
        self.charge_class(qidx, elapsed);

        // Put the future back (or leave placeholder if task completed)
        {
            let mut tasks = self.tasks.borrow_mut();
            let task = match tasks.get_mut(id) {
                Some(task) => task,
                None => {
                    // Task was removed (shouldn't happen, but handle gracefully)
                    return (false, elapsed);
                }
            };

            match poll {
                Poll::Ready(()) => {
                    // Task completed - leave the placeholder future in place
                    // The task will be removed by complete_task later
                    (true, elapsed)
                }
                Poll::Pending => {
                    // Task still pending - put the future back
                    let placeholder = mem::replace(&mut task.fut, extracted_fut);
                    // Drop the placeholder
                    drop(placeholder);
                    (false, elapsed)
                }
            }
        }
    }

    /// Complete a task that finished (Ready).
    fn complete_task(&self, id: TaskId, _qidx: usize) {
        let mut tasks = self.tasks.borrow_mut();
        let task = tasks.get_mut(id).expect("task should exist");
        task.header.set_done();
        tasks.remove(id);
    }

    /// Execute tasks from a selected queue until the timeslice is exhausted,
    /// preemption is requested, or max polls is reached.
    fn run_timeslice(&self, qidx: usize, timeslice: Duration, enable_stats: bool) -> Instant {
        let now = Instant::now();
        let until = now + timeslice;
        if enable_stats {
            self.queues.borrow_mut()[qidx]
                .stats
                .record_first_service_after_runnable(now);
        }

        let mut start = now;
        let mut polls_this_slice = 0u32;
        let max_polls = self.options.max_polls_per_yield;

        loop {
            set_yield_maybe_deadline(until);

            let Some(id) = self.pop_next_task_from_queue(qidx) else {
                break; // Queue became empty
            };

            let (completed, elapsed) = self.poll_task(id, qidx, start);
            let end = start + elapsed;

            if completed {
                self.complete_task(id, qidx);
            }
            start = end;
            polls_this_slice += 1;

            // Yield to driver after max_polls_per_yield polls to let reactor process I/O
            if polls_this_slice >= max_polls {
                break;
            }

            if end > until {
                if enable_stats {
                    self.stats.borrow_mut().record_poll(elapsed, true);
                    let mut queues = self.queues.borrow_mut();
                    queues[qidx].stats.record_slice_overrun();
                    queues[qidx].stats.record_slice_exhausted();
                }
                break;
            }
            // Check for preemption - a higher priority queue has tasks
            if self.preempt_state.check() {
                break;
            }
        }
        start
    }

    /// Yield to the driver and record stats.
    async fn yield_to_driver(&self, last_yield: Instant, now: Instant) -> Instant {
        let since_last = now - last_yield;
        yield_once().await;
        let after_yield = Instant::now();
        let in_driver = after_yield.duration_since(now);
        self.stats
            .borrow_mut()
            .record_driver_yield(since_last, in_driver);
        after_yield
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
                let executor = ExecutorBuilder::new().with_queue(0, 1).build().unwrap();
                let counter = Arc::new(AtomicU32::new(0));

                let counter_clone = counter.clone();
                // Run executor until task completes
                let result = executor.run_until(async {
                    let queue = executor.queue(0).unwrap();
                    let handle = queue.spawn(async move {
                        counter_clone.fetch_add(1, Ordering::Relaxed);
                    });
                    handle.await
                });
                let result = timeout(Duration::from_millis(100), result).await;
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
                let executor = ExecutorBuilder::new().with_queue(0, 1).build().unwrap();

                let result = executor.run_until(async {
                    let queue = executor.queue(0).unwrap();
                    let handle = queue.spawn(async move { 42 });
                    handle.await
                });
                let result = timeout(Duration::from_millis(100), result).await;
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
                let executor = ExecutorBuilder::new().with_queue(0, 1).build().unwrap();
                let started = Arc::new(AtomicBool::new(false));
                let completed = Arc::new(AtomicBool::new(false));
                let started_clone = started.clone();
                let completed_clone = completed.clone();

                let queue = executor.queue(0).unwrap();
                let handle = executor
                    .run_until(async {
                        let handle = queue.spawn(async move {
                            started_clone.store(true, Ordering::Relaxed);
                            // Task that runs for a while
                            for _ in 0..100 {
                                sleep(Duration::from_millis(10)).await;
                            }
                            completed_clone.store(true, Ordering::Relaxed);
                        });
                        // Wait a bit for task to start
                        sleep(Duration::from_millis(50)).await;
                        assert!(started.load(Ordering::Relaxed), "Task should have started");

                        // Abort the task
                        handle.abort();
                        handle
                    })
                    .await;

                // Wait for abort to be processed
                let result = timeout(Duration::from_millis(500), handle).await;
                assert!(result.is_ok(), "JoinHandle should complete after abort");
                let join_result = result.unwrap();
                assert!(matches!(join_result, Err(JoinError::Cancelled)));

                // verify task didn't complete
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
                let executor = ExecutorBuilder::new()
                    .with_queue(0, 8)
                    .with_queue(1, 1)
                    .build()
                    .unwrap();
                let queue1 = executor.queue(0).unwrap();
                let queue2 = executor.queue(1).unwrap();
                let high = Arc::new(AtomicU32::new(0));
                let low = Arc::new(AtomicU32::new(0));
                let high_clone = high.clone();
                let low_clone = low.clone();

                executor
                    .run_until(async {
                        // Spawn tasks that run indefinitely with some work per iteration.
                        // Note: We use yield_once() instead of sleep() because sleep() makes tasks
                        // pending (not runnable), so they can't compete for CPU, thus
                        // giving low weight class access to the CPU when high weight
                        // class is not runnable.
                        let handle1 = queue1.spawn(async move {
                            loop {
                                for _ in 0..100_000 {
                                    high_clone.fetch_add(1, Ordering::Relaxed);
                                }
                                yield_once().await;
                            }
                        });
                        let handle2 = queue2.spawn(async move {
                            loop {
                                for _ in 0..100_000 {
                                    low_clone.fetch_add(1, Ordering::Relaxed);
                                }
                                yield_once().await;
                            }
                        });
                        sleep(Duration::from_millis(100)).await;
                        handle1.abort();
                        handle2.abort();
                    })
                    .await;
                let high_count = high.load(Ordering::Relaxed);
                let low_count = low.load(Ordering::Relaxed);
                // High weight class should get more CPU time (roughly 8x)
                assert!(
                    low_count * 4 < high_count && high_count < low_count * 12,
                    "High weight class should get significantly more CPU time. High: {}, Low: {}",
                    high_count,
                    low_count
                );
            })
            .await;
    }

    #[tokio::test]
    async fn test_policy_fifo_ordering() {
        let local = LocalSet::new();
        local
            .run_until(async {
                let executor = ExecutorBuilder::new().with_queue(0, 1).build().unwrap();
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
                    // Run until timeout to let tasks complete
                    executor_clone
                        .run_until(sleep(Duration::from_millis(200)))
                        .await;
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
                let executor = ExecutorBuilder::new().with_queue(0, 1).build().unwrap();
                let queue = executor.queue(0).unwrap();
                let counter = Arc::new(AtomicU32::new(0));
                let counter_clone = counter.clone();

                executor
                    .run_until(async {
                        let mut handles = Vec::new();
                        for _ in 0..5 {
                            let counter_clone = counter.clone();
                            let handle = queue.spawn(async move {
                                counter_clone.fetch_add(1, Ordering::Relaxed);
                            });
                            handles.push(handle);
                        }
                        for handle in handles {
                            let result = timeout(Duration::from_millis(100), handle).await;
                            assert!(result.is_ok(), "All tasks should complete");
                        }
                    })
                    .await;
                assert_eq!(counter_clone.load(Ordering::Relaxed), 5);
            })
            .await;
    }

    #[tokio::test]
    async fn test_task_with_yield() {
        let local = LocalSet::new();
        local
            .run_until(async {
                let executor = ExecutorBuilder::new().with_queue(0, 1).build().unwrap();
                let queue = executor.queue(0).unwrap();
                let counter = Arc::new(AtomicU32::new(0));

                let counter_clone = counter.clone();
                executor
                    .run_until(async {
                        let handle = queue.spawn(async move {
                            for _ in 0..3 {
                                counter_clone.fetch_add(1, Ordering::Relaxed);
                                sleep(Duration::from_millis(10)).await;
                            }
                        });
                        let result = timeout(Duration::from_millis(500), handle).await;
                        assert!(
                            result.is_ok(),
                            "Task with yields should complete, got {:?}",
                            result
                        );
                    })
                    .await;

                assert_eq!(counter.load(Ordering::Relaxed), 3);
            })
            .await;
    }

    #[tokio::test]
    async fn test_abort_before_task_starts() {
        let local = LocalSet::new();
        local
            .run_until(async {
                let executor = ExecutorBuilder::new().with_queue(0, 1).build().unwrap();
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
                    executor_clone
                        .run_until(sleep(Duration::from_millis(100)))
                        .await;
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
                let executor = ExecutorBuilder::new()
                    .with_queue(QueueId::High, 1)
                    .with_queue(QueueId::Low, 1)
                    .build()
                    .unwrap();
                let high = Arc::new(AtomicU32::new(0));
                let low = Arc::new(AtomicU32::new(0));

                let high_clone = high.clone();
                let low_clone = low.clone();

                let executor_clone = executor.clone();
                local.spawn_local(async move {
                    executor_clone
                        .run_until(sleep(Duration::from_millis(100)))
                        .await;
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
                let executor = ExecutorBuilder::new()
                    .with_queue(0, 1)
                    .with_queue(1, 1)
                    .build()
                    .unwrap();
                let counter = Arc::new(AtomicU32::new(0));
                let counter_clone = counter.clone();
                let q1 = executor.queue(0).unwrap();
                executor
                    .run_until(async {
                        let handle = q1.spawn(async move {
                            for _ in 0..1000 {
                                counter_clone.fetch_add(1, Ordering::Relaxed);
                                yield_once().await;
                            }
                        });
                        let result = timeout(Duration::from_millis(100), handle).await;
                        assert!(result.is_ok(), "Task should complete");
                        assert_eq!(counter.load(Ordering::Relaxed), 1000);
                        let vruntime1 = executor.queues.borrow()[0].vruntime;
                        assert!(vruntime1 > 0);
                        // now spawn a task in the second queue
                        let counter_clone = counter.clone();
                        let q2 = executor.queue(1).unwrap();
                        let handle = q2.spawn(async move {
                            counter_clone.fetch_add(1, Ordering::Relaxed);
                        });
                        let result = timeout(Duration::from_millis(100), handle).await;
                        assert!(result.is_ok(), "Task should complete");
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
            })
            .await;
    }

    #[tokio::test]
    async fn test_yield_maybe() {
        let local = LocalSet::new();
        local
            .run_until(async {
                let executor = ExecutorBuilder::new().with_queue(0, 1).build().unwrap();
                let queue = executor.queue(0).unwrap();
                let counter1 = Arc::new(AtomicU32::new(0));
                let counter1_clone = counter1.clone();
                local.spawn_local(async move {
                    executor
                        .run_until(async {
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
                });
            })
            .await;
    }

    // Test with smol runtime
    #[test]
    fn test_smol_runtime() {
        let executor = ExecutorBuilder::new().with_queue(0, 1).build().unwrap();
        let smol_local_ex = smol::LocalExecutor::new();
        let h2 = smol_local_ex.spawn(async move {
            let queue = executor.queue(0).unwrap();
            executor
                .run_until(async {
                    let handle = queue.spawn(async move { 42 });
                    handle.await
                })
                .await
        });

        let res = smol::future::block_on(smol_local_ex.run(async { h2.await }));
        assert_eq!(res, Ok(42));
    }

    #[tokio::test]
    async fn test_abort_after_done() {
        let local = LocalSet::new();
        local
            .run_until(async {
                let executor = ExecutorBuilder::new().with_queue(0, 1).build().unwrap();
                let counter = Arc::new(AtomicU32::new(0));
                let counter_clone = counter.clone();
                let queue = executor.queue(0).unwrap();
                let result = executor
                    .run_until(async {
                        let handle = queue.spawn(async move {
                            counter_clone.fetch_add(1, Ordering::Relaxed);
                            42
                        });
                        // wait for task to complete
                        sleep(Duration::from_millis(100)).await;
                        assert!(counter.load(Ordering::Relaxed) > 0);
                        // handle should still be abortable - though no-op
                        handle.abort();
                        handle.await
                    })
                    .await;
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
            let executor = ExecutorBuilder::new().with_queue(0, 1).build().unwrap();
            let counter = Arc::new(AtomicU32::new(0));

            let counter_clone = counter.clone();
            let queue = executor.queue(0).unwrap();
            let result = executor
                .run_until(async {
                    // initial value should be 0
                    assert_eq!(counter.load(Ordering::Relaxed), 0);

                    let handle = queue.spawn(async move {
                        counter_clone.fetch_add(1, Ordering::Relaxed);
                        42
                    });
                    monoio::time::sleep(Duration::from_millis(100)).await;
                    // task should have completed
                    assert_eq!(counter.load(Ordering::Relaxed), 1);
                    handle.await
                })
                .await;
            assert_eq!(result, Ok(42));
        });
    }

    #[test]
    fn test_bad_executor_creation() {
        // can't create executor with 0 shares
        let result = ExecutorBuilder::new().with_queue(0, 0).build();
        assert!(result.is_err());
        // can't create executor with duplicate queue IDs
        let result = ExecutorBuilder::new()
            .with_queue(0, 1)
            .with_queue(0, 1)
            .build();
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
                let executor = ExecutorBuilder::new().with_queue(0, 1).build().unwrap();
                let queue = executor.queue(0).unwrap();
                let handle = tokio::task::spawn_local(async move {
                    executor.run_until(sleep(Duration::from_millis(100))).await;
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

    #[tokio::test]
    async fn test_panic_caught_when_configured() {
        let local = LocalSet::new();
        local
            .run_until(async {
                // Configure executor to catch panics instead of crashing
                let executor = ExecutorBuilder::new()
                    .with_panic_on_task_panic(false)
                    .with_queue(0, 1)
                    .build()
                    .unwrap();
                let queue = executor.queue(0).unwrap();
                let result = executor.run_until(async {
                    let task_handle = queue.spawn(async {
                        panic!("test panic message");
                    });
                    task_handle.await
                });

                // Wait for the task to complete (should complete with Panic error)
                let result = timeout(Duration::from_millis(100), result).await;
                assert!(result.is_ok(), "Task should complete (with panic error)");

                let join_result = result.unwrap();
                assert!(join_result.is_err(), "Task should return an error");

                match join_result.unwrap_err() {
                    JoinError::Panic(_) => {
                        // Expected - panic was caught and converted to JoinError::Panic
                    }
                    other => panic!("Expected JoinError::Panic, got {:?}", other),
                }

                // Executor should still be running (not crashed)
                assert_eq!(
                    executor.num_live_tasks(),
                    0,
                    "Task should be removed after panic"
                );
            })
            .await;
    }

    #[tokio::test]
    async fn test_preemption_mask_computed_correctly() {
        // Test that select_queue computes the preempt mask correctly
        let local = LocalSet::new();
        local
            .run_until(async {
                // Create executor with queues of different weights
                // Queue 0: weight 8 (highest priority when empty has lowest vruntime)
                // Queue 1: weight 4
                // Queue 2: weight 1 (lowest priority)
                let executor = ExecutorBuilder::new()
                    .with_queue(0, 8)
                    .with_queue(1, 4)
                    .with_queue(2, 1)
                    .build()
                    .unwrap();

                let queue2 = executor.queue(2).unwrap();
                let preempt_state = executor.preempt_state.clone();

                executor
                    .run_until(async {
                        // Spawn a task only on queue 2 (lowest priority)
                        let handle = queue2.spawn(async {
                            loop {
                                yield_once().await;
                            }
                        });

                        // Use sleep to allow the executor to run select_queue() and compute the mask.
                        // Sleep gives the tokio driver a chance to run, and when it returns,
                        // the executor will have already run at least one iteration calling select_queue().
                        sleep(Duration::from_millis(10)).await;

                        // At this point, queue 2 should be selected (only runnable)
                        // and queues 0 and 1 should be in the preempt mask since they
                        // would have higher priority if they got tasks
                        assert!(
                            preempt_state.would_preempt(0),
                            "Queue 0 (weight 8) should preempt queue 2 (weight 1)"
                        );
                        assert!(
                            preempt_state.would_preempt(1),
                            "Queue 1 (weight 4) should preempt queue 2 (weight 1)"
                        );
                        assert!(
                            !preempt_state.would_preempt(2),
                            "Queue 2 is runnable, should not be in preempt mask"
                        );
                        assert!(
                            !preempt_state.check(),
                            "Preempt flag should not be set (no higher priority task enqueued)"
                        );

                        handle.abort();
                        let _ = handle.await;
                    })
                    .await;
            })
            .await;
    }
}
