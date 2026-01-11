use std::time::{Duration, Instant};

/// Fixed log2 histogram for durations in nanoseconds.
/// Buckets: [0..1ns], [1..2ns], [2..4ns], ..., up to 2^63 ns (~292 years).
#[derive(Clone, Copy, Debug)]
pub struct Log2Histogram {
    pub buckets: [u64; 64],
    pub count: u64,
    pub sum_ns: u128,
    pub max_ns: u64,
}

impl Log2Histogram {
    pub const fn new() -> Self {
        Self {
            buckets: [0; 64],
            count: 0,
            sum_ns: 0,
            max_ns: 0,
        }
    }

    #[inline]
    pub fn record_ns(&mut self, ns: u64) {
        let idx = if ns == 0 {
            0
        } else {
            63 - ns.leading_zeros() as usize
        };
        self.buckets[idx] += 1;
        self.count += 1;
        self.sum_ns += ns as u128;
        if ns > self.max_ns {
            self.max_ns = ns;
        }
    }

    #[inline]
    pub fn record_duration(&mut self, d: Duration) {
        self.record_ns(d.as_nanos().min(u128::from(u64::MAX)) as u64);
    }
}

/// Executor-wide stats.
/// Single-threaded: these can be plain u64s. If you later go multi-thread,
/// either shard stats per worker or switch to atomics behind a feature flag.
#[derive(Clone, Debug)]
pub struct ExecutorStats {
    // Lifecycle / time accounting
    pub started_at: Instant,
    pub busy_ns: u128,   // time spent polling tasks + scheduling overhead
    pub idle_ns: u128,   // time spent with nothing runnable (optional)
    pub driver_ns: u128, // time spent yielding to driver/reactor (if measurable)

    // Loop + wakeups
    pub loop_iters: u64,
    pub wakeups_drained: u64, // total wake events pulled from ingress
    pub wakeups_dropped: u64, // if you coalesce/dedupe and drop duplicates

    // Polling
    pub polls: u64,             // number of task polls executed
    pub poll_ns: Log2Histogram, // duration of each poll() call (or each poll segment)
    pub polls_over_budget: u64, // poll took longer than configured max_poll_ns (or remaining budget)
    pub preempt_requests: u64,  // executor requested cooperative preemption (set deadline/fuel)

    // Scheduler decisions
    pub schedule_decisions: u64, // number of "pick next queue/task" decisions
    pub decision_ns: Log2Histogram, // time spent choosing next queue/task (optional but useful)

    // Driver yields (reactor fairness)
    pub driver_yields: u64,
    pub between_driver_yields_ns: Log2Histogram, // spacing between yields (measure in executor)
    pub max_time_without_driver_yield_ns: u64,

    // Gauges (sampled occasionally)
    pub runnable_queues_samples: u64,
    pub runnable_queues_sum: u64,
    pub runnable_tasks_samples: u64,
    pub runnable_tasks_sum: u64,
}

impl ExecutorStats {
    pub fn new(now: Instant) -> Self {
        Self {
            started_at: now,
            busy_ns: 0,
            idle_ns: 0,
            driver_ns: 0,
            loop_iters: 0,
            wakeups_drained: 0,
            wakeups_dropped: 0,
            polls: 0,
            poll_ns: Log2Histogram::new(),
            polls_over_budget: 0,
            preempt_requests: 0,
            schedule_decisions: 0,
            decision_ns: Log2Histogram::new(),
            driver_yields: 0,
            between_driver_yields_ns: Log2Histogram::new(),
            max_time_without_driver_yield_ns: 0,
            runnable_queues_samples: 0,
            runnable_queues_sum: 0,
            runnable_tasks_samples: 0,
            runnable_tasks_sum: 0,
        }
    }

    #[inline]
    pub fn record_loop_iter(&mut self) {
        self.loop_iters += 1;
    }

    #[inline]
    pub fn record_wakeups_drained(&mut self, n: u64) {
        self.wakeups_drained += n;
    }

    #[inline]
    pub fn record_poll(&mut self, dur: Duration, over_budget: bool) {
        self.polls += 1;
        self.poll_ns.record_duration(dur);
        if over_budget {
            self.polls_over_budget += 1;
        }
    }

    #[inline]
    pub fn record_preempt_request(&mut self) {
        self.preempt_requests += 1;
    }

    #[inline]
    pub fn record_schedule_decision(&mut self, dur: Duration) {
        self.schedule_decisions += 1;
        self.decision_ns.record_duration(dur);
    }

    /// Call when you yield to the driver/reactor.
    /// `since_last_yield` should be measured by executor.
    #[inline]
    pub fn record_driver_yield(&mut self, since_last_yield: Duration, in_driver: Duration) {
        self.driver_yields += 1;
        let ns = since_last_yield.as_nanos().min(u128::from(u64::MAX)) as u64;
        self.between_driver_yields_ns.record_ns(ns);
        if ns > self.max_time_without_driver_yield_ns {
            self.max_time_without_driver_yield_ns = ns;
        }
        self.driver_ns += in_driver.as_nanos();
    }

    /// Sample runnable counts (cheap; do it every N iters).
    #[inline]
    pub fn sample_runnable(&mut self, runnable_queues: u64, runnable_tasks: u64) {
        self.runnable_queues_samples += 1;
        self.runnable_queues_sum += runnable_queues;
        self.runnable_tasks_samples += 1;
        self.runnable_tasks_sum += runnable_tasks;
    }
}

/// Per-queue stats
#[derive(Clone, Debug)]
pub struct QueueStats {
    pub id: u8,

    // Weight/policy metadata (optional but useful to export)
    pub weight: u64,

    // Service accounting (ground truth)
    pub service_ns: u128,       // total time spent polling tasks from this queue
    pub polls: u64,             // number of task polls from this queue
    pub poll_ns: Log2Histogram, // per-poll duration when running tasks from this queue

    // Runnable backlog / flow
    pub runnable_enqueued: u64, // wake events assigned to this queue
    pub runnable_dequeued: u64, // tasks popped to poll
    pub runnable_len_samples: u64,
    pub runnable_len_sum: u64,
    pub runnable_len_max: u64,

    // Queueing delay (queue became runnable -> first service)
    pub became_runnable: u64, // number of not-runnable -> runnable transitions
    pub queue_delay_ns: Log2Histogram, // delay until first poll after transition

    // Slice / budget usage
    pub slice_exhaustions: u64,
    pub slice_overruns: u64,    // e.g., single poll blew past remaining slice
    pub preempt_requested: u64, // preempt signaled while running this queue

    // Starvation-ish
    pub max_runnable_without_service_ns: u64,

    // Internal state for measuring queue delay (kept inside stats for convenience)
    last_became_runnable_at: Option<Instant>,
    last_serviced_at: Option<Instant>,
}

impl QueueStats {
    pub fn new(id: u8, weight: u64) -> Self {
        Self {
            id,
            weight: weight as u64,
            service_ns: 0,
            polls: 0,
            poll_ns: Log2Histogram::new(),
            runnable_enqueued: 0,
            runnable_dequeued: 0,
            runnable_len_samples: 0,
            runnable_len_sum: 0,
            runnable_len_max: 0,
            became_runnable: 0,
            queue_delay_ns: Log2Histogram::new(),
            slice_exhaustions: 0,
            slice_overruns: 0,
            preempt_requested: 0,
            max_runnable_without_service_ns: 0,
            last_became_runnable_at: None,
            last_serviced_at: None,
        }
    }

    #[inline]
    pub fn record_runnable_enqueue(&mut self, became_runnable: bool, now: Instant) {
        self.runnable_enqueued += 1;
        if became_runnable {
            self.became_runnable += 1;
            self.last_became_runnable_at = Some(now);
        }
    }

    #[inline]
    pub fn record_runnable_dequeue(&mut self) {
        self.runnable_dequeued += 1;
    }

    /// Sample runnable queue length (do this occasionally, not per op).
    #[inline]
    pub fn sample_runnable_len(&mut self, len: u64) {
        self.runnable_len_samples += 1;
        self.runnable_len_sum += len;
        if len > self.runnable_len_max {
            self.runnable_len_max = len;
        }
    }

    /// Call around each poll of a task from this queue.
    #[inline]
    pub fn record_poll(&mut self, dur: Duration) {
        self.polls += 1;
        self.poll_ns.record_duration(dur);
        self.service_ns += dur.as_nanos();
    }

    /// Call when you are about to run the first task after the queue became runnable.
    #[inline]
    pub fn record_first_service_after_runnable(&mut self, now: Instant) {
        if let Some(t0) = self.last_became_runnable_at.take() {
            let d = now.duration_since(t0);
            self.queue_delay_ns.record_duration(d);
        }
        // starvation-ish measurement:
        if let Some(last) = self.last_serviced_at {
            let gap = now.duration_since(last);
            let ns = gap.as_nanos().min(u128::from(u64::MAX)) as u64;
            if ns > self.max_runnable_without_service_ns {
                self.max_runnable_without_service_ns = ns;
            }
        }
        self.last_serviced_at = Some(now);
    }

    #[inline]
    pub fn record_slice_exhausted(&mut self) {
        self.slice_exhaustions += 1;
    }
    #[inline]
    pub fn record_slice_overrun(&mut self) {
        self.slice_overruns += 1;
    }
    #[inline]
    pub fn record_preempt_requested(&mut self) {
        self.preempt_requested += 1;
    }
}
