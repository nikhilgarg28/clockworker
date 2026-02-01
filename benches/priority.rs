mod utils;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};
use std::thread;
use std::time::{Duration, Instant};
use tabled::Table;

use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};

use utils::{Metrics, Step, Work};

// Cache core IDs at startup to avoid issues with affinity changes
static CORE_IDS: OnceLock<Vec<core_affinity::CoreId>> = OnceLock::new();

fn get_cached_core_ids() -> &'static Vec<core_affinity::CoreId> {
    CORE_IDS.get_or_init(|| {
        core_affinity::get_core_ids().unwrap_or_default()
    })
}

// ============================================================================
// Configuration
// ============================================================================

pub struct PriorityBenchmarkSpec {
    /// Number of foreground tasks to complete
    pub foreground_count: usize,
    /// Target foreground arrival rate (tasks per second)
    pub foreground_rps: usize,
    /// Number of concurrent background tasks
    pub background_count: usize,
}

impl Default for PriorityBenchmarkSpec {
    fn default() -> Self {
        Self {
            foreground_count: 5000,
            foreground_rps: 1000,
            background_count: 8,
        }
    }
}

// ============================================================================
// Background Work Tracking
// ============================================================================

/// Global counter for background work iterations across all background tasks.
/// Each background task increments this every iteration.
static BACKGROUND_ITERATIONS: AtomicU64 = AtomicU64::new(0);

fn reset_counters() {
    BACKGROUND_ITERATIONS.store(0, Ordering::SeqCst);
}

fn get_background_iterations() -> u64 {
    BACKGROUND_ITERATIONS.load(Ordering::SeqCst)
}

// ============================================================================
// Work Definitions
// ============================================================================

/// Generate foreground work: 1-3 segments of [100-500us CPU + 100-500us sleep]
fn generate_foreground_work(rng: &mut impl Rng) -> Work {
    let segments = rng.gen_range(1..=3) as usize;
    let mut steps = Vec::with_capacity(segments * 2);
    for i in 0..segments {
        let cpu = rng.gen_range(100..=500);
        steps.push(Step::CPU(Duration::from_micros(cpu)));
        if i < segments - 1 {
            let io = rng.gen_range(100..=500);
            steps.push(Step::Sleep(Duration::from_micros(io)));
        }
    }
    Work::new(steps)
}

/// Run background work in a loop until shutdown signal
async fn background_loop(shutdown: Arc<AtomicBool>, rng: &mut impl Rng) {
    while !shutdown.load(Ordering::Relaxed) {
        let cpu = rng.gen_range(100..=500);
        let io = rng.gen_range(100..=500);
        let work = Work::new(vec![
            Step::CPU(Duration::from_micros(cpu)),
            Step::Sleep(Duration::from_micros(io)),
        ]);
        work.run().await;
        BACKGROUND_ITERATIONS.fetch_add(1, Ordering::Relaxed);
    }
}

// ============================================================================
// Foreground Task
// ============================================================================

struct ForegroundTask {
    work: Work,
    submit_time: Instant,
}

// ============================================================================
// Task Generator (runs on separate thread)
// ============================================================================

/// Generate foreground tasks with Poisson-like arrival pattern.
/// Runs on a separate thread to avoid interfering with the runtime under test.
fn generate_foreground_tasks(tx: flume::Sender<ForegroundTask>, count: usize, rps: usize) {
    let mut rng = StdRng::seed_from_u64(0xC0FFEE);
    let period = Duration::from_nanos(1_000_000_000u64 / rps as u64);

    for _ in 0..count {
        // Wait for exponentially distributed delay (Poisson arrival)
        let delay = utils::exponential_delay(&mut rng, period);
        utils::do_cpu_work(delay);

        // Generate and send task
        let work = generate_foreground_work(&mut rng);
        let task = ForegroundTask {
            work,
            submit_time: Instant::now(),
        };

        if tx.send(task).is_err() {
            break; // Receiver dropped, benchmark ending
        }
    }
}

// ============================================================================
// Benchmark: Clockworker
// ============================================================================

pub fn benchmark_clockworker(spec: PriorityBenchmarkSpec) -> BenchmarkResult {
    reset_counters();
    let (tx, rx) = flume::unbounded();
    let cores = get_cached_core_ids();
    let target_core = cores.first().copied().unwrap();
    let other_core = cores.get(1).copied().unwrap();

    let gen_handle = {
        let count = spec.foreground_count;
        let rps = spec.foreground_rps;
        thread::spawn(move || {
            core_affinity::set_for_current(other_core);
            generate_foreground_tasks(tx, count, rps)
        })
    };
    let start_time = Instant::now();
    let main_handle = thread::spawn(move || {
        // Pin to target core
        core_affinity::set_for_current(target_core);

        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();

        let metrics = rt.block_on(async move {
            let local = tokio::task::LocalSet::new();
            // Wait for driver to complete
            let metrics = local
                .run_until(async move {
                    let shutdown = Arc::new(AtomicBool::new(false));
                    let executor = clockworker::ExecutorBuilder::new()
                        .with_queue(0, 99)
                        .with_queue(1, 1)
                        .build()
                        .unwrap();
                    let executor_clone = executor.clone();
                    let result = executor_clone
                        .run_until(async move {
                            let rng = StdRng::seed_from_u64(0xC0FFEE);
                            for _ in 0..spec.background_count {
                                let shutdown = shutdown.clone();
                                let mut rng = rng.clone();
                                executor.queue(1).unwrap().spawn(async move {
                                    background_loop(shutdown, &mut rng).await;
                                });
                            }
                            let handle = executor
                                .queue(0)
                                .unwrap()
                                .spawn(async move {
                                    let mut handles = Vec::with_capacity(8192);
                                    while let Ok(task) = rx.recv_async().await {
                                        let handle = executor.queue(0).unwrap().spawn(async move {
                                            let start_time = Instant::now();
                                            let queue_delay =
                                                start_time.duration_since(task.submit_time);
                                            task.work.run().await;
                                            let total_latency = task.submit_time.elapsed();
                                            (queue_delay, total_latency)
                                        });
                                        handles.push(handle);
                                    }

                                    // Collect results
                                    let mut metrics = Metrics::new();
                                    for handle in handles {
                                        let (queue_delay, total_latency) = handle.await.unwrap();
                                        metrics.record(queue_delay, &["queue_delay"]);
                                        metrics.record(total_latency, &["total_latency"]);
                                    }
                                    shutdown.store(true, Ordering::SeqCst);
                                    metrics
                                })
                                .await;
                            handle.unwrap()
                        })
                        .await;
                    result
                })
                .await;
            metrics
        });
        metrics
    });
    let metrics = main_handle.join().unwrap();
    let elapsed = start_time.elapsed();
    gen_handle.join().unwrap();
    let bg_iterations = get_background_iterations();

    BenchmarkResult {
        metrics,
        elapsed,
        background_iterations: bg_iterations,
    }
}

// ============================================================================
// Benchmark: Vanilla Tokio Single-Threaded
// ============================================================================

pub fn benchmark_tokio_single(spec: PriorityBenchmarkSpec) -> BenchmarkResult {
    reset_counters();

    let (tx, rx) = flume::unbounded();
    let shutdown = Arc::new(AtomicBool::new(false));
    let cores = get_cached_core_ids();
    let target_core = cores.first().copied().unwrap();
    let other_core = cores.get(1).copied().unwrap();

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();

    // Spawn generator on separate thread
    let gen_handle = {
        let count = spec.foreground_count;
        let rps = spec.foreground_rps;
        thread::spawn(move || {
            core_affinity::set_for_current(other_core);
            generate_foreground_tasks(tx, count, rps)
        })
    };

    let benchmark_start = Instant::now();

    let metrics = rt.block_on(async {
        core_affinity::set_for_current(target_core);
        let local = tokio::task::LocalSet::new();

        local
            .run_until(async {
                // Spawn background tasks (no priority distinction - same as foreground)
                let rng = StdRng::seed_from_u64(0xC0FFEE);
                for _ in 0..spec.background_count {
                    let shutdown = shutdown.clone();
                    let mut rng = rng.clone();
                    tokio::task::spawn_local(async move {
                        background_loop(shutdown, &mut rng).await;
                    });
                }

                // Drive foreground tasks
                let mut handles = Vec::with_capacity(8192);
                while let Ok(task) = rx.recv_async().await {
                    let handle = tokio::task::spawn_local(async move {
                        let start_time = Instant::now();
                        let queue_delay = start_time.duration_since(task.submit_time);
                        task.work.run().await;
                        let total_latency = task.submit_time.elapsed();
                        (queue_delay, total_latency)
                    });
                    handles.push(handle);
                }

                let mut metrics = Metrics::new();
                for h in handles {
                    let (queue_delay, total_latency) = h.await.unwrap();
                    metrics.record(queue_delay, &["queue_delay"]);
                    metrics.record(total_latency, &["total_latency"]);
                }

                metrics
            })
            .await
    });

    let elapsed = benchmark_start.elapsed();

    shutdown.store(true, Ordering::SeqCst);
    gen_handle.join().unwrap();

    let bg_iterations = get_background_iterations();

    BenchmarkResult {
        metrics,
        elapsed,
        background_iterations: bg_iterations,
    }
}

// ============================================================================
// Benchmark: Two Runtimes with OS Priority
// ============================================================================

pub fn benchmark_two_runtime(spec: PriorityBenchmarkSpec) -> BenchmarkResult {
    reset_counters();

    let (task_tx, task_rx) = flume::unbounded::<ForegroundTask>();
    let (result_tx, result_rx) = flume::unbounded::<(Duration, Duration)>();
    let shutdown = Arc::new(AtomicBool::new(false));

    // Determine target core (use core 0, or first available)
    let cores = get_cached_core_ids();
    let target_core = cores.first().copied().expect("No CPU cores available");

    // Spawn background runtime thread (low priority)
    let bg_shutdown = shutdown.clone();
    let bg_count = spec.background_count;
    let bg_handle = thread::spawn(move || {
        // Pin to target core
        core_affinity::set_for_current(target_core);

        // Set low priority (nice +19 on Unix)
        #[cfg(unix)]
        unsafe {
            libc::setpriority(libc::PRIO_PROCESS, 0, 19);
        }

        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();

        rt.block_on(async {
            let local = tokio::task::LocalSet::new();
            local
                .run_until(async {
                    // Spawn background tasks
                    let rng = StdRng::seed_from_u64(0xC0FFEE);
                    for _ in 0..bg_count {
                        let shutdown = bg_shutdown.clone();
                        let mut rng = rng.clone();
                        tokio::task::spawn_local(async move {
                            background_loop(shutdown, &mut rng).await;
                        });
                    }

                    // Keep running until shutdown
                    while !bg_shutdown.load(Ordering::Relaxed) {
                        tokio::time::sleep(Duration::from_millis(10)).await;
                    }
                })
                .await;
        });
    });

    // Spawn foreground runtime thread (high priority)
    let fg_shutdown = shutdown.clone();
    let fg_handle = thread::spawn(move || {
        // Pin to same core
        core_affinity::set_for_current(target_core);

        // Set high priority (nice -20 on Unix, requires root or CAP_SYS_NICE)
        #[cfg(unix)]
        unsafe {
            libc::setpriority(libc::PRIO_PROCESS, 0, -20);
        }

        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();

        rt.block_on(async {
            let local = tokio::task::LocalSet::new();
            local
                .run_until(async {
                    let mut handles = Vec::with_capacity(8192);

                    while let Ok(task) = task_rx.recv_async().await {
                        let result_tx = result_tx.clone();
                        let handle = tokio::task::spawn_local(async move {
                            let start_time = Instant::now();
                            let queue_delay = start_time.duration_since(task.submit_time);
                            task.work.run().await;
                            let total_latency = task.submit_time.elapsed();
                            let _ = result_tx.send((queue_delay, total_latency));
                        });
                        handles.push(handle);
                    }

                    // Wait for all tasks to complete
                    for h in handles {
                        let _ = h.await;
                    }

                    fg_shutdown.store(true, Ordering::SeqCst);
                })
                .await;
        });
    });

    // Spawn generator on a DIFFERENT core to avoid interference
    let other_core = get_cached_core_ids()
        .get(1)
        .copied()
        .unwrap_or(target_core); // Fall back if only one core

    let gen_handle = {
        let count = spec.foreground_count;
        let rps = spec.foreground_rps;
        thread::spawn(move || {
            core_affinity::set_for_current(other_core);
            generate_foreground_tasks(task_tx, count, rps);
        })
    };

    // Collect results
    let benchmark_start = Instant::now();
    gen_handle.join().unwrap();
    fg_handle.join().unwrap();
    let elapsed = benchmark_start.elapsed();

    shutdown.store(true, Ordering::SeqCst);
    bg_handle.join().unwrap();

    // Collect metrics from results channel
    let mut metrics = Metrics::new();
    while let Ok((queue_delay, total_latency)) = result_rx.try_recv() {
        metrics.record(queue_delay, &["queue_delay"]);
        metrics.record(total_latency, &["total_latency"]);
    }

    let bg_iterations = get_background_iterations();

    BenchmarkResult {
        metrics,
        elapsed,
        background_iterations: bg_iterations,
    }
}

// ============================================================================
// Results
// ============================================================================

pub struct BenchmarkResult {
    pub metrics: Metrics,
    pub elapsed: Duration,
    pub background_iterations: u64,
}

impl BenchmarkResult {
    pub fn background_throughput(&self) -> f64 {
        self.background_iterations as f64 / self.elapsed.as_secs_f64()
    }

    pub fn print_summary(&self, name: &str) {
        println!("\n=== {} ===", name);
        println!("Foreground tasks: {}", self.metrics.len());
        println!("Elapsed: {:.2?}", self.elapsed);
        println!("Background iterations: {}", self.background_iterations);
        println!(
            "Background throughput: {:.0} iter/sec",
            self.background_throughput()
        );
        println!();
        println!("Queue Delay (time from submit to task start):");
        println!(
            "  p50:  {:>10.2?}",
            self.metrics.quantile(50.0, "queue_delay")
        );
        println!(
            "  p90:  {:>10.2?}",
            self.metrics.quantile(90.0, "queue_delay")
        );
        println!(
            "  p99:  {:>10.2?}",
            self.metrics.quantile(99.0, "queue_delay")
        );
        println!(
            "  p999: {:>10.2?}",
            self.metrics.quantile(99.9, "queue_delay")
        );
        println!(
            "  max:  {:>10.2?}",
            self.metrics.quantile(100.0, "queue_delay")
        );
        println!();
        println!("Total Latency (submit to completion):");
        println!(
            "  p50:  {:>10.2?}",
            self.metrics.quantile(50.0, "total_latency")
        );
        println!(
            "  p90:  {:>10.2?}",
            self.metrics.quantile(90.0, "total_latency")
        );
        println!(
            "  p99:  {:>10.2?}",
            self.metrics.quantile(99.0, "total_latency")
        );
        println!(
            "  p999: {:>10.2?}",
            self.metrics.quantile(99.9, "total_latency")
        );
        println!(
            "  max:  {:>10.2?}",
            self.metrics.quantile(100.0, "total_latency")
        );
    }
}

// ============================================================================
// Main
// ============================================================================

fn main() {
    let spec = PriorityBenchmarkSpec::default();

    println!("Priority Benchmark");
    println!("==================");
    println!(
        "Foreground: {} tasks @ {} rps",
        spec.foreground_count, spec.foreground_rps
    );
    println!("Background: {} continuous tasks", spec.background_count);
    println!("Foreground work: 1-3 segments of 100μs CPU + 100μs sleep");
    println!("Background work: loop of 100μs CPU + 100μs sleep");

    // Run benchmarks
    println!("\nRunning Clockworker benchmark...");
    let spec = PriorityBenchmarkSpec::default();
    let cw_result = benchmark_clockworker(spec);
    cw_result.print_summary("Clockworker (both foreground + background)");

    let mut spec = PriorityBenchmarkSpec::default();
    spec.background_count = 0;
    let cw_result_fg_only = benchmark_clockworker(spec);
    cw_result_fg_only.print_summary("Clockworker (foreground only)");

    println!("\nRunning Tokio single-threaded benchmark...");
    let tokio_result = benchmark_tokio_single(PriorityBenchmarkSpec::default());
    tokio_result.print_summary("Tokio Single-Threaded");
    let mut spec = PriorityBenchmarkSpec::default();
    spec.background_count = 0;
    let tokio_result_fg_only = benchmark_tokio_single(spec);
    tokio_result_fg_only.print_summary("Tokio Single-Threaded (foreground only)");

    println!("\nRunning Two-Runtime (OS priority) benchmark...");
    let two_rt_result = benchmark_two_runtime(PriorityBenchmarkSpec::default());
    two_rt_result.print_summary("Two Runtimes (OS Priority)");

    // Comparison table
    println!("\n");
    println!("=== COMPARISON TABLE ===");
    println!();
    print_comparison_table(&[
        ("Clockworker (fg + bg)", &cw_result),
        ("Clockworker (fg only)", &cw_result_fg_only),
        ("Tokio (fg + bg)", &tokio_result),
        ("Tokio (fg only)", &tokio_result_fg_only),
        ("Tokio Two-RT/OS", &two_rt_result),
    ]);
}

fn print_comparison_table(results: &[(&str, &BenchmarkResult)]) {
    #[derive(tabled::Tabled)]
    struct ComparisonTable {
        name: String,
        p50_queue_delay: String,
        p90_queue_delay: String,
        p99_queue_delay: String,
        p50_total_latency: String,
        p90_total_latency: String,
        p99_total_latency: String,
        bg_iter_s: String,
    }
    let mut rows = Vec::new();
    for (name, result) in results {
        rows.push(ComparisonTable {
            name: name.to_string(),
            p50_queue_delay: print_quantile(&result.metrics, "queue_delay", 50.0),
            p90_queue_delay: print_quantile(&result.metrics, "queue_delay", 90.0),
            p99_queue_delay: print_quantile(&result.metrics, "queue_delay", 99.0),
            p50_total_latency: print_quantile(&result.metrics, "total_latency", 50.0),
            p90_total_latency: print_quantile(&result.metrics, "total_latency", 90.0),
            p99_total_latency: print_quantile(&result.metrics, "total_latency", 99.0),
            bg_iter_s: format!("{:.0}", result.background_throughput()),
        });
    }
    let table = Table::builder(rows).index().column(0).transpose().build();
    println!("{}", table);
}

fn print_quantile(metrics: &Metrics, tag: &str, quantile: f64) -> String {
    let micros = metrics.quantile(quantile, tag).as_micros();
    // print two decimal places
    format!("{:.2}ms", micros as f64 / 1000.0)
}
