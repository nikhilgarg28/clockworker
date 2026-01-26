//! Overhead benchmarks for Clockworker
//!
//! Run with: cargo bench --bench overhead
//!
//! Benchmarks:
//! - 1A: Spawn throughput (minimal work tasks)
//! - 1B: Yield/poll overhead (tasks that yield K times)
//! - 1C: IO reactor integration (timer-based tasks)

use clockworker::{
    scheduler::{RunnableFifo, LAS, QLAS},
    ExecutorBuilder,
};
use std::io::Write;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::task::LocalSet;

use crate::utils::{Step, Work};

mod utils;

// ============================================================================
// Configuration
// ============================================================================

const WARMUP_ITERS: usize = 3;
const BENCH_ITERS: usize = 10;

// 1A: Spawn throughput
const SPAWN_TASK_COUNTS: &[usize] = &[1_000, 10_000, 100_000];

// 1B: Yield overhead
const YIELD_TASK_COUNT: usize = 1_000;
const YIELDS_PER_TASK: [usize; 3] = [10, 100, 1_000];

// 1C: IO reactor
const IO_TASK_COUNT: usize = 100;
const SLEEPS_PER_TASK: usize = 10;
const SLEEP_DURATION: Duration = Duration::from_micros(500);

async fn drive<F>(executor: utils::Executor, n: usize, factory: impl Fn() -> F) -> Duration
where
    F: std::future::Future<Output = ()> + 'static,
{
    let start = Instant::now();
    let mut handles = Vec::with_capacity(n);
    for _ in 0..n {
        let fut = factory();
        handles.push(executor.spawn(async move {
            fut.await;
        }));
    }
    for h in handles {
        let _ = h.await;
    }
    start.elapsed()
}

fn run_1a() -> Vec<utils::Metrics> {
    let mut results = Vec::new();

    for &n in SPAWN_TASK_COUNTS {
        println!("\n  [] Spawn throughput: n={}", n);
        // first run tokio and then clockworker
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let factory = || async move {
            let fut = Work::new(vec![]);
            fut.run().await
        };

        let mut tokio_result = utils::Metrics::new();
        let mut cw_result = utils::Metrics::new();

        // Warmup
        for _ in 0..WARMUP_ITERS {
            rt.block_on(async move {
                let local = LocalSet::new();
                let executor = utils::Executor::start_tokio(local).await;
                executor
                    .run_until(drive(executor.clone(), n, factory))
                    .await;
            })
        }
        // Bench
        print!("Running Tokio: ");
        for _ in 0..BENCH_ITERS {
            let dur = rt.block_on(async move {
                let executor = utils::Executor::start_tokio(LocalSet::new()).await;
                let dur = executor
                    .run_until(drive(executor.clone(), n, factory))
                    .await;
                print!(".");
                std::io::stdout().flush().unwrap();
                dur
            });
            tokio_result.record(dur, &["spawn"]);
        }
        println!();
        // now run clockworker
        print!("Running Clockworker (FIFO): ");
        let cw_executor = ExecutorBuilder::new()
            .with_queue(0u8, 1, RunnableFifo::new())
            .build()
            .unwrap();
        for _ in 0..WARMUP_ITERS {
            let cw_executor = cw_executor.clone();
            let dur = rt.block_on(async move {
                let local = LocalSet::new();
                let executor = utils::Executor::start_clockworker(cw_executor, local).await;
                executor
                    .run_until(drive(executor.clone(), n, factory))
                    .await
            });
            cw_result.record(dur, &["spawn"]);
            print!(".");
            std::io::stdout().flush().unwrap();
        }
        for _ in 0..BENCH_ITERS {
            let cw_executor = cw_executor.clone();
            let dur = rt.block_on(async move {
                let local = LocalSet::new();
                let executor = utils::Executor::start_clockworker(cw_executor.clone(), local).await;
                executor
                    .run_until(drive(executor.clone(), n, factory))
                    .await
            });
            cw_result.record(dur, &["spawn"]);
            print!(".");
            std::io::stdout().flush().unwrap();
        }
        println!();

        // Print comparison
        let cw_tput = n as f64 / cw_result.mean("spawn").as_secs_f64();
        let tokio_tput = n as f64 / tokio_result.mean("spawn").as_secs_f64();
        let overhead = (cw_result.mean("spawn").as_secs_f64()
            / tokio_result.mean("spawn").as_secs_f64()
            - 1.0)
            * 100.0;

        println!(
            "    Clockworker: {:.2} tasks/sec (mean: {:?}, stddev: {:?})",
            cw_tput,
            cw_result.mean("spawn"),
            cw_result.stddev("spawn")
        );
        println!(
            "    Tokio:       {:.2} tasks/sec (mean: {:?}, stddev: {:?})",
            tokio_tput,
            tokio_result.mean("spawn"),
            tokio_result.stddev("spawn")
        );
        println!("    Overhead:    {:.1}%", overhead);

        results.push(cw_result);
        results.push(tokio_result);
    }

    results
}

// ============================================================================
// Benchmark 1B: Yield/Poll Overhead
// ============================================================================

fn run_1b() -> Vec<utils::Metrics> {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();

    let mut results = Vec::new();
    let n = YIELD_TASK_COUNT;

    for k in YIELDS_PER_TASK {
        let total_polls = n * (k + 1); // +1 for final Ready poll
        println!(
            "\n  [1B] Yield overhead: n={}, k={} ({} total polls)",
            n, k, total_polls
        );
        let factory = move || async move {
            let steps = (0..k).map(|_| Step::Yield).collect::<Vec<_>>();
            let fut = Work::new(steps);
            fut.run().await
        };

        // Clockworker with FIFO (lightweight scheduler)
        print!("Running Clockworker (FIFO): ");
        let mut cw_fifo = utils::Metrics::new();
        let executor = ExecutorBuilder::new()
            .with_queue(0u8, 1, RunnableFifo::new())
            .build()
            .unwrap();
        for _ in 0..WARMUP_ITERS {
            let executor = executor.clone();
            rt.block_on(async move {
                let local = LocalSet::new();
                let executor = utils::Executor::start_clockworker(executor, local).await;
                executor
                    .run_until(drive(executor.clone(), n, factory))
                    .await;
            })
        }
        for _ in 0..BENCH_ITERS {
            let executor = executor.clone();
            let dur = rt.block_on(async move {
                let local = LocalSet::new();
                let executor = utils::Executor::start_clockworker(executor, local).await;
                executor
                    .run_until(drive(executor.clone(), n, factory))
                    .await
            });
            cw_fifo.record(dur, &["yield"]);
            print!(".");
            std::io::stdout().flush().unwrap();
        }
        println!();

        // Clockworker with LAS (heavier scheduler)
        print!("Running Clockworker (LAS): ");
        let mut cw_las = utils::Metrics::new();
        let cw_executor = ExecutorBuilder::new()
            .with_queue(0u8, 1, LAS::new())
            .build()
            .unwrap();
        for _ in 0..WARMUP_ITERS {
            let cw_executor = cw_executor.clone();
            rt.block_on(async move {
                let local = LocalSet::new();
                let executor = utils::Executor::start_clockworker(cw_executor, local).await;
                executor
                    .run_until(drive(executor.clone(), n, factory))
                    .await;
            })
        }
        for _ in 0..BENCH_ITERS {
            let cw_executor = cw_executor.clone();
            let dur = rt.block_on(async move {
                let local = LocalSet::new();
                let executor = utils::Executor::start_clockworker(cw_executor, local).await;
                executor
                    .run_until(drive(executor.clone(), n, factory))
                    .await
            });
            cw_las.record(dur, &["yield"]);
            print!(".");
            std::io::stdout().flush().unwrap();
        }
        println!();
        // now run clockworker with QLAS
        print!("Running Clockworker (QLAS): ");
        let mut cw_qlas = utils::Metrics::new();
        let cw_executor = ExecutorBuilder::new()
            .with_queue(0u8, 1, QLAS::new())
            .build()
            .unwrap();
        for _ in 0..WARMUP_ITERS {
            let cw_executor = cw_executor.clone();
            rt.block_on(async move {
                let local = LocalSet::new();
                let executor = utils::Executor::start_clockworker(cw_executor, local).await;
                executor
                    .run_until(drive(executor.clone(), n, factory))
                    .await;
            })
        }
        for _ in 0..BENCH_ITERS {
            let cw_executor = cw_executor.clone();
            let dur = rt.block_on(async move {
                let local = LocalSet::new();
                let executor = utils::Executor::start_clockworker(cw_executor, local).await;
                executor
                    .run_until(drive(executor.clone(), n, factory))
                    .await
            });
            cw_qlas.record(dur, &["yield"]);
            print!(".");
            std::io::stdout().flush().unwrap();
        }
        println!();

        // Tokio baseline
        let mut tokio_result = utils::Metrics::new();
        for _ in 0..WARMUP_ITERS {
            rt.block_on(async move {
                let local = LocalSet::new();
                let executor = utils::Executor::start_tokio(local).await;
                executor
                    .run_until(drive(executor.clone(), n, factory))
                    .await;
            })
        }
        for _ in 0..BENCH_ITERS {
            let dur = rt.block_on(async move {
                let local = LocalSet::new();
                let executor = utils::Executor::start_tokio(local).await;
                executor
                    .run_until(drive(executor.clone(), n, factory))
                    .await
            });
            tokio_result.record(dur, &["yield"]);
            print!(".");
            std::io::stdout().flush().unwrap();
        }
        println!();

        // Print comparison
        let tokio_polls_per_sec = total_polls as f64 / tokio_result.mean("yield").as_secs_f64();
        let fifo_polls_per_sec = total_polls as f64 / cw_fifo.mean("yield").as_secs_f64();
        let las_polls_per_sec = total_polls as f64 / cw_las.mean("yield").as_secs_f64();
        let qlas_polls_per_sec = total_polls as f64 / cw_qlas.mean("yield").as_secs_f64();
        let fifo_overhead = (cw_fifo.mean("yield").as_nanos() as f64
            / tokio_result.mean("yield").as_nanos() as f64
            - 1.0)
            * 100.0;
        let las_overhead = (cw_las.mean("yield").as_nanos() as f64
            / tokio_result.mean("yield").as_nanos() as f64
            - 1.0)
            * 100.0;
        let qlas_overhead = (cw_qlas.mean("yield").as_nanos() as f64
            / tokio_result.mean("yield").as_nanos() as f64
            - 1.0)
            * 100.0;

        println!(
            "    Tokio:            {:.2e} polls/sec (mean: {:?})",
            tokio_polls_per_sec,
            tokio_result.mean("yield")
        );
        println!(
            "    Clockworker/QLAS: {:.2e} polls/sec (mean: {:?}) [overhead: {:.1}%]",
            qlas_polls_per_sec,
            cw_qlas.mean("yield"),
            qlas_overhead
        );
        println!(
            "    Clockworker/FIFO: {:.2e} polls/sec (mean: {:?}) [overhead: {:.1}%]",
            fifo_polls_per_sec,
            cw_fifo.mean("yield"),
            fifo_overhead
        );
        println!(
            "    Clockworker/LAS:  {:.2e} polls/sec (mean: {:?}) [overhead: {:.1}%]",
            las_polls_per_sec,
            cw_las.mean("yield"),
            las_overhead
        );

        results.push(cw_fifo);
        results.push(cw_las);
        results.push(cw_qlas);
        results.push(tokio_result);
    }

    results
}

// ============================================================================
// Benchmark 1C: IO Reactor Integration
// ============================================================================

/// Clockworker: tasks that sleep (exercises reactor integration)
async fn bench_1c_clockworker(
    n: usize,
    sleeps: usize,
    sleep_dur: Duration,
) -> (Duration, Vec<Duration>) {
    let executor = ExecutorBuilder::new()
        .with_queue(0u8, 1, RunnableFifo::new())
        .build()
        .unwrap();

    let queue = executor.queue(0).unwrap();

    let executor_clone = executor.clone();
    let runner = tokio::task::spawn_local(async move {
        executor_clone.run().await;
    });

    // Collect actual sleep durations to measure accuracy
    let sleep_accuracies: Arc<std::sync::Mutex<Vec<Duration>>> =
        Arc::new(std::sync::Mutex::new(Vec::new()));

    let start = Instant::now();

    let mut handles = Vec::with_capacity(n);
    for _ in 0..n {
        let accuracies = sleep_accuracies.clone();
        let dur = sleep_dur;
        handles.push(queue.spawn(async move {
            for _ in 0..sleeps {
                let before = Instant::now();
                tokio::time::sleep(dur).await;
                let actual = before.elapsed();
                accuracies.lock().unwrap().push(actual);
            }
        }));
    }

    for h in handles {
        let _ = h.await;
    }

    let elapsed = start.elapsed();

    runner.abort();

    let accuracies = Arc::try_unwrap(sleep_accuracies)
        .unwrap()
        .into_inner()
        .unwrap();
    (elapsed, accuracies)
}

/// Tokio baseline: tasks that sleep
async fn bench_1c_tokio(n: usize, sleeps: usize, sleep_dur: Duration) -> (Duration, Vec<Duration>) {
    let sleep_accuracies: Arc<std::sync::Mutex<Vec<Duration>>> =
        Arc::new(std::sync::Mutex::new(Vec::new()));

    let start = Instant::now();

    let mut handles = Vec::with_capacity(n);
    for _ in 0..n {
        let accuracies = sleep_accuracies.clone();
        let dur = sleep_dur;
        handles.push(tokio::task::spawn_local(async move {
            for _ in 0..sleeps {
                let before = Instant::now();
                tokio::time::sleep(dur).await;
                let actual = before.elapsed();
                accuracies.lock().unwrap().push(actual);
            }
        }));
    }

    for h in handles {
        let _ = h.await;
    }

    let elapsed = start.elapsed();

    let accuracies = Arc::try_unwrap(sleep_accuracies)
        .unwrap()
        .into_inner()
        .unwrap();
    (elapsed, accuracies)
}

fn analyze_sleep_accuracy(
    accuracies: &[Duration],
    expected: Duration,
) -> (Duration, Duration, f64) {
    let mean: Duration = accuracies.iter().sum::<Duration>() / accuracies.len() as u32;
    let max = *accuracies.iter().max().unwrap();
    let overslept_ratio = mean.as_nanos() as f64 / expected.as_nanos() as f64;
    (mean, max, overslept_ratio)
}

fn run_1c() -> Vec<utils::Metrics> {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();

    let mut results = Vec::new();

    println!(
        "\n  [1C] IO reactor integration: n={}, sleeps={}, sleep_dur={:?}",
        IO_TASK_COUNT, SLEEPS_PER_TASK, SLEEP_DURATION
    );

    let expected_total = SLEEP_DURATION * (SLEEPS_PER_TASK as u32);
    println!(
        "    Expected minimum time: {:?} (if fully parallel)",
        expected_total
    );

    // Clockworker
    let mut cw_result = utils::Metrics::new();
    let mut cw_accuracies = Vec::new();

    for _ in 0..WARMUP_ITERS {
        let local = LocalSet::new();
        rt.block_on(local.run_until(bench_1c_clockworker(
            IO_TASK_COUNT,
            SLEEPS_PER_TASK,
            SLEEP_DURATION,
        )));
    }

    for _ in 0..BENCH_ITERS {
        let local = LocalSet::new();
        let (dur, accuracies) = rt.block_on(local.run_until(bench_1c_clockworker(
            IO_TASK_COUNT,
            SLEEPS_PER_TASK,
            SLEEP_DURATION,
        )));
        cw_result.record(dur, &["sleep"]);
        cw_accuracies.extend(accuracies);
        print!(".");
        std::io::stdout().flush().unwrap();
    }
    println!();

    // Tokio baseline
    let mut tokio_result = utils::Metrics::new();
    let mut tokio_accuracies = Vec::new();

    for _ in 0..WARMUP_ITERS {
        let local = LocalSet::new();
        rt.block_on(local.run_until(bench_1c_tokio(
            IO_TASK_COUNT,
            SLEEPS_PER_TASK,
            SLEEP_DURATION,
        )));
    }

    for _ in 0..BENCH_ITERS {
        let local = LocalSet::new();
        let (dur, accuracies) = rt.block_on(local.run_until(bench_1c_tokio(
            IO_TASK_COUNT,
            SLEEPS_PER_TASK,
            SLEEP_DURATION,
        )));
        tokio_result.record(dur, &["sleep"]);
        tokio_accuracies.extend(accuracies);
        print!(".");
        std::io::stdout().flush().unwrap();
    }
    println!();

    // Analyze
    let (cw_mean_sleep, cw_max_sleep, cw_ratio) =
        analyze_sleep_accuracy(&cw_accuracies, SLEEP_DURATION);
    let (tokio_mean_sleep, tokio_max_sleep, tokio_ratio) =
        analyze_sleep_accuracy(&tokio_accuracies, SLEEP_DURATION);

    println!("    Tokio:");
    println!("      Total time: {:?} (mean)", tokio_result.mean("sleep"));
    println!(
        "      Sleep accuracy: mean={:?} max={:?} ratio={:.2}x",
        tokio_mean_sleep, tokio_max_sleep, tokio_ratio
    );

    println!("    Clockworker:");
    println!("      Total time: {:?} (mean)", cw_result.mean("sleep"));
    println!(
        "      Sleep accuracy: mean={:?} max={:?} ratio={:.2}x",
        cw_mean_sleep, cw_max_sleep, cw_ratio
    );

    let overhead = (cw_result.mean("sleep").as_nanos() as f64
        / tokio_result.mean("sleep").as_nanos() as f64
        - 1.0)
        * 100.0;
    println!("    Overhead: {:.1}%", overhead);

    if cw_ratio > tokio_ratio * 1.5 {
        println!(
            "    ⚠️  WARNING: Clockworker sleeps are {:.1}x longer than Tokio's - possible reactor starvation",
            cw_ratio / tokio_ratio
        );
    }

    results.push(cw_result);
    results.push(tokio_result);

    results
}

// ============================================================================
// Main
// ============================================================================

fn main() {
    println!("╔══════════════════════════════════════════════════════════════╗");
    println!("║          Clockworker Overhead Benchmarks                     ║");
    println!("╚══════════════════════════════════════════════════════════════╝");
    println!();
    println!("Configuration:");
    println!("  Warmup iterations: {}", WARMUP_ITERS);
    println!("  Bench iterations:  {}", BENCH_ITERS);
    println!();

    // 1A: Spawn throughput
    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
    println!("Benchmark 1A: Spawn Throughput");
    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
    run_1a();

    // 1B: Yield overhead
    println!("\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
    println!("Benchmark 1B: Yield/Poll Overhead");
    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
    run_1b();

    // 1C: IO reactor
    println!("\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
    println!("Benchmark 1C: IO Reactor Integration");
    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
    run_1c();
}
