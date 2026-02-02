# Clockworker

Clockworker, loosely inspired by Seastar, is a single-threaded async executor
with fair scheduling across multiple queues. Clockworker is agnostic to the underlying
async runtime and can sit on top of any runtime like Tokio, Monoio, or Smol.

┌─────────────────────────────────────────┐
│            Your Application             │
├─────────────────────────────────────────┤
│   Clockworker (async executor/scheduler)│
│   - Priority queues                     |
│   - Timeslice management                │
|   - Priority based preemption           |
├─────────────────────────────────────────┤
│   Any async IO Driver                   │
│   ┌─────────┐ ┌─────────┐ ┌─────────┐   │
│   │  tokio  │ │ monoio  │ │ glommio │   │
│   │ (epoll) │ │(io_uring)│ │(io_uring)│ │
│   └─────────┘ └─────────┘ └─────────┘   │
└─────────────────────────────────────────┘

**⚠️ Early/Alpha Release**: This project is in early development. APIs may change in breaking ways between versions. Use at your own risk.

## What is Clockworker for?

Single-threaded async runtimes are a great fit for a class of problems.
Several such runtimes exist in the Rust ecosystem—Tokio, Monoio, Glommio, etc. But
almost none of these (with the exception of Glommio) provide the ability to run
multiple configurable work queues with different priorities. This becomes important
for many real-world single-threaded systems, at minimum to separate foreground and
background work. Clockworker aims to solve this problem.

It does so via work queues with configurable time-shares onto which tasks can be
spawned. Clockworker uses an EEVDF-inspired scheduler: it chooses the
queue to poll based on its fair time share (inspired by Linux CFS/EEVDF), and then
executes tasks from that queue in FIFO order.

Note that Clockworker itself is just an executor loop, not a full async runtime, and
is designed to sit on top of any other runtime.

## Benchmarks

**Setup**

Foreground tasks are generated at RPS of 1K/sec, each doing 1-3 yields - at each
yield point, the task does variable (100-500us) of cpu work and a variable (100-500us)
of sleep. In addition, some background tasks (0 or 8) are also generated that do 
similar amount of cpu work and sleep. Background tasks, when present, have enough
CPU work in aggregate to saturate the CPU.

The workload runs on a single thread pinned to a core while the work generation
happens on another thread/core. Two latencies are measured: a) time from task
generation to task starting (i.e. queue delay) b) end to end time from task 
generation to task finishing (i.e. total latency). In addition, the amount of
background work done is also measured.


The benchmark compares five options:
1. Tokio single threaded runtime with only foreground tasks (baseline)
2. Tokio single threaded runtime with both foreground & background tasks
3. Clockworker executor on top of tokio single threaded runtime with only foreground tasks
4. Same as #3 above but with both foreground/background tasks.
5. Two separate single threaded tokio runtimes - one for background and one for
foreground (no coordination between them)

**Results**
|       Metric       |    Tokio (fg only)     |       CW (fg+bg)       |      CW (fg only)      |     Tokio (fg+bg)      |       Two-RT/OS        |
|--------------------|------------------------|------------------------|------------------------|------------------------|------------------------|
|  p50 queue delay   |         0.40ms         |      0.41ms (+2%)      |      0.38ms (-5%)      |    8.33ms (+1982%)     |     0.49ms (+22%)      |
|  p90 queue delay   |         1.23ms         |      1.18ms (-4%)      |      1.20ms (-2%)      |    12.05ms (+880%)     |     1.39ms (+13%)      |
|  p99 queue delay   |         2.19ms         |      2.18ms (-0%)      |      2.18ms (-0%)      |    15.89ms (+626%)     |     2.52ms (+15%)      |
| p50 total latency  |         2.44ms         |      2.37ms (-3%)      |      2.46ms (+1%)      |    14.78ms (+506%)     |      2.56ms (+5%)      |
| p90 total latency  |         4.65ms         |      4.53ms (-3%)      |      4.71ms (+1%)      |    24.23ms (+421%)     |      4.83ms (+4%)      |
| p99 total latency  |         6.16ms         |      6.11ms (-1%)      |      6.30ms (+2%)      |    31.60ms (+413%)     |      6.63ms (+8%)      |
|   BG throughput    |           0            |          1299          |           0            |          1337          |          1299          |

**Interpretation**
1. Clockworker adds negligible overhead and with foreground only tasks, is competitive
   with singel threaded tokio runtime.
2. With Tokio runtime, foreground latency degrades 4-5x with background tasks.
3. With Clockworker executor, background tasks don't impact the foreground latency 
   at all - and the performance is competitive with tokio foreground only
4. Adding two-runtimes is also competitive from latency POV
5. All three options with background tasks are comparable in terms of the volume
   of the background work done.

## Clockworker vs Multiple Runtimes

While running multiple runtimes, one for each priority queue, is competitive
with Clockworker as shown in above benchmarks, there are a few other benefits
of using clockworker:
1. Less complexity - it's some additional work to setup & maintain multiple runtimes
   (e.g. graceful shutdowns, sharing stats etc.)
2. Clockworkes works with !Send and !Sync futures - no need for Arc/Mutex etc. This both
   improves ergonomics and may also show up as overhead depending on the application 
   (not tested in the benchmarks)
3. It's non-trivial to setup multiple runtimes in platform agnostic way (e.g. CPU
    affinity works differently on Mac vs Linux)

## Features

- **EEVDF-based queue scheduling**: Fair CPU time distribution between queues using virtual runtime (inspired by Linux CFS/EEVDF)
- **Premption**: If a queue becomes runnable while the timeslice of another lower
 priority queue is being executed, the timeslice is preempted at the next poll
- **FIFO task ordering**: Tasks within each queue execute in FIFO order
- **Task cancellation**: Abort running tasks via `JoinHandle::abort()`
- **Panic handling**: Configurable panic behavior (propagate or catch as `JoinError::Panic`)
- **Statistics**: Built-in metrics for monitoring executor and queue performance

## Quick Start

Add to your `Cargo.toml`:

```toml
[dependencies]
clockworker = "0.2.0"
```

## Examples

### Basic Usage

The simplest example - spawn a task and wait for it:

```rust
use clockworker::ExecutorBuilder;
use tokio::task::LocalSet;

#[tokio::main(flavor = "current_thread")]
async fn main() {
    let local = LocalSet::new();
    local.run_until(async {
        // Create executor with a single queue
        let executor = ExecutorBuilder::new()
            .with_queue(0, 1)
            .build()
            .unwrap();

        // Get handle to the queue
        let queue = executor.queue(0).unwrap();

        // Run executor until our task completes
        let result = executor.run_until(async {
            // Spawn a task
            let handle = queue.spawn(async {
                println!("Hello from clockworker!");
                42
            });

            // Wait for task to complete
            handle.await
        }).await;

        println!("Task result: {:?}", result); // Ok(42)
    }).await;
}
```

### Multiple Queues with Different Weights

Allocate CPU time proportionally between queues:

```rust
use clockworker::{ExecutorBuilder, yield_maybe};
use tokio::task::LocalSet;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};

#[derive(Debug, PartialEq, Eq, Hash, Copy, Clone)]
enum Queue {
    Foreground,
    Background,
}

#[tokio::main(flavor = "current_thread")]
async fn main() {
    let local = LocalSet::new();
    local.run_until(async {
        // Create executor with two queues:
        // - Foreground: weight 9 (gets 9/10 of CPU time)
        // - Background: weight 1 (gets 1/10 of CPU time)
        // Note: Queue IDs can be enums (as shown here) or integers, strings, or any
        // type implementing QueueKey
        let executor = ExecutorBuilder::new()
            .with_queue(Queue::Foreground, 8)  // High priority queue
            .with_queue(Queue::Background, 1)  // Low priority queue
            .build()
            .unwrap();

        let high_count = Arc::new(AtomicU32::new(0));
        let low_count = Arc::new(AtomicU32::new(0));

        // Spawn tasks in both queues
        let high_queue = executor.queue(Queue::Foreground).unwrap();
        let low_queue = executor.queue(Queue::Background).unwrap();

        let high_clone = high_count.clone();
        let low_clone = low_count.clone();

        executor.run_until(async {
            high_queue.spawn({
                let count = high_clone;
                async move {
                    loop {
                        count.fetch_add(1, Ordering::Relaxed);
                        yield_maybe().await;
                    }
                }
            });

            low_queue.spawn({
                let count = low_clone;
                async move {
                    loop {
                        count.fetch_add(1, Ordering::Relaxed);
                        yield_maybe().await;
                    }
                }
            });

            // Run for a bit
            tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
        }).await;

        // After running for a bit, high_count should be ~9x low_count
        println!("High: {}, Low: {}",
                 high_count.load(Ordering::Relaxed),
                 low_count.load(Ordering::Relaxed));
    }).await;
}
```

### Task Cancellation

Cancel tasks using `JoinHandle::abort()`:

```rust
use clockworker::ExecutorBuilder;
use tokio::task::LocalSet;
use tokio::time::{sleep, Duration};

#[tokio::main(flavor = "current_thread")]
async fn main() {
    let local = LocalSet::new();
    local.run_until(async {
        let executor = ExecutorBuilder::new()
            .with_queue(0, 1)
            .build()
            .unwrap();

        let queue = executor.queue(0).unwrap();

        executor.run_until(async {
            // Spawn a long-running task
            let handle = queue.spawn(async {
                loop {
                    println!("Running...");
                    sleep(Duration::from_millis(100)).await;
                }
            });

            // Cancel it after 500ms
            sleep(Duration::from_millis(500)).await;
            handle.abort();

            // Wait for cancellation to complete
            let result = handle.await;
            assert!(result.is_err());
            println!("Task cancelled: {:?}", result);
        }).await;
    }).await;
}
```

### Panic Handling

By default, the executor also panics when any of the tasks panic (same behavior
as Tokio's single-threaded runtime). However, this can be configured:

```rust
use clockworker::{ExecutorBuilder, JoinError};
use tokio::task::LocalSet;

#[tokio::main(flavor = "current_thread")]
async fn main() {
    let local = LocalSet::new();
    local.run_until(async {
        // Configure executor to catch panics instead of crashing
        let executor = ExecutorBuilder::new()
            .with_queue(0, 1)
            .with_panic_on_task_panic(false)  // Catch panics
            .build()
            .unwrap();

        let queue = executor.queue(0).unwrap();

        let result = executor.run_until(async {
            let handle = queue.spawn(async {
                panic!("Something went wrong!");
            });
            handle.await
        }).await;

        // Panic is caught and returned as JoinError::Panic
        match result {
            Err(JoinError::Panic(err)) => {
                println!("Task panicked: {}", err);
            }
            _ => {}
        }
    }).await;
}
```

## Architecture

Clockworker uses a two-level scheduling approach based on EEVDF:

1. **Queue-level scheduling (EEVDF)**: Fairly distributes CPU time between queues based on their weights using virtual runtime
2. **Task-level scheduling (FIFO)**: Within each queue, tasks execute in FIFO order

This design allows you to:
- Allocate CPU resources between different workload classes (via queue weights)
- Ensure predictable task ordering within each queue


## Requirements

- Rust 1.70+
- Works with any async runtime (tokio, smol, monoio, etc.) via `LocalSet` or similar

## License

Licensed under the Apache License, Version 2.0. See [LICENSE](LICENSE) for details.
