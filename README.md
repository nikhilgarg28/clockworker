# Clockworker

Clockworker, loosely inspired by Seastar, is a single-threaded async executor
with powerful, pluggable scheduling. Clockworker is agnostic to the underlying
async runtime and can sit on top of any runtime like Tokio, Monoio, or Smol.

**⚠️ Early/Alpha Release**: This project is in early development. APIs may change in breaking ways between versions. Use at your own risk.

## What is Clockworker for?

There is a class of settings where single-threaded async runtimes are a great fit.
Several such runtimes exist in the Rust ecosystem—Tokio, Monoio, Glommio, etc. But
almost none of these (with the exception of Glommio) provide the ability to run
multiple configurable work queues with different priorities. This becomes important
for many real-world single-threaded systems, at minimum to separate foreground and
background work. Clockworker aims to solve this problem.

It does so via work queues with configurable time-shares onto which tasks can be
spawned. Clockworker has a two-level scheduler: the top-level scheduler chooses the
queue to poll based on its fair time share (inspired by Linux CFS/EEVDF), and then
a task is chosen from that queue based on a queue-specific scheduler, which is
fully pluggable—you can use one of the built-in schedulers or write your own by
implementing a simple trait.

Note that Clockworker itself is just an executor loop, not a full async runtime, and
is designed to sit on top of any other runtime. 

## Features

- **EEVDF-based queue scheduling**: Fair CPU time distribution between queues using virtual runtime (inspired by Linux CFS/EEVDF)
- **Pluggable task schedulers**: Choose how tasks are ordered within each queue
- **Task cancellation**: Abort running tasks via `JoinHandle::abort()`
- **Panic handling**: Configurable panic behavior (propagate or catch as `JoinError::Panic`)
- **Statistics**: Built-in metrics for monitoring executor and queue performance

## Pre-written schedulers
- **LAS (Least Attained Service)**: Recommended for latency-sensitive workloads; good
  at minimizing tail latencies.
- **RunnableFifo**: Simple FIFO ordering based on when tasks become runnable,
  good for throughput.
- **ArrivalFifo**: FIFO ordering based on task arrival time

## Quick Start

Add to your `Cargo.toml`:

```toml
[dependencies]
clockworker = "0.1.0"
```

## Examples

### Basic Usage

The simplest example - spawn a task and wait for it:

```rust
use clockworker::{ExecutorBuilder, scheduler};
use tokio::task::LocalSet;

#[tokio::main(flavor = "current_thread")]
async fn main() {
    let local = LocalSet::new();
    local.run_until(async {
        // Create executor with a single queue using LAS scheduler
        let executor = ExecutorBuilder::new()
            .with_queue(0, 1, scheduler::LAS::new())
            .build()
            .unwrap();

        // Get handle to the queue
        let queue = executor.queue(0).unwrap();

        // Spawn executor in background
        local.spawn_local(async move {
            executor.run().await;
        });

        // Spawn a task
        let handle = queue.spawn(async {
            println!("Hello from clockworker!");
            42
        });

        // Wait for task to complete
        let result = handle.await;
        println!("Task result: {:?}", result); // Ok(42)
    }).await;
}
```

### Multiple Queues with Different Weights

Allocate CPU time proportionally between queues:

```rust
use clockworker::{ExecutorBuilder, scheduler, yield_maybe};
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
        // - Foreground: weight 8 (gets 8/9 of CPU time)
        // - Background: weight 1 (gets 1/9 of CPU time)
        // Note: Queue IDs can be enums (as shown here) or integers, strings, or any
        // type implementing QueueKey
        let executor = ExecutorBuilder::new()
            .with_queue(Queue::Foreground, 8, scheduler::LAS::new())  // High priority queue
            .with_queue(Queue::Background, 1, scheduler::LAS::new())  // Low priority queue
            .build()
            .unwrap();

        let executor_clone = executor.clone();
        local.spawn_local(async move {
            executor_clone.run().await;
        });

        let high_count = Arc::new(AtomicU32::new(0));
        let low_count = Arc::new(AtomicU32::new(0));

        // Spawn tasks in both queues
        let high_queue = executor.queue(Queue::Foreground).unwrap();
        let low_queue = executor.queue(Queue::Background).unwrap();

        high_queue.spawn({
            let count = high_count.clone();
            async move {
                loop {
                    count.fetch_add(1, Ordering::Relaxed);
                    yield_maybe().await;
                }
            }
        });

        low_queue.spawn({
            let count = low_count.clone();
            async move {
                loop {
                    count.fetch_add(1, Ordering::Relaxed);
                    yield_maybe().await;
                }
            }
        });

        // After running for a bit, high_count should be ~8x low_count
        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
        println!("High: {}, Low: {}", 
                 high_count.load(Ordering::Relaxed),
                 low_count.load(Ordering::Relaxed));
    }).await;
}
```

### Task Cancellation

Cancel tasks using `JoinHandle::abort()`:

```rust
use clockworker::{ExecutorBuilder, scheduler};
use tokio::task::LocalSet;
use tokio::time::{sleep, Duration};

#[tokio::main(flavor = "current_thread")]
async fn main() {
    let local = LocalSet::new();
    local.run_until(async {
        let executor = ExecutorBuilder::new()
            .with_queue(0, 1, scheduler::LAS::new())
            .build()
            .unwrap();

        let executor_clone = executor.clone();
        local.spawn_local(async move {
            executor_clone.run().await;
        });

        let queue = executor.queue(0).unwrap();
        
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
}
```

### Panic Handling

By default, the executor also panics when any of the tasks panic (same behavior
as Tokio's single-threaded runtime). However, this can be configured:

```rust
use clockworker::{ExecutorBuilder, scheduler, JoinError};
use tokio::task::LocalSet;

#[tokio::main(flavor = "current_thread")]
async fn main() {
    let local = LocalSet::new();
    local.run_until(async {
        // Configure executor to catch panics instead of crashing
        let executor = ExecutorBuilder::new()
            .with_queue(0, 1, scheduler::LAS::new())
            .with_panic_on_task_panic(false)  // Catch panics
            .build()
            .unwrap();

        let executor_clone = executor.clone();
        local.spawn_local(async move {
            executor_clone.run().await;
        });

        let queue = executor.queue(0).unwrap();
        let handle = queue.spawn(async {
            panic!("Something went wrong!");
        });

        // Panic is caught and returned as JoinError::Panic
        match handle.await {
            Err(JoinError::Panic(err)) => {
                println!("Task panicked: {}", err);
            }
            _ => {}
        }
    }).await;
}
```

### Task Grouping

Group related tasks together for better scheduling. This can be useful for
policies across tenants, gRPC streams, noisy clients, or even cases where a task
spawns many child tasks and you want to make lineage-aware scheduling choices.

```rust
use clockworker::{ExecutorBuilder, scheduler};
use tokio::task::LocalSet;

#[tokio::main(flavor = "current_thread")]
async fn main() {
    let local = LocalSet::new();
    local.run_until(async {
        let executor = ExecutorBuilder::new()
            .with_queue(0, 1, scheduler::LAS::new())
            .build()
            .unwrap();

        let executor_clone = executor.clone();
        local.spawn_local(async move {
            executor_clone.run().await;
        });

        let queue = executor.queue(0).unwrap();

        // Group tasks by tenant ID. The hash of group IDs is passed to the queue
        // scheduler, so you can configure any behavior you want.
        queue.group("tenant1").spawn(async { /* some task */ });
        queue.group("tenant2").spawn(async { /* some task */ });
    }).await;
}
```

## Choosing a Scheduler

### LAS (Least Attained Service) - Recommended for Latency

**Use QLAS when you need low latency and fair scheduling:**

```rust
use clockworker::{ExecutorBuilder, scheduler::QLAS};

let _executor = ExecutorBuilder::new()
    .with_queue(0, 1, QLAS::new())
    .build()
    .unwrap();
```

LAS prioritizes tasks that have received the least CPU time, which helps ensure:
- Low tail latencies
- Fair CPU distribution within groups
- Better responsiveness for interactive workloads

### RunnableFifo

**Use RunnableFifo for simple FIFO ordering:**

```rust
use clockworker::{ExecutorBuilder, scheduler::RunnableFifo};

let _executor = ExecutorBuilder::new()
    .with_queue(0, 1, RunnableFifo::new())
    .build()
    .unwrap();
```

Tasks are ordered by when they become runnable (not arrival time). If a task goes to sleep and wakes up, it goes to the back of the queue.

### ArrivalFifo

**Use ArrivalFifo for strict arrival-time ordering:**

```rust
use clockworker::{ExecutorBuilder, scheduler::ArrivalFifo};

let _executor = ExecutorBuilder::new()
    .with_queue(0, 1, ArrivalFifo::new())
    .build()
    .unwrap();
```
Tasks maintain their position based on when they were first spawned, even if they go to sleep.

## Architecture

Clockworker uses a two-level scheduling approach:

1. **Queue-level scheduling (EEVDF)**: Fairly distributes CPU time between queues based on their weights using virtual runtime
2. **Task-level scheduling (pluggable)**: Within each queue, a scheduler (LAS, RunnableFifo, etc.) chooses which task to run next

This design allows you to:
- Allocate CPU resources between different workload classes (via queue weights)
- Control latency and fairness within each class (via task schedulers)

## Requirements

- Rust 1.70+
- Works with any async runtime (tokio, smol, monoio, etc.) via `LocalSet` or similar

## License

Licensed under the Apache License, Version 2.0. See [LICENSE](LICENSE) for details.
