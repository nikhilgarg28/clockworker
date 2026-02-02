#[allow(dead_code)]
mod utils;
use clockworker::ExecutorBuilder;
use futures::future::select;
use futures::FutureExt;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::Duration;
use tabled::Table;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    task::LocalSet,
    time::{sleep, Duration as TokioDuration},
};

use crate::utils::Metrics;

// Configuration
const NUM_CACHE_ACTORS: usize = 8;
const NUM_DB_ACTORS: usize = 8;
const CACHE_MISS_RATE: f64 = 0.2; // 20% miss rate
const CPU_WORK_ITERATIONS: usize = 10_000; // Simulate CPU work
const IO_DELAY_US: u64 = 100; // 100us IO delay

// Message types for actor communication
struct CacheRequest {
    key: u64,
    response_tx: tokio::sync::oneshot::Sender<Option<Vec<u8>>>,
}

struct DbRequest {
    _key: u64, // Keep for potential future use (e.g., different DB queries based on key)
    response_tx: tokio::sync::oneshot::Sender<Vec<u8>>,
}

// Cache actor - simulates cache lookup with CPU work and IO
async fn cache_actor(mut rx: tokio::sync::mpsc::Receiver<CacheRequest>, miss_rate: f64) {
    while let Some(req) = rx.recv().await {
        // Simulate CPU work
        let mut _sum = 0u64;
        for _ in 0..CPU_WORK_ITERATIONS {
            _sum = _sum.wrapping_add(1);
        }

        // Simulate IO (cache lookup delay)
        sleep(TokioDuration::from_micros(IO_DELAY_US)).await;

        // Determine hit/miss based on miss rate
        let is_miss = (req.key as f64 % 100.0) < (miss_rate * 100.0);
        let result = if is_miss {
            None // Cache miss
        } else {
            Some(vec![1, 2, 3, 4]) // Cache hit - return dummy data
        };

        let _ = req.response_tx.send(result);
    }
}

// DB actor - simulates database lookup with CPU work and IO
async fn db_actor(mut rx: tokio::sync::mpsc::Receiver<DbRequest>) {
    while let Some(req) = rx.recv().await {
        // Simulate CPU work
        let mut _sum = 0u64;
        for _ in 0..CPU_WORK_ITERATIONS {
            _sum = _sum.wrapping_add(1);
        }

        // Simulate IO (database query delay)
        sleep(TokioDuration::from_micros(IO_DELAY_US)).await;

        // Return dummy data
        let data = vec![5, 6, 7, 8];
        let _ = req.response_tx.send(data);
    }
}

// Handle a single request - routes to cache/db and sends response
async fn handle_request(
    mut stream: TcpStream,
    cache_txs: Arc<Vec<tokio::sync::mpsc::Sender<CacheRequest>>>,
    db_txs: Arc<Vec<tokio::sync::mpsc::Sender<DbRequest>>>,
    request_counter: Arc<AtomicU64>,
) {
    let mut buf = vec![0; 8];
    loop {
        // Read request (8 bytes - just a key)
        match stream.read_exact(&mut buf).await {
            Ok(_) => {}
            Err(_) => return,
        }

        let key = u64::from_be_bytes([
            buf[0], buf[1], buf[2], buf[3], buf[4], buf[5], buf[6], buf[7],
        ]);
        let request_id = request_counter.fetch_add(1, Ordering::Relaxed);

        // Pick random cache actor
        let cache_idx = (request_id as usize) % cache_txs.len();
        let cache_tx = &cache_txs[cache_idx];

        // Send to cache actor
        let (tx, rx) = tokio::sync::oneshot::channel();
        let cache_req = CacheRequest {
            key,
            response_tx: tx,
        };

        if cache_tx.send(cache_req).await.is_err() {
            return;
        }

        // Wait for cache response
        let cache_result = match rx.await {
            Ok(result) => result,
            Err(_) => return,
        };

        let data = match cache_result {
            Some(data) => data, // Cache hit
            None => {
                // Cache miss - query DB
                let db_idx = (request_id as usize) % db_txs.len();
                let db_tx = &db_txs[db_idx];

                let (tx, rx) = tokio::sync::oneshot::channel();
                let db_req = DbRequest {
                    _key: key,
                    response_tx: tx,
                };

                if db_tx.send(db_req).await.is_err() {
                    return;
                }

                match rx.await {
                    Ok(data) => data,
                    Err(_) => return,
                }
            }
        };

        // Send response back to client
        if stream.write_all(&data).await.is_err() {
            return;
        }
    }
}

async fn serve_clockworker(addr: &str, shutdown: Arc<AtomicBool>, enable_lifo: bool) {
    let listener = TcpListener::bind(addr).await.unwrap();
    let executor = ExecutorBuilder::new()
        .with_queue(0, 1)
        .with_enable_lifo(enable_lifo)
        .build()
        .unwrap();
    let queue_handle = executor.queue(0).unwrap();

    let mut cache_txs = Vec::new();
    let mut db_txs = Vec::new();

    // Spawn cache actors
    for _ in 0..NUM_CACHE_ACTORS {
        let (tx, rx) = tokio::sync::mpsc::channel(1000);
        cache_txs.push(tx);
        let rx_clone = rx;
        queue_handle.spawn(async move {
            cache_actor(rx_clone, CACHE_MISS_RATE).await;
        });
    }

    // Spawn DB actors
    for _ in 0..NUM_DB_ACTORS {
        let (tx, rx) = tokio::sync::mpsc::channel(1000);
        db_txs.push(tx);
        let rx_clone = rx;
        queue_handle.spawn(async move {
            db_actor(rx_clone).await;
        });
    }

    let cache_txs = Arc::new(cache_txs);
    let db_txs = Arc::new(db_txs);
    let request_counter = Arc::new(AtomicU64::new(0));

    executor
        .run_until(async move {
            loop {
                if shutdown.load(Ordering::Relaxed) {
                    break;
                }

                let accept_fut = listener.accept();
                let timeout_fut = sleep(TokioDuration::from_millis(100));
                let accept_fut = accept_fut.boxed();
                let timeout_fut = timeout_fut.boxed();

                match select(accept_fut, timeout_fut).await {
                    futures::future::Either::Left((result, _)) => match result {
                        Ok((stream, _addr)) => {
                            let cache_txs_clone = cache_txs.clone();
                            let db_txs_clone = db_txs.clone();
                            let request_counter_clone = request_counter.clone();
                            queue_handle.spawn(handle_request(
                                stream,
                                cache_txs_clone,
                                db_txs_clone,
                                request_counter_clone,
                            ));
                        }
                        Err(_) => break,
                    },
                    futures::future::Either::Right((_, _)) => {
                        continue;
                    }
                }
            }
        })
        .await;
}

async fn serve_tokio(addr: &str, shutdown: Arc<AtomicBool>) {
    let listener = TcpListener::bind(addr).await.unwrap();
    let mut cache_txs = Vec::new();
    let mut db_txs = Vec::new();
    let request_counter = Arc::new(AtomicU64::new(0));

    // Spawn cache actors
    for _ in 0..NUM_CACHE_ACTORS {
        let (tx, rx) = tokio::sync::mpsc::channel(1000);
        cache_txs.push(tx);
        tokio::task::spawn_local(async move {
            cache_actor(rx, CACHE_MISS_RATE).await;
        });
    }

    // Spawn DB actors
    for _ in 0..NUM_DB_ACTORS {
        let (tx, rx) = tokio::sync::mpsc::channel(1000);
        db_txs.push(tx);
        tokio::task::spawn_local(async move {
            db_actor(rx).await;
        });
    }

    let cache_txs = Arc::new(cache_txs);
    let db_txs = Arc::new(db_txs);

    loop {
        if shutdown.load(Ordering::Relaxed) {
            break;
        }

        let accept_fut = listener.accept();
        let timeout_fut = sleep(TokioDuration::from_millis(100));
        let accept_fut = accept_fut.boxed();
        let timeout_fut = timeout_fut.boxed();

        match select(accept_fut, timeout_fut).await {
            futures::future::Either::Left((result, _)) => match result {
                Ok((stream, _addr)) => {
                    let cache_txs_clone = cache_txs.clone();
                    let db_txs_clone = db_txs.clone();
                    let request_counter_clone = request_counter.clone();
                    tokio::task::spawn_local(handle_request(
                        stream,
                        cache_txs_clone,
                        db_txs_clone,
                        request_counter_clone,
                    ));
                }
                Err(_) => break,
            },
            futures::future::Either::Right((_, _)) => {
                continue;
            }
        }
    }
}

fn generate_traffic(
    addr: String,
    num_connections: usize,
    shutdown: Arc<AtomicBool>,
) -> std::thread::JoinHandle<Metrics> {
    thread::spawn(move || {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let local = tokio::task::LocalSet::new();
        rt.block_on(local.run_until(async {
            let mut handles = Vec::new();
            for _ in 0..num_connections {
                if shutdown.load(Ordering::Relaxed) {
                    break;
                }
                let addr_clone = addr.clone();
                let shutdown_clone = shutdown.clone();
                handles.push(tokio::task::spawn_local(async move {
                    let mut durations = Vec::new();
                    let mut stream = match TcpStream::connect(&addr_clone).await {
                        Ok(s) => s,
                        Err(_) => return durations,
                    };
                    let mut key: u64 = 0;
                    while !shutdown_clone.load(Ordering::Relaxed) {
                        let start = std::time::Instant::now();
                        let key_bytes = key.to_be_bytes();
                        if stream.write_all(&key_bytes).await.is_err() {
                            break;
                        }
                        let mut response = vec![0; 4];
                        if stream.read_exact(&mut response).await.is_err() {
                            break;
                        }
                        let duration = std::time::Instant::now().duration_since(start);
                        durations.push(duration);
                        key += 1;
                    }
                    durations
                }));
            }
            let mut metrics = Metrics::new();
            for handle in handles {
                if let Ok(durations) = handle.await {
                    for duration in durations {
                        metrics.record(duration, &["request"]);
                    }
                }
            }
            metrics
        }))
    })
}

fn run_benchmark(
    addr: &str,
    num_connections: usize,
    spawn_server: impl FnOnce(Arc<AtomicBool>) -> std::thread::JoinHandle<()>,
) -> (f64, Metrics) {
    let shutdown = Arc::new(AtomicBool::new(false));

    // Start server thread
    let server_shutdown = shutdown.clone();
    let server_handle = spawn_server(server_shutdown);

    // Start client thread
    let client_shutdown = shutdown.clone();
    let addr_client = addr.to_string();
    let client_handle = generate_traffic(addr_client, num_connections, client_shutdown);

    // Sleep 5 seconds
    thread::sleep(Duration::from_secs(5));

    // Signal shutdown
    shutdown.store(true, Ordering::Release);

    // Wait for both threads
    server_handle.join().unwrap();
    let metrics = client_handle.join().unwrap();

    // Calculate throughput
    let total_requests = metrics.len();
    let throughput = total_requests as f64 / 5.0;

    (throughput, metrics)
}

fn print_comparison_table(results: &[(&str, f64, Metrics)]) {
    #[derive(tabled::Tabled)]
    struct ComparisonTable {
        runtime: String,
        throughput_req_s: String,
        p50_ms: String,
        p90_ms: String,
        p99_ms: String,
        p99_9_ms: String,
    }

    let mut rows = Vec::new();
    for (runtime, throughput, metrics) in results {
        rows.push(ComparisonTable {
            runtime: runtime.to_string(),
            throughput_req_s: format!("{:.2}", throughput),
            p50_ms: format_duration(metrics.quantile(50.0, "request")),
            p90_ms: format_duration(metrics.quantile(90.0, "request")),
            p99_ms: format_duration(metrics.quantile(99.0, "request")),
            p99_9_ms: format_duration(metrics.quantile(99.9, "request")),
        });
    }

    let table = Table::builder(rows).index().column(0).transpose().build();
    println!("\n=== PingPong Benchmark Results ===\n{}", table);
}

fn format_duration(d: Duration) -> String {
    let micros = d.as_micros();
    format!("{:.2}", micros as f64 / 1000.0)
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    // Skip bench name if present (args[1] might be "pingpong" when run via cargo bench)
    let base_addr = args
        .iter()
        .skip(1)
        .find(|s| s.contains(':'))
        .map(|s| s.as_str())
        .unwrap_or("127.0.0.1");

    let num_connections = 100;
    let mut results = Vec::new();

    // Run Tokio benchmark
    println!("Running Tokio benchmark...");
    {
        let addr = format!("{}:9999", base_addr);
        let addr_clone = addr.clone();
        let (throughput, metrics) = run_benchmark(&addr, num_connections, |shutdown| {
            let addr = addr_clone.clone();
            thread::spawn(move || {
                let rt = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .unwrap();
                let local = LocalSet::new();
                rt.block_on(local.run_until(serve_tokio(&addr, shutdown)));
            })
        });
        results.push(("Tokio", throughput, metrics));
    }

    // Run Clockworker+Tokio (without LIFO) benchmark
    println!("Running Clockworker+Tokio (no LIFO) benchmark...");
    {
        let addr = format!("{}:10000", base_addr);
        let addr_clone = addr.clone();
        let (throughput, metrics) = run_benchmark(&addr, num_connections, |shutdown| {
            let addr = addr_clone.clone();
            thread::spawn(move || {
                let rt = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .unwrap();
                let local = LocalSet::new();
                rt.block_on(local.run_until(serve_clockworker(&addr, shutdown, false)));
            })
        });
        results.push(("Clockworker+Tokio (no LIFO)", throughput, metrics));
    }

    // Run Clockworker+Tokio (with LIFO) benchmark
    println!("Running Clockworker+Tokio (with LIFO) benchmark...");
    {
        let addr = format!("{}:10001", base_addr);
        let addr_clone = addr.clone();
        let (throughput, metrics) = run_benchmark(&addr, num_connections, |shutdown| {
            let addr = addr_clone.clone();
            thread::spawn(move || {
                let rt = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .unwrap();
                let local = LocalSet::new();
                rt.block_on(local.run_until(serve_clockworker(&addr, shutdown, true)));
            })
        });
        results.push(("Clockworker+Tokio (with LIFO)", throughput, metrics));
    }

    // Print comparison table
    print_comparison_table(&results);
}
