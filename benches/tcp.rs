mod utils;
use clockworker::ExecutorBuilder;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    task::LocalSet,
};

use crate::utils::Metrics;

const PACKET_SIZE: usize = 1024;

async fn handle_stream(mut stream: TcpStream) {
    let mut buf = vec![0; PACKET_SIZE];
    loop {
        match stream.read_exact(&mut buf).await {
            Ok(_) => {}
            Err(_) => return,
        }
        match stream.write_all(&buf).await {
            Ok(_) => {}
            Err(_) => return,
        }
    }
}

async fn serve_clockworker(addr: &str, num_connections: usize) {
    let listener = TcpListener::bind(addr).await.unwrap();
    let mut handles = Vec::new();

    let executor = ExecutorBuilder::new().with_queue(0, 1).build().unwrap();
    let queue = executor.queue(0).unwrap();

    executor
        .run_until(async move {
            for _ in 0..num_connections {
                let (stream, _addr) = match listener.accept().await {
                    Ok(conn) => conn,
                    Err(_) => break,
                };
                handles.push(queue.spawn(handle_stream(stream)));
            }
            for handle in handles {
                let _ = handle.await;
            }
        })
        .await;
}

async fn serve_tokio(addr: &str, num_connections: usize) {
    let listener = TcpListener::bind(addr).await.unwrap();
    let mut handles = Vec::new();
    for _ in 0..num_connections {
        let (stream, _addr) = match listener.accept().await {
            Ok(conn) => conn,
            Err(_) => break,
        };
        handles.push(tokio::task::spawn_local(handle_stream(stream)));
    }
    for handle in handles {
        let _ = handle.await;
    }
}

fn generate_traffic(
    addr: String,
    num_connections: usize,
    num_messages: usize,
) -> std::thread::JoinHandle<Metrics> {
    std::thread::spawn(move || {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let local = tokio::task::LocalSet::new();
        rt.block_on(local.run_until(async {
            let mut handles = Vec::new();
            for _ in 0..num_connections {
                let addr_clone = addr.clone();
                handles.push(tokio::task::spawn_local(async move {
                    let mut durations = Vec::new();
                    let mut stream = TcpStream::connect(&addr_clone).await.unwrap();
                    let mut buf = vec![0; PACKET_SIZE];
                    let num_reqs = num_messages / num_connections;
                    for _ in 0..num_reqs {
                        let start = std::time::Instant::now();
                        stream.write_all(&buf).await.unwrap();
                        let _ = stream.read_exact(&mut buf).await.unwrap();
                        let duration = std::time::Instant::now().duration_since(start);
                        durations.push(duration);
                    }
                    durations
                }));
            }
            let mut metrics = Metrics::new();
            for handle in handles {
                let durations = handle.await.unwrap();
                for duration in durations {
                    metrics.record(duration, &["write"]);
                }
            }
            metrics
        }))
    })
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let addr = args
        .get(2)
        .map(|s| s.as_str())
        .unwrap_or("127.0.0.1:9999")
        .to_string();

    // spin a thread to generate traffic
    let num_connections = 100;
    let num_messages = 1000_000;

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();

    // first run tokio
    rt.block_on(async {
        let handle = generate_traffic(addr.clone(), num_connections, num_messages);
        println!("Running Tokio...");
        let start = std::time::Instant::now();
        let local = LocalSet::new();
        local.run_until(serve_tokio(&addr, num_connections)).await;
        let end = std::time::Instant::now();
        let duration = end.duration_since(start);
        println!("Time taken: {:?}", duration);
        let throughput = num_messages as f64 / duration.as_secs_f64();
        println!("Throughput: {:.2} req/s", throughput);
        let metrics = handle.join().unwrap();
        println!("Latency (p99.9): {:?}", metrics.quantile(99.9, "write"));
    });
    // now run clockworker
    rt.block_on(async {
        println!("Running Clockworker...");
        let handle = generate_traffic(addr.clone(), num_connections, num_messages);
        let start = std::time::Instant::now();
        let local = LocalSet::new();
        local
            .run_until(serve_clockworker(&addr, num_connections))
            .await;
        let end = std::time::Instant::now();
        let duration = end.duration_since(start);
        println!("Time taken: {:?}", duration);
        let throughput = num_messages as f64 / duration.as_secs_f64();
        println!("Throughput: {:.2} req/s", throughput);
        let metrics = handle.join().unwrap();
        println!("Latency (p99.9): {:?}", metrics.quantile(99.9, "write"));
    });
}
