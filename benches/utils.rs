use smol::future::FutureExt;
use std::{
    ops::{Deref, DerefMut},
    task::Poll,
    time::{Duration, Instant},
};

// ============================================================================
// Work Task
// ============================================================================

#[derive(Clone)]
pub enum Step {
    CPU(Duration),
    Sleep(Duration),
    Yield,
}

#[derive(Clone)]
pub struct Work {
    steps: Vec<Step>,
}
impl Work {
    pub fn new(steps: Vec<Step>) -> Self {
        Self { steps }
    }

    pub async fn run(&self) {
        for step in self.steps.iter() {
            match step {
                Step::CPU(duration) => Self::do_cpu_work(*duration),
                Step::Sleep(duration) => tokio::time::sleep(*duration).await,
                Step::Yield => tokio::task::yield_now().await,
            }
        }
    }

    /// Do approximately `duration` of CPU work
    #[inline(never)]
    fn do_cpu_work(duration: Duration) {
        let start = Instant::now();
        let mut acc: u64 = 0;
        while start.elapsed() < duration {
            for _ in 0..1000 {
                acc = acc.wrapping_mul(6364136223846793005).wrapping_add(1);
            }
            std::hint::black_box(acc);
        }
    }
}

// ============================================================================
// Latency Statistics
// ============================================================================

#[derive(Debug, Clone)]
struct Point {
    duration: Duration,
    tags: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct Metrics {
    iters: Vec<Point>,
}

impl Metrics {
    pub fn new() -> Self {
        Self { iters: Vec::new() }
    }
    pub fn record(&mut self, lat: Duration, tags: &[&str]) {
        self.iters.push(Point {
            duration: lat,
            tags: tags.iter().map(|s| s.to_string()).collect(),
        });
    }
    pub fn quantile(&self, q: f64, tag: &str) -> Duration {
        let mut iters = self
            .iters
            .iter()
            .filter(|p| p.tags.contains(&tag.to_string()))
            .map(|p| p.duration)
            .collect::<Vec<_>>();
        if iters.is_empty() {
            return Duration::ZERO;
        }
        iters.sort();
        let idx = ((q / 100.0) * (iters.len() - 1) as f64).round() as usize;
        iters[idx.min(iters.len() - 1)]
    }
    pub fn len(&self) -> u64 {
        self.iters.len() as u64
    }
    pub fn mean(&self, tag: &str) -> Duration {
        let mut sum = Duration::ZERO;
        let mut count = 0;
        for p in self.iters.iter() {
            if p.tags.contains(&tag.to_string()) {
                sum += p.duration;
                count += 1;
            }
        }
        if count == 0 {
            return Duration::ZERO;
        }
        sum / count
    }
    pub fn stddev(&self, tag: &str) -> Duration {
        let mean = self.mean(tag);
        let mut sum = 0;
        let mut count = 0;
        for p in self.iters.iter() {
            if p.tags.contains(&tag.to_string()) {
                let diff = p.duration.as_nanos().saturating_sub(mean.as_nanos());
                sum += diff * diff;
                count += 1;
            }
        }
        if count == 0 {
            return Duration::ZERO;
        }
        let variance = sum as f64 / count as f64;
        let stddev = variance.sqrt();
        Duration::from_nanos(stddev as u64)
    }
}

#[derive(Clone)]
pub enum Executor {
    Clockworker {
        executor: std::rc::Rc<clockworker::Executor<u8>>,
        local: std::rc::Rc<tokio::task::LocalSet>,
    },
    Tokio {
        local: std::rc::Rc<tokio::task::LocalSet>,
    },
}

pub enum Handle<T> {
    Clockworker(clockworker::JoinHandle<T, u8>),
    Tokio(tokio::task::JoinHandle<T>),
}

impl<T> std::future::Future for Handle<T> {
    type Output = Result<T, ()>;
    fn poll(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Self::Output> {
        match self.deref_mut() {
            Handle::Clockworker(handle) => match handle.poll(cx) {
                Poll::Ready(r) => Poll::Ready(r.map_err(|_| ())),
                Poll::Pending => Poll::Pending,
            },
            Handle::Tokio(handle) => match handle.poll(cx) {
                Poll::Ready(r) => Poll::Ready(r.map_err(|_| ())),
                Poll::Pending => Poll::Pending,
            },
        }
    }
}

impl Executor {
    pub async fn start_tokio(local: tokio::task::LocalSet) -> Self {
        Self::Tokio {
            local: std::rc::Rc::new(local),
        }
    }

    pub async fn start_clockworker(
        executor: std::rc::Rc<clockworker::Executor<u8>>,
        local: tokio::task::LocalSet,
    ) -> Self {
        let executor_clone = executor.clone();
        local.spawn_local(async move {
            executor_clone.run().await;
        });
        Self::Clockworker {
            executor,
            local: std::rc::Rc::new(local),
        }
    }
    pub fn spawn<T: 'static>(
        &self,
        fut: impl std::future::Future<Output = T> + 'static,
    ) -> Handle<T> {
        match self.clone() {
            Executor::Clockworker { executor, .. } => {
                let queue = executor.queue(0).unwrap();
                Handle::Clockworker(queue.spawn(fut))
            }
            Executor::Tokio { local } => Handle::Tokio(local.spawn_local(fut)),
        }
    }
    pub async fn run_until<T>(&self, fut: impl std::future::Future<Output = T> + 'static) -> T {
        match self.clone() {
            Executor::Clockworker { local, .. } => local.run_until(fut).await,
            Executor::Tokio { local } => local.run_until(fut).await,
        }
    }
}

/// Generate exponentially distributed inter-arrival time
/// For Poisson process with rate λ, inter-arrival times are Exp(λ)
/// Mean inter-arrival time = 1/λ
pub fn exponential_delay(rng: &mut impl rand::Rng, mean: Duration) -> Duration {
    let u: f64 = rng.gen(); // uniform [0, 1)
                            // Inverse transform: -ln(1-u) * mean, but -ln(u) works since u is uniform
    let u = u.max(f64::EPSILON);
    let multiplier = -u.ln();
    Duration::from_secs_f64(mean.as_secs_f64() * multiplier)
}
