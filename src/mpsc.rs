use core::task::{Context, Poll};
use crossbeam_queue::SegQueue;
use futures_util::future::poll_fn;
use futures_util::task::AtomicWaker;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

#[derive(Debug)]
pub struct Mpsc<T> {
    q: SegQueue<T>,
    len: AtomicUsize,
    waker: AtomicWaker,
    closed: AtomicBool,
}

impl<T> Mpsc<T> {
    pub fn new() -> Self {
        Self {
            q: SegQueue::new(),
            len: AtomicUsize::new(0),
            waker: AtomicWaker::new(),
            closed: AtomicBool::new(false),
        }
    }

    /// Called from spawn() and from wake() paths (sync, from any thread).
    pub fn enqueue(&self, item: T) -> Result<(), ()> {
        if self.closed.load(Ordering::Acquire) {
            return Err(());
        }
        self.q.push(item);

        // Increment length and wake if queue was empty (old value was 0)
        let old_len = self.len.fetch_add(1, Ordering::Release);
        if old_len == 0 {
            self.waker.wake();
        }
        Ok(())
    }

    pub fn close(&self) {
        self.closed.store(true, Ordering::Release);
    }

    /// Check if the queue has items (non-blocking).
    /// This uses the atomic length counter for accurate checking.
    pub fn is_empty(&self) -> bool {
        self.len.load(Ordering::Acquire) == 0
    }

    pub fn len(&self) -> usize {
        self.len.load(Ordering::Acquire)
    }

    /// Register a waker to be notified when items are enqueued.
    /// Used by the executor to wait for tasks without allocating futures.
    pub fn register_waker(&self, waker: &std::task::Waker) {
        self.waker.register(waker);
    }

    /// Non-async drain used by the executor's run loop.
    /// Returns number of items drained into `out`.
    pub fn try_drain<const N: usize>(&self, out: &mut [T; N]) -> usize {
        let mut n = 0;
        while n < N {
            match self.q.pop() {
                Some(v) => {
                    out[n] = v;
                    n += 1;
                }
                None => break,
            }
        }

        // Decrement length for each item we actually popped
        if n > 0 {
            self.len.fetch_sub(n, Ordering::Release);
        }

        n
    }
    pub fn pop(&self) -> Option<T> {
        match self.q.pop() {
            Some(v) => {
                self.len.fetch_sub(1, Ordering::Release);
                Some(v)
            }
            None => None,
        }
    }

    /// Async receive for cases where you want to park until something arrives.
    pub async fn recv_one(&self) -> Result<T, ()> {
        Ok(poll_fn(|cx| self.poll_recv_one(cx)).await)
    }

    fn poll_recv_one(&self, cx: &mut Context<'_>) -> Poll<T> {
        if let Some(v) = self.q.pop() {
            // Decrement length for the item we just popped
            self.len.fetch_sub(1, Ordering::Release);
            return Poll::Ready(v);
        }

        // Register then re-check to avoid missed wakeups.
        self.waker.register(cx.waker());
        if let Some(v) = self.q.pop() {
            // Decrement length for the item we just popped
            self.len.fetch_sub(1, Ordering::Release);
            return Poll::Ready(v);
        }

        Poll::Pending
    }

    /// Wait for the queue to become non-empty without consuming any items.
    /// This is useful for the executor to wake up when tasks arrive,
    /// without consuming the task IDs (which will be drained by the scheduler).
    pub async fn wait(&self) {
        poll_fn(|cx| self.poll_wait(cx)).await
    }

    fn poll_wait(&self, cx: &mut Context<'_>) -> Poll<()> {
        // Fast path: check if already non-empty
        if !self.is_empty() {
            return Poll::Ready(());
        }

        // Register waker so we get notified when items arrive
        self.waker.register(cx.waker());

        // Re-check after registering to avoid missed wakeups
        if !self.is_empty() {
            return Poll::Ready(());
        }

        Poll::Pending
    }
}
