use crate::task::TaskHeader;
use futures::task::AtomicWaker;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};
/// Error returned from awaiting a JoinHandle.
#[derive(Debug, PartialEq)]
pub enum JoinError {
    Cancelled,
    ResultTaken,
}
#[derive(Debug)]
pub struct JoinState<T> {
    done: std::sync::atomic::AtomicBool,
    // Stored exactly once by the winner; guarded by state.
    result: std::sync::Mutex<Option<Result<T, JoinError>>>,
    waker: AtomicWaker,
}

impl<T> JoinState<T> {
    pub fn new() -> Self {
        Self {
            done: std::sync::atomic::AtomicBool::new(false),
            result: std::sync::Mutex::new(None),
            waker: AtomicWaker::new(),
        }
    }

    #[inline]
    pub fn is_done(&self) -> bool {
        self.done.load(Ordering::Acquire)
    }

    /// Attempt to complete with Ok(val). Returns true if we won.
    pub fn try_complete_ok(&self, val: T) -> bool {
        if self
            .done
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return false;
        }
        *self.result.lock().unwrap() = Some(Ok(val));
        self.waker.wake();
        true
    }

    /// Attempt to complete with Cancelled. Returns true if we won.
    pub fn try_complete_cancelled(&self) -> bool {
        if self
            .done
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return false;
        }
        *self.result.lock().unwrap() = Some(Err(JoinError::Cancelled));
        self.waker.wake();
        true
    }

    /// Called by JoinHandle::poll after it sees is_done().
    /// Consumes the result exactly once.
    fn take_result(&self) -> Result<T, JoinError> {
        let mut g = self.result.lock().unwrap();
        if g.is_none() {
            return Err(JoinError::ResultTaken);
        }
        g.take().unwrap()
    }
}

/// A JoinHandle that detaches on drop, and supports explicit abort().
#[derive(Clone, Debug)]
pub struct JoinHandle<T> {
    header: Arc<TaskHeader>,
    join: Arc<JoinState<T>>,
}

impl<T> JoinHandle<T> {
    pub fn new(header: Arc<TaskHeader>, join: Arc<JoinState<T>>) -> Self {
        Self { header, join }
    }
    pub fn abort(&self) {
        self.header.cancel();
        self.join.try_complete_cancelled();
        self.header.enqueue();
    }
}

impl<T> Future for JoinHandle<T> {
    type Output = Result<T, JoinError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        if self.join.is_done() {
            return Poll::Ready(self.join.take_result());
        }
        self.join.waker.register(cx.waker());
        // Re-check after registering to avoid missed wake.
        if self.join.is_done() {
            return Poll::Ready(self.join.take_result());
        }
        Poll::Pending
    }
}
