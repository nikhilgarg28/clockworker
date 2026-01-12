use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

/// Future that yields control to the outer runtime once
pub struct YieldOnce(bool);

pub fn yield_once() -> YieldOnce {
    YieldOnce(false)
}

impl Future for YieldOnce {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        if self.0 {
            Poll::Ready(())
        } else {
            self.0 = true;
            cx.waker().wake_by_ref(); // re-schedule ourselves
            Poll::Pending // yield to the outer runtime
        }
    }
}
