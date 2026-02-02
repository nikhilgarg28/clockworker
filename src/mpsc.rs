//! # Lock-free MPSC Queue (Vyukov Algorithm)
//!
//! A multi-producer single-consumer queue optimized for async executor task scheduling.
//!
//! ## Algorithm
//!
//! Based on Dmitry Vyukov's MPSC queue. Key properties:
//! - **Push**: One atomic swap + one atomic store (no CAS loop)
//! - **Pop**: Loads only in common case (single consumer = no contention)
//! - **Trade-off**: One allocation per push (vs SegQueue's amortized allocation)

use std::cell::UnsafeCell;
use std::mem::MaybeUninit;
use std::ptr;
use std::sync::atomic::{AtomicPtr, Ordering};

/// Internal queue node. Allocated on push, freed on pop.
struct Node<T> {
    value: MaybeUninit<T>,
    next: AtomicPtr<Node<T>>,
}

/// Lock-free multi-producer single-consumer queue.
///
/// ## Thread Safety
/// - `push`: Safe from any thread, concurrently
/// - `pop`: Single consumer only (executor thread)
///
/// This is a pure queue - no length tracking or waker management.
/// Those are handled by TaskQueue.
#[derive(Debug)]
pub struct Mpsc<T> {
    /// Points to the most recently pushed node.
    head: AtomicPtr<Node<T>>,
    /// Points to the sentinel node. Only consumer accesses.
    tail: UnsafeCell<*mut Node<T>>,
    /// The permanent stub node (initial sentinel).
    stub: *mut Node<T>,
}

unsafe impl<T: Send> Send for Mpsc<T> {}
unsafe impl<T: Send> Sync for Mpsc<T> {}

impl<T> Mpsc<T> {
    pub fn new() -> Self {
        let stub = Box::into_raw(Box::new(Node {
            value: MaybeUninit::uninit(),
            next: AtomicPtr::new(ptr::null_mut()),
        }));

        Self {
            head: AtomicPtr::new(stub),
            tail: UnsafeCell::new(stub),
            stub,
        }
    }

    /// Enqueues an item.
    pub fn push(&self, item: T) {
        let node = Box::into_raw(Box::new(Node {
            value: MaybeUninit::new(item),
            next: AtomicPtr::new(ptr::null_mut()),
        }));

        // Step 1: Claim spot by swapping head
        let prev = self.head.swap(node, Ordering::AcqRel);

        // Step 2: Link previous node to us
        // SAFETY: prev is valid (stub or previous node not yet freed)
        unsafe {
            (*prev).next.store(node, Ordering::Release);
        }
    }

    /// Attempts to pop one item.
    ///
    /// # Safety Contract
    /// Must only be called from the single consumer thread.
    #[inline]
    pub fn pop(&self) -> Option<T> {
        // SAFETY: Single consumer invariant upheld by caller (executor)
        unsafe { self.pop_inner() }
    }

    #[inline]
    unsafe fn pop_inner(&self) -> Option<T> {
        let tail = *self.tail.get();
        let next = (*tail).next.load(Ordering::Acquire);

        if next.is_null() {
            // Check if truly empty or producer mid-push
            let head = self.head.load(Ordering::Acquire);
            if tail == head {
                return None; // Truly empty
            }
            // Producer mid-push, spin for next
            return self.spin_pop_inner(tail);
        }

        // Read value from NEXT (not tail!)
        let value = (*next).value.assume_init_read();

        // Advance tail to next (next becomes new sentinel)
        *self.tail.get() = next;

        // Free old sentinel (unless it's the stub)
        if tail != self.stub {
            drop(Box::from_raw(tail));
        }

        Some(value)
    }

    /// Spin waiting for producer to finish linking, then pop.
    #[cold]
    #[inline(never)]
    unsafe fn spin_pop_inner(&self, tail: *mut Node<T>) -> Option<T> {
        let mut spin = 0u32;
        loop {
            std::hint::spin_loop();

            let next = (*tail).next.load(Ordering::Acquire);
            if !next.is_null() {
                let value = (*next).value.assume_init_read();
                *self.tail.get() = next;
                if tail != self.stub {
                    drop(Box::from_raw(tail));
                }
                return Some(value);
            }

            spin += 1;
            if spin > 1000 {
                #[cfg(debug_assertions)]
                panic!("MPSC spin limit exceeded");
                #[cfg(not(debug_assertions))]
                return None;
            }
        }
    }
}

impl<T> Default for Mpsc<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T> Drop for Mpsc<T> {
    fn drop(&mut self) {
        // Drain remaining items
        // SAFETY: We have &mut self, so no concurrent access
        unsafe { while self.pop_inner().is_some() {} }

        // Free the final sentinel
        unsafe {
            let tail = *self.tail.get();
            drop(Box::from_raw(tail));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::thread;

    #[test]
    fn test_basic_push_pop() {
        let q = Mpsc::new();

        q.push(1);
        q.push(2);
        q.push(3);

        assert_eq!(q.pop(), Some(1));
        assert_eq!(q.pop(), Some(2));
        assert_eq!(q.pop(), Some(3));
        assert_eq!(q.pop(), None);
    }

    #[test]
    fn test_fifo_order() {
        let q = Mpsc::new();

        for i in 0..100 {
            q.push(i);
        }

        for i in 0..100 {
            assert_eq!(q.pop(), Some(i));
        }
        assert_eq!(q.pop(), None);
    }

    #[test]
    fn test_concurrent_push() {
        let q = Arc::new(Mpsc::new());
        let num_threads = 32;
        let items_per_thread = 10_000;

        let handles: Vec<_> = (0..num_threads)
            .map(|t| {
                let q = Arc::clone(&q);
                thread::spawn(move || {
                    for i in 0..items_per_thread {
                        q.push(t * items_per_thread + i);
                    }
                })
            })
            .collect();

        for h in handles {
            h.join().unwrap();
        }

        let mut count = 0;
        let mut last_per_thread = vec![None; num_threads];
        while let Some(v) = q.pop() {
            count += 1;
            let thread_id = v / items_per_thread;
            let last = last_per_thread[thread_id];
            assert!(last.is_none() || last.unwrap() + 1 == v);
            last_per_thread[thread_id] = Some(v);
        }
        assert_eq!(count, num_threads * items_per_thread);
    }

    #[test]
    fn test_empty_queue() {
        let q: Mpsc<i32> = Mpsc::new();

        assert_eq!(q.pop(), None);
        assert_eq!(q.pop(), None);
    }

    #[test]
    fn test_drop_with_items() {
        let q = Mpsc::new();
        for i in 0..100 {
            q.push(Box::new(i));
        }
        drop(q);
    }
}
