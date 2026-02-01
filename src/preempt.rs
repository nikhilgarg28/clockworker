use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
/// A 256-bit atomic bitmask for tracking which queues would preempt the current one.
/// Supports up to 256 queues (4 x 64 bits).
pub struct PreemptMask {
    bits: [AtomicU64; 4],
}

impl std::fmt::Debug for PreemptMask {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PreemptMask")
            .field("bits[0]", &self.bits[0].load(Ordering::Relaxed))
            .field("bits[1]", &self.bits[1].load(Ordering::Relaxed))
            .field("bits[2]", &self.bits[2].load(Ordering::Relaxed))
            .field("bits[3]", &self.bits[3].load(Ordering::Relaxed))
            .finish()
    }
}

impl PreemptMask {
    pub fn new() -> Self {
        Self {
            bits: [
                AtomicU64::new(0),
                AtomicU64::new(0),
                AtomicU64::new(0),
                AtomicU64::new(0),
            ],
        }
    }

    /// Set bit at index i (0-255)
    #[inline]
    pub fn set(&self, i: u8) {
        let i = i as usize;
        let word = i / 64;
        let bit = i % 64;
        self.bits[word].fetch_or(1 << bit, Ordering::Release);
    }

    /// Clear all bits
    #[inline]
    pub fn clear(&self) {
        for word in &self.bits {
            word.store(0, Ordering::Release);
        }
    }

    /// Check if bit at index i is set
    #[inline]
    pub fn contains(&self, i: u8) -> bool {
        let i = i as usize;
        let word = i / 64;
        let bit = i % 64;
        (self.bits[word].load(Ordering::Acquire) >> bit) & 1 == 1
    }
}

/// Shared preemption state accessible by all task wakers.
/// When a task enqueues to a queue that would have higher priority than the
/// currently running queue, it sets the preempt flag. The executor checks this
/// flag after each poll and yields early if set.
pub struct PreemptState {
    /// Bitmask of queue indices that would preempt the current queue
    mask: PreemptMask,
    /// Flag set by wakers when they enqueue to a higher-priority queue
    preempt: AtomicBool,
}

impl std::fmt::Debug for PreemptState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PreemptState")
            .field("mask", &self.mask)
            .field("preempt", &self.preempt.load(Ordering::Relaxed))
            .finish()
    }
}

impl PreemptState {
    pub fn new() -> Self {
        Self {
            mask: PreemptMask::new(),
            preempt: AtomicBool::new(false),
        }
    }

    /// Check if preemption is requested and clear the flag
    #[inline]
    pub fn check(&self) -> bool {
        self.preempt.load(Ordering::Acquire)
    }

    /// Set preemption flag
    #[inline]
    pub fn request_preempt(&self) {
        self.preempt.store(true, Ordering::Release);
    }

    /// Clear preemption flag (called when selecting a new timeslice)
    #[inline]
    pub fn clear_preempt(&self) {
        self.preempt.store(false, Ordering::Release);
    }

    /// Check if a queue index would preempt the current queue
    #[inline]
    pub fn would_preempt(&self, qidx: usize) -> bool {
        self.mask.contains(qidx as u8)
    }

    /// Update the mask with queues that would preempt
    #[inline]
    pub fn update_mask<I: Iterator<Item = usize>>(&self, higher_priority_queues: I) {
        self.mask.clear();
        for qidx in higher_priority_queues {
            self.mask.set(qidx as u8);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_preemption_mask_basic() {
        // Test that PreemptMask can set, check, and clear bits
        let mask = PreemptMask::new();

        // Initially all bits should be unset
        assert!(!mask.contains(0));
        assert!(!mask.contains(100));
        assert!(!mask.contains(255));

        // Set some bits
        mask.set(0);
        mask.set(63);
        mask.set(64);
        mask.set(127);
        mask.set(128);
        mask.set(255);

        // Check they are set
        assert!(mask.contains(0));
        assert!(mask.contains(63));
        assert!(mask.contains(64));
        assert!(mask.contains(127));
        assert!(mask.contains(128));
        assert!(mask.contains(255));

        // Other bits should still be unset
        assert!(!mask.contains(1));
        assert!(!mask.contains(100));
        assert!(!mask.contains(200));

        // Clear all
        mask.clear();
        assert!(!mask.contains(0));
        assert!(!mask.contains(255));
    }

    #[test]
    fn test_preemption_state_check_clear() {
        // Test that check returns true only once
        let state = PreemptState::new();

        // Initially should be false
        assert!(!state.check());

        // Request preemption
        state.request_preempt();

        // check should return true
        assert!(state.check());

        // clear preemption
        state.clear_preempt();
        assert!(!state.check());
    }
}
