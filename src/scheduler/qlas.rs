use crate::{queue::TaskId, scheduler::Scheduler};
use ahash::AHashMap as HashMap;
use std::collections::VecDeque;
use std::time::Instant;

/// Quantized LAS scheduler.
/// For each group, stores (total service time, list of tasks within the group).
/// Store groups in hierarchical queues - each covering 2^k to 2^(k+1) service time.
pub struct QLAS {
    // bitmask denoting if ith queue has any runnable tasks
    present: u32,

    // queues[i] contains tasks belonging to groups g such that when
    // task was enqueued, g's service time was in [2^i, 2^(i+1))
    queues: [VecDeque<TaskId>; 32],

    // service time for each group
    service: HashMap<u64, u128>,
}

impl QLAS {
    pub fn new() -> Self {
        let mut queues = Vec::with_capacity(32);
        for _ in 0..32 {
            queues.push(VecDeque::new());
        }
        let queues = queues.try_into().unwrap();
        Self {
            present: 0,
            queues,
            service: HashMap::new(),
        }
    }
}
impl Scheduler for QLAS {
    fn push(&mut self, id: TaskId, gid: u64, _at: Instant) {
        let service = self.service.get(&gid).map_or(0, |s| *s);
        let queue_idx = if service <= 1 {
            0
        } else {
            service.ilog2().min(31) as usize
        };
        self.queues[queue_idx].push_back(id);
        if self.present & (1 << queue_idx) == 0 {
            self.present |= 1 << queue_idx;
        }
    }
    fn pop(&mut self) -> Option<TaskId> {
        let present = self.present;
        for queue_idx in 0..32 {
            if present & (1 << queue_idx) != 0 {
                let q = &mut self.queues[queue_idx];
                let ret = q.pop_front();
                if q.is_empty() {
                    self.present &= !(1 << queue_idx);
                }
                return ret;
            }
        }
        None
    }
    fn clear_task_state(&mut self, _id: TaskId, _gid: u64) {}
    fn clear_group_state(&mut self, gid: u64) {
        self.service.remove(&gid);
    }
    fn is_runnable(&self) -> bool {
        self.present != 0
    }
    fn observe(&mut self, _id: TaskId, gid: u64, start: Instant, end: Instant, _ready: bool) {
        let old = self.service.get(&gid).map_or(0, |s| *s);
        self.service
            .insert(gid, old + end.duration_since(start).as_nanos());
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_qlas() {
        let mut scheduler = QLAS::new();
        let now = Instant::now();
        //initially not runnable but it will be after pushing a task
        assert!(!scheduler.is_runnable());
        scheduler.push(0, 0, now);
        scheduler.push(1, 1, now);
        assert!(scheduler.is_runnable());

        // initially all groups have total service time 0
        // so it returns task 0 from group 0
        assert_eq!(scheduler.pop(), Some(0));

        // now observe that task 0 was run for 2 nanoseconds
        let start = Instant::now();
        let end = start + std::time::Duration::from_nanos(2);
        scheduler.observe(0, 0, start, end, true);
        assert_eq!(*scheduler.service.get(&0).unwrap(), 2);

        // now group 0 has service time 2 and group 1 has no service time
        // enqueue a task to both groups 0 and 1
        scheduler.push(2, 0, now);
        scheduler.push(3, 1, now);

        // first pop should give task 1 from group 1, and then task 3 from group 0
        assert_eq!(scheduler.pop(), Some(1));
        assert_eq!(scheduler.pop(), Some(3));
        assert_eq!(scheduler.pop(), Some(2));
        assert!(!scheduler.is_runnable());

        // now observe that task 3 ran for 6 nanoseconds
        // and task 2 for 8 nanoseconds
        let start = Instant::now();
        let end = start + std::time::Duration::from_nanos(6);
        scheduler.observe(3, 1, start, end, true);
        let start = Instant::now();
        let end = start + std::time::Duration::from_nanos(8);
        scheduler.observe(2, 0, start, end, true);
        assert_eq!(*scheduler.service.get(&0).unwrap(), 10);
        assert_eq!(*scheduler.service.get(&1).unwrap(), 6);

        // 10 and 6 go to different queues
        // add two tasks to group 0 and one to group 1
        scheduler.push(4, 0, now);
        scheduler.push(5, 1, now);

        // now we should get task 5 from group 1
        assert_eq!(scheduler.pop(), Some(5));
        assert!(scheduler.is_runnable());

        // now observe that task 5 ran for 3 nanoseconds
        let start = Instant::now();
        let end = start + std::time::Duration::from_nanos(3);
        scheduler.observe(5, 1, start, end, true);
        assert_eq!(*scheduler.service.get(&1).unwrap(), 9);

        // now add a task to group 0 and one to group 1
        scheduler.push(7, 1, now);
        scheduler.push(6, 0, now);

        // at this point, group 0 has service time of 10, and group 1 has service time of 9
        // both have ilog = 3 so are in same queue, we now get in order of
        // insertion
        assert_eq!(scheduler.pop(), Some(4));
        assert_eq!(scheduler.pop(), Some(7));
        assert_eq!(scheduler.pop(), Some(6));
        assert!(!scheduler.is_runnable());
    }
}
