use crate::{queue::TaskId, scheduler::Scheduler};
use ahash::AHashMap;
use std::{collections::BTreeMap, time::Instant};

/// Schedules that picks tasks on the basis of lease accessed service time.
/// For each group, stores (total service time, list of tasks within the group).
pub struct LAS {
    // stores (service time, task id) for runnable tasks
    // group's service time as of when task was enqueued
    runnable: BTreeMap<(u128, TaskId), ()>,
    // maps group id to total service time
    service: AHashMap<u64, u128>,
}

impl LAS {
    pub fn new() -> Self {
        Self {
            runnable: BTreeMap::new(),
            service: AHashMap::new(),
        }
    }
}
impl Scheduler for LAS {
    fn push(&mut self, id: TaskId, gid: u64, _at: Instant) {
        let service = self.service.get(&gid).map_or(0, |s| *s);
        self.runnable.insert((service, id), ());
    }
    fn pop(&mut self) -> Option<TaskId> {
        let entry = self.runnable.pop_first();
        if entry.is_none() {
            return None;
        }
        let ((_service, id), _) = entry.unwrap();
        Some(id)
    }
    fn clear_task_state(&mut self, _id: TaskId, _gid: u64) {}

    fn clear_group_state(&mut self, gid: u64) {
        self.service.remove(&gid);
    }

    fn is_runnable(&self) -> bool {
        !self.runnable.is_empty()
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
    fn test_las() {
        let mut scheduler = LAS::new();
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

        // pop task 1 from group 1
        assert_eq!(scheduler.pop(), Some(1));
        assert!(scheduler.is_runnable());

        // next pop should give task 3 from group 1
        assert_eq!(scheduler.pop(), Some(3));
        assert!(scheduler.is_runnable());

        // next pop should give task 2 from group 0
        assert_eq!(scheduler.pop(), Some(2));
        assert!(!scheduler.is_runnable());
        // now observe that task 3 ran for 1 nanoseconds
        let start = Instant::now();
        let end = start + std::time::Duration::from_nanos(1);
        scheduler.observe(3, 1, start, end, true);
        assert_eq!(*scheduler.service.get(&1).unwrap(), 1);
        assert_eq!(*scheduler.service.get(&0).unwrap(), 2);

        // now enqueue task to both groups 0 and 1
        scheduler.push(4, 0, now);
        scheduler.push(5, 1, now);

        // next pop should give task 5 from group 1
        assert_eq!(scheduler.pop(), Some(5));
        assert!(scheduler.is_runnable());

        assert_eq!(scheduler.pop(), Some(4));
        assert!(!scheduler.is_runnable());
    }
}
