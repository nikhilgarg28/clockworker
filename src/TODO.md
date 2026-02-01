<!-- 1) Add shutdown to runtime -->
<!-- 2) Add couple more schedulers e.g. SRTF, LAS -->
<!-- 3) Add tests that verify it can work with other runtimes -->
<!-- 4) Change class from i32 -> u8, don't use hashmap but vec<option<queue>> -->
<!-- 5) Base it on new linux scheduler - deadline, min vruntime, sched_latency, 
   <!-- min_slice, and driver_yield --> -->
<!-- 6) Build basic stats -->
<!-- 7) Allow queue to configure panic behavior -->
8) Allow queue to configure some debug on futures taking "too long"
<!-- 9) Allow spawned tasks to inherit parent's properties? -->
10) Enable LIFO optimization in some cases?
<!-- 11) Add yield_maybe -->
<!-- 12) Add test to verify bad executor can't be created (0 shares, duplicate ID) -->
<!-- 13) Add test to verify that panic on task causes the whole thread to panic -->
<!-- 14) Make key system generic over Eq + Sized -->
<!-- 15) Add arrival_time and root_arrival_time per task, send to enqueue. Or should
    we add parent_taskid? -->
<!-- 16) dont' make spawn return error - if not accepting, make tasks canceled -->
17) Option to enable stats
<!-- 18) Configuration builder (e.g. sched_latency etc.) -->
Remove mpsc2
<!-- Add method for with_queue and with_queue_having_scheduler -->
Change tcp benchmark to not have handle.await and instead do shutdown signal
Add fast path where there is only one queue - bypass vruntime etc.