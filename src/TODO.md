1) Add shutdown to runtime
2) Add couple more schedulers e.g. SRTF
3) Add tests that verify it can work with other runtimes
4) Change class from i32 -> u8, don't use hashmap but vec<option<queue>>
5) Base it on new linux scheduler - deadline, min vruntime, sched_latency, 
   min_slice, and driver_yield
6) Build basic stats
7) Allow queue to configure panic behavior
8) Allow queue to configure some debug on futures taking "too long"
9) Allow spawned tasks to inherit parent's properties?
10) Enable LIFO optimization in some cases?