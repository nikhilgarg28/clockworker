# Clockworker Benchmarks

## Setup

All benchmarks require a Tokio runtime. Run them using:

```bash
cargo bench --bench <benchmark_name>
```

## Available Benchmarks

### `overhead`
Measures executor overhead compared to Tokio baseline:
- Task spawn throughput
- Yield/poll overhead
- IO reactor integration

**Usage**:
```bash
cargo bench --bench overhead
```

### `priority`
Compares Clockworker's fair scheduling with Tokio when running foreground and background tasks simultaneously.

**Usage**:
```bash
cargo bench --bench priority
```

### `tcp`
A 'real-world' benchmark stress-testing TCP based ping/pong workload.

**Usage**:
```bash
cargo bench --bench tcp [address]
# Example: cargo bench --bench tcp 127.0.0.1
```

### `pingpong`
Actor-based ping-pong benchmark with cache and database actors. Compares:
- Tokio (baseline)
- Clockworker + Tokio (without LIFO)
- Clockworker + Tokio (with LIFO)

**Usage**:
```bash
cargo bench --bench pingpong [address]
# Example: cargo bench --bench pingpong 127.0.0.1
```

### `tail`
Tail latency benchmark measuring p50, p90, p99, p99.9 latencies when the workload
consists of a mixture of thin and fat tasks.

**Usage**:
```bash
cargo bench --bench tail
```

### `overhead_profile`
Profiling benchmark for performance analysis. Use with `cargo flamegraph`:

**Usage**:
```bash
# Regular run
cargo bench --bench overhead_profile

# With flamegraph (requires: cargo install flamegraph)
cargo flamegraph --bench overhead_profile
```

## Configuration

Most benchmarks accept optional command-line arguments. Check individual benchmark files for details.

## Results

Results are printed to stdout in table format. Some benchmarks also output CSV files for further analysis.
