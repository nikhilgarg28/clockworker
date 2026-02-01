#!/usr/bin/env python3
"""
Aggregate priority benchmark results from multiple runs.

Usage:
    # Run benchmark 3 times in Docker and aggregate:
    ./scripts/aggregate_benchmark.py --runs 3 --docker

    # Parse existing output from stdin:
    cat benchmark_output.txt | ./scripts/aggregate_benchmark.py

    # Run locally (not in Docker):
    ./scripts/aggregate_benchmark.py --runs 3
"""

import argparse
import subprocess
import sys
from statistics import median
from typing import Dict, List, Optional


def run_benchmark_docker() -> str:
    """Run the benchmark in Docker with optimized settings."""
    cmd = [
        "docker", "run", "--rm",
        "--privileged",
        "--cpuset-cpus=0,1",
        "--memory=4g",
        "--memory-swappiness=0",
        "clockworker-bench"
    ]
    result = subprocess.run(cmd, capture_output=True, text=True)
    return result.stdout + result.stderr


def run_benchmark_local() -> str:
    """Run the benchmark locally."""
    cmd = ["cargo", "bench", "--bench", "priority", "--", "--nocapture"]
    result = subprocess.run(cmd, capture_output=True, text=True)
    return result.stdout + result.stderr


def parse_benchmark_output(output: str) -> Dict[str, List[float]]:
    """Parse benchmark output and extract metrics."""
    metrics = {}

    for line in output.split('\n'):
        line = line.strip()
        if not line.startswith('|'):
            continue
        if 'name' in line.lower() or '---' in line:
            continue

        parts = [p.strip() for p in line.split('|')[1:-1]]
        if len(parts) < 6:
            continue

        metric = parts[0]
        if not metric or metric == 'name':
            continue

        try:
            # Parse values, handling 'ms' suffix and plain numbers
            values = []
            for p in parts[1:6]:
                p = p.replace('ms', '').replace('/s', '').strip()
                values.append(float(p))
            metrics[metric] = values
        except (ValueError, IndexError):
            continue

    return metrics


def aggregate_runs(all_runs: List[Dict[str, List[float]]]) -> Dict[str, List[float]]:
    """Compute median of each metric across all runs."""
    if not all_runs:
        return {}

    aggregated = {}
    all_metrics = set()
    for run in all_runs:
        all_metrics.update(run.keys())

    for metric in all_metrics:
        medians = []
        for config_idx in range(5):  # 5 configurations
            values = [
                run[metric][config_idx]
                for run in all_runs
                if metric in run and len(run[metric]) > config_idx
            ]
            if values:
                medians.append(median(values))
            else:
                medians.append(None)
        aggregated[metric] = medians

    return aggregated


def format_value(value: Optional[float], metric: str, baseline: Optional[float] = None) -> str:
    """Format a value with optional % change from baseline."""
    if value is None:
        return "-"

    if 'bg_iter' in metric:
        formatted = f"{value:.0f}"
    else:
        formatted = f"{value:.2f}ms"

    # Add % change if baseline provided
    if baseline is not None and baseline > 0:
        pct_change = ((value - baseline) / baseline) * 100
        if pct_change > 0:
            formatted += f" (+{pct_change:.0f}%)"
        elif pct_change < 0:
            formatted += f" ({pct_change:.0f}%)"
        else:
            formatted += " (baseline)"

    return formatted


def print_table(aggregated: Dict[str, List[float]], show_pct: bool = True,
                include_queue: bool = True, include_latency: bool = True,
                include_throughput: bool = True):
    """Print the final comparison table."""
    # Original order from benchmark output
    # Index: 0=CW(fg+bg), 1=CW(fg only), 2=Tokio(fg+bg), 3=Tokio(fg only), 4=Two-RT/OS
    # Reorder to put baseline (Tokio fg only) first
    orig_configs = ['CW (fg+bg)', 'CW (fg only)', 'Tokio (fg+bg)', 'Tokio (fg only)', 'Two-RT/OS']
    # New order: baseline first, then others
    new_order = [3, 0, 1, 2, 4]  # Tokio(fg only), CW(fg+bg), CW(fg only), Tokio(fg+bg), Two-RT/OS
    configs = [orig_configs[i] for i in new_order]
    baseline_idx = 0  # Tokio (fg only) is now first

    all_metrics = [
        ('p50_queue_delay', 'p50 queue delay', 'queue'),
        ('p90_queue_delay', 'p90 queue delay', 'queue'),
        ('p99_queue_delay', 'p99 queue delay', 'queue'),
        ('p50_total_latency', 'p50 total latency', 'latency'),
        ('p90_total_latency', 'p90 total latency', 'latency'),
        ('p99_total_latency', 'p99 total latency', 'latency'),
        ('bg_iter_s', 'BG throughput', 'throughput'),
    ]

    # Filter metrics based on flags
    metrics_to_show = []
    for key, name, category in all_metrics:
        if category == 'queue' and not include_queue:
            continue
        if category == 'latency' and not include_latency:
            continue
        if category == 'throughput' and not include_throughput:
            continue
        metrics_to_show.append((key, name))

    # Calculate column widths
    col_widths = [18] + [22] * 5

    # Print header
    header = "| {:^{}} |".format("Metric", col_widths[0])
    for i, config in enumerate(configs):
        header += " {:^{}} |".format(config, col_widths[i+1])
    print(header)

    # Print separator
    sep = "|" + "|".join(["-" * (w + 2) for w in col_widths]) + "|"
    print(sep)

    # Print rows
    for metric_key, metric_name in metrics_to_show:
        if metric_key not in aggregated:
            continue

        orig_values = aggregated[metric_key]
        # Reorder values to match new column order
        values = [orig_values[i] if i < len(orig_values) else None for i in new_order]
        baseline = values[baseline_idx] if show_pct else None

        row = "| {:^{}} |".format(metric_name, col_widths[0])
        for i, val in enumerate(values):
            # Don't show % for baseline column or bg throughput
            if i == baseline_idx or 'bg_iter' in metric_key:
                formatted = format_value(val, metric_key, None)
            else:
                formatted = format_value(val, metric_key, baseline)
            row += " {:^{}} |".format(formatted, col_widths[i+1])
        print(row)


def main():
    parser = argparse.ArgumentParser(
        description='Aggregate priority benchmark results',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s --runs 3 --docker                  # Run 3 times in Docker
  %(prog)s --runs 3 --docker --no-queue       # Exclude queue delay metrics
  %(prog)s --runs 3 --docker --latency-only   # Only show total latency metrics
  %(prog)s --runs 3 --docker --no-pct         # Hide percentage changes
        """
    )
    parser.add_argument('--runs', type=int, default=3, help='Number of benchmark runs')
    parser.add_argument('--docker', action='store_true', help='Run in Docker')
    parser.add_argument('--no-pct', action='store_true', help='Hide percentage changes')
    parser.add_argument('--no-queue', action='store_true', help='Exclude queue delay metrics')
    parser.add_argument('--no-latency', action='store_true', help='Exclude total latency metrics')
    parser.add_argument('--no-throughput', action='store_true', help='Exclude throughput metrics')
    parser.add_argument('--latency-only', action='store_true', help='Only show total latency + throughput')
    parser.add_argument('--queue-only', action='store_true', help='Only show queue delay + throughput')
    args = parser.parse_args()

    # Handle convenience flags
    include_queue = not args.no_queue
    include_latency = not args.no_latency
    include_throughput = not args.no_throughput

    if args.latency_only:
        include_queue = False
        include_latency = True
        include_throughput = True
    if args.queue_only:
        include_queue = True
        include_latency = False
        include_throughput = True

    all_runs = []

    if sys.stdin.isatty() or args.docker or args.runs > 0:
        # Run benchmarks (either explicitly requested or no stdin)
        for i in range(args.runs):
            print(f"Running benchmark {i+1}/{args.runs}...", file=sys.stderr)
            if args.docker:
                output = run_benchmark_docker()
            else:
                output = run_benchmark_local()

            metrics = parse_benchmark_output(output)
            if metrics:
                all_runs.append(metrics)
                print(f"  Collected {len(metrics)} metrics", file=sys.stderr)
            else:
                print(f"  Warning: No metrics parsed from run {i+1}", file=sys.stderr)

    if not all_runs and not sys.stdin.isatty():
        # Parse from stdin
        output = sys.stdin.read()

        # Split by run markers if present
        if '========== RUN' in output:
            parts = output.split('========== RUN')
            for part in parts[1:]:  # Skip first empty part
                metrics = parse_benchmark_output(part)
                if metrics:
                    all_runs.append(metrics)
        else:
            # Single run
            metrics = parse_benchmark_output(output)
            if metrics:
                all_runs.append(metrics)

    if not all_runs:
        print("Error: No benchmark data collected", file=sys.stderr)
        sys.exit(1)

    print(f"\nAggregating {len(all_runs)} runs (median):\n", file=sys.stderr)

    aggregated = aggregate_runs(all_runs)
    print_table(
        aggregated,
        show_pct=not args.no_pct,
        include_queue=include_queue,
        include_latency=include_latency,
        include_throughput=include_throughput
    )


if __name__ == '__main__':
    main()
