# Single-stage build for clockworker benchmarks
FROM rust:slim-bookworm

# Install build dependencies
RUN apt-get update && apt-get install -y \
    build-essential \
    pkg-config \
    libssl-dev \
    && rm -rf /var/lib/apt/lists/*

# Create a working directory
WORKDIR /clockworker

# Copy only source files (not target directory or other build artifacts)
COPY Cargo.toml Cargo.lock* README.md ./
COPY src ./src
COPY benches ./benches

# Build the benchmarks in release mode
RUN cargo build --release --bench priority

# Default command runs the priority benchmark
CMD ["cargo", "bench", "--bench", "priority", "--", "--nocapture"]
