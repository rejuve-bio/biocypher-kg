FROM rust:latest

# Install dependencies including curl for healthcheck
RUN apt-get update && apt-get install -y \
    build-essential \
    cmake \
    git \
    python3 \
    python3-pip \
    curl && \
    rm -rf /var/lib/apt/lists/*

# Set working dir
WORKDIR /app

# Build PathMap
RUN git clone https://github.com/Adam-Vandervorst/PathMap.git /app/PathMap && \
    cd /app/PathMap && \
    cargo build --release

# Build MORK (server branch)
RUN git clone -b server https://github.com/Abdu1964/MORK.git /app/MORK && \
    cd /app/MORK/server && \
    cargo build --release

# Create data, reports, and benchmarks directories
RUN mkdir -p /app/data /app/reports /app/benchmarks

# Create a user with UID 1000 (common default user ID)
RUN useradd -r -u 1000 -s /bin/false mork && \
    chown -R mork:mork /app/data /app/reports /app/benchmarks && \
    chmod 755 /app/data /app/reports /app/benchmarks

# Switch to non-root user
USER mork

# Expose the server port
EXPOSE 8027

# Add health check
HEALTHCHECK --interval=30s --timeout=3s --start-period=5s --retries=3 \
    CMD curl -f http://localhost:8027/status/- || exit 1

# Run the built MORK server binary
CMD ["/app/MORK/target/release/mork-server", "--data-dir", "/app/data", "--host", "0.0.0.0", "--port", "8027"]