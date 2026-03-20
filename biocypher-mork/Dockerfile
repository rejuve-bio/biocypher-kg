FROM rustlang/rust:nightly

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
RUN git clone --depth=1 https://github.com/Adam-Vandervorst/PathMap.git /app/PathMap && \
    cd /app/PathMap && \
    cargo build --release

# Build MORK Server (server branch)
RUN git clone --depth=1 -b server https://github.com/Abdu1964/MORK.git /app/MORK_SERVER && \
    cd /app/MORK_SERVER/server && cargo build --release


# Create data, reports, and benchmarks directories
RUN mkdir -p /app/data /app/reports /app/benchmarks

# Link the server binary to a stable location
RUN ln -s /app/MORK_SERVER/target/release/mork-server /app/mork-server

# Copy entrypoint script
COPY entrypoint.sh /app/entrypoint.sh
RUN chmod +x /app/entrypoint.sh

# Create a user with UID 1000 (common default user ID)
RUN useradd -r -u 1000 -s /bin/false mork && \
    chown -R mork:mork /app && \
    chmod 755 /app/data /app/reports /app/benchmarks

# Switch to non-root user
USER mork

# Expose the server port
EXPOSE 8027

ENTRYPOINT ["/app/entrypoint.sh"]