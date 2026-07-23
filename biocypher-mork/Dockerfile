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

# gxhash (a PathMap/MORK dependency) needs CPU intrinsics enabled at build time
# (aes+neon on ARM / aes on x86). Without this, the build fails with:
#   "Gxhash requires aes and neon intrinsics ... build with RUSTFLAGS=..."
# target-cpu=native enables whatever the build host supports (works on Apple
# Silicon and x86 alike for a locally-built image).
ENV RUSTFLAGS="-C target-cpu=native"

# Build PathMap
RUN git clone --depth=1 https://github.com/Adam-Vandervorst/PathMap.git /app/PathMap && \
    cd /app/PathMap && \
    cargo build --release

# Build MORK Server (server branch)
RUN git clone --depth=1 -b server https://github.com/Abdu1964/MORK.git /app/MORK_SERVER && \
    cd /app/MORK_SERVER/server && cargo build --release

# RUN git clone https://github.com/trueagi-io/MORK.git /app/MORK && \  
#     cd /app/MORK/kernel && \
#     cargo build --release

# Create data, reports, benchmarks, and persist directories. /app/persist must exist
# and be mork-owned so a named volume mounted there inherits that ownership (a fresh
# named volume copies the image dir's ownership; otherwise it comes up root-owned and
# the entrypoint can't write its snapshot/WAL).
RUN mkdir -p /app/data /app/reports /app/benchmarks /app/persist

# Link the server binary to a stable location
RUN ln -s /app/MORK_SERVER/target/release/mork-server /app/mork-server

# Copy entrypoint script
COPY entrypoint.sh /app/entrypoint.sh
RUN chmod +x /app/entrypoint.sh

# Create a user with UID 1000 (common default user ID)
RUN useradd -r -u 1000 -s /bin/false mork && \
    chown -R mork:mork /app && \
    chmod 755 /app/data /app/reports /app/benchmarks /app/persist

# Switch to non-root user
USER mork

# Expose the server port
EXPOSE 8027

ENTRYPOINT ["/app/entrypoint.sh"]