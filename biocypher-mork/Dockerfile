FROM rustlang/rust:nightly

# Build deps (curl for the healthcheck).
RUN apt-get update && apt-get install -y \
    build-essential \
    cmake \
    git \
    python3 \
    python3-pip \
    curl && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app

# gxhash (a MORK dep) needs CPU intrinsics; target-cpu=native enables them on ARM/x86.
ENV RUSTFLAGS="-C target-cpu=native"

# Pin nightly: rustlang/rust:nightly is unpinned, so a broken nightly can break the build.
ARG RUST_NIGHTLY=2026-07-01
RUN rustup toolchain install "nightly-${RUST_NIGHTLY}" --profile minimal && \
    rustup default "nightly-${RUST_NIGHTLY}"

RUN git clone --depth=1 https://github.com/Adam-Vandervorst/PathMap.git /app/PathMap && \
    cd /app/PathMap && \
    cargo build --release

RUN git clone --depth=1 -b server https://github.com/Abdu1964/MORK.git /app/MORK_SERVER && \
    cd /app/MORK_SERVER/server && cargo build --release

# RUN git clone https://github.com/trueagi-io/MORK.git /app/MORK && \  
#     cd /app/MORK/kernel && \
#     cargo build --release

# /app/persist must be mork-owned so a named volume mounted there inherits that ownership.
RUN mkdir -p /app/data /app/reports /app/benchmarks /app/persist

RUN ln -s /app/MORK_SERVER/target/release/mork-server /app/mork-server

COPY entrypoint.sh /app/entrypoint.sh
RUN chmod +x /app/entrypoint.sh

RUN useradd -r -u 1000 -s /bin/false mork && \
    chown -R mork:mork /app && \
    chmod 755 /app/data /app/reports /app/benchmarks /app/persist

USER mork

EXPOSE 8027

ENTRYPOINT ["/app/entrypoint.sh"]