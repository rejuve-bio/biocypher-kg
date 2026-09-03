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

# MORK pulls its core dependency PathMap from git `master` (unpinned) in its
# workspace Cargo.toml and ships no Cargo.lock, so every rebuild fetches PathMap
# HEAD. A PathMap API change then silently breaks the MORK kernel build with
# mismatched-type errors in kernel/src/space.rs. Pin PathMap to the commit that
# was master HEAD when MORK's `server` branch last changed (2026-03-09), for a
# reproducible build. Bump this only when intentionally moving MORK forward.
# (The standalone PathMap clone was dropped: MORK's Cargo.toml uses the git
# dependency, not a local path, so a separate clone was never actually used.)
ARG PATHMAP_REV=23f91cdbb7c4462d0f015767d9a4b3b33e2e7655
RUN git clone --depth=1 -b server https://github.com/Abdu1964/MORK.git /app/MORK_SERVER && \
    sed -i "s|^branch = \"master\"|rev = \"${PATHMAP_REV}\"|" /app/MORK_SERVER/Cargo.toml && \
    grep -q "rev = \"${PATHMAP_REV}\"" /app/MORK_SERVER/Cargo.toml && \
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