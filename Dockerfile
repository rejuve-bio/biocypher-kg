# BioCypher KG Console — single image running the FastAPI backend, serving the
# built React frontend at /console, and able to launch KG builds (which shell out
# to create_knowledge_graph.py). Because builds run with the repo root as cwd, the
# whole repository and its pipeline dependencies live in this image.
#
# No "# syntax=" directive on purpose: it makes BuildKit pull docker/dockerfile
# from Docker Hub before the build can start, which breaks on a flaky/offline
# network. This Dockerfile uses only standard features, so the built-in frontend
# is enough. Base images are ARGs so they can point at a mirror or local registry
# when Docker Hub is unreachable (e.g. NODE_IMAGE=<mirror>/node:20-slim).
#
# Build:  docker build -t biocypher-kg-console .
# Run:    docker run -p 9000:9000 biocypher-kg-console
# Open:   http://localhost:9000/console/

ARG NODE_IMAGE=node:20-slim
ARG UV_IMAGE=ghcr.io/astral-sh/uv:python3.10-bookworm-slim

# ---- Stage 1: build the frontend ----
FROM ${NODE_IMAGE} AS frontend
WORKDIR /fe
COPY kg-service/frontend/package.json kg-service/frontend/package-lock.json ./
RUN npm ci
COPY kg-service/frontend/ ./
RUN npm run build          # -> /fe/dist

# ---- Stage 2: python runtime (uv) ----
FROM ${UV_IMAGE} AS runtime
ENV DEBIAN_FRONTEND=noninteractive

# System libraries required to build/run the heavy pipeline deps
# (pysam, psycopg2, hgvs, owlready2, ...).
RUN apt-get update && apt-get install -y --no-install-recommends \
        build-essential \
        zlib1g-dev libbz2-dev liblzma-dev \
        libcurl4-openssl-dev libssl-dev \
        libpq-dev \
        git curl \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Install dependencies first (cached layer keyed on the lockfile).
COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-install-project

# Copy the repository (needed: builds run create_knowledge_graph.py from here).
COPY . .
# The shipped config, read-only. /app/config is each instance's own copy (a volume):
# edits made in the console stay there, and files it hasn't edited follow this one.
RUN rm -rf /app/config/__pycache__ && cp -a /app/config /app/.config-shipped

# Drop in the built frontend so the backend can serve it at /console.
COPY --from=frontend /fe/dist ./kg-service/frontend/dist

ENV REPO_ROOT=/app \
    BUILDS_DIR=/app/kg-service/.builds \
    UV_BIN=uv \
    SERVE_FRONTEND=true \
    PORT=9000 \
    PYTHONUNBUFFERED=1

EXPOSE 9000

# Run uvicorn from kg-service (so `backend.*` imports resolve); the venv's uvicorn
# is used directly. Build subprocesses still run `uv run ... ` with cwd=REPO_ROOT.
WORKDIR /app/kg-service
CMD ["sh", "-c", "/app/.venv/bin/uvicorn backend.api.main:app --host 0.0.0.0 --port ${PORT}"]
