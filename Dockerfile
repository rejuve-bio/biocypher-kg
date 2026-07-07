# syntax=docker/dockerfile:1
#
# BioCypher KG Console — single image running the FastAPI backend, serving the
# built React frontend at /console, and able to launch KG builds (which shell out
# to create_knowledge_graph.py). Because builds run with the repo root as cwd, the
# whole repository and its pipeline dependencies live in this image.
#
# Build:  docker build -t biocypher-kg-console .
# Run:    docker run -p 8000:8000 biocypher-kg-console
# Open:   http://localhost:8000/console/

# ---- Stage 1: build the frontend ----
FROM node:20-slim AS frontend
WORKDIR /fe
COPY kg-service/frontend/package.json kg-service/frontend/package-lock.json ./
RUN npm ci
COPY kg-service/frontend/ ./
RUN npm run build          # -> /fe/dist

# ---- Stage 2: python runtime (uv) ----
FROM ghcr.io/astral-sh/uv:python3.10-bookworm-slim AS runtime
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

# Drop in the built frontend so the backend can serve it at /console.
COPY --from=frontend /fe/dist ./kg-service/frontend/dist

ENV REPO_ROOT=/app \
    BUILDS_DIR=/app/kg-service/.builds \
    UV_BIN=uv \
    SERVE_FRONTEND=true \
    PORT=8000 \
    PYTHONUNBUFFERED=1

EXPOSE 8000

# Run uvicorn from kg-service (so `backend.*` imports resolve); the venv's uvicorn
# is used directly. Build subprocesses still run `uv run ... ` with cwd=REPO_ROOT.
WORKDIR /app/kg-service
CMD ["sh", "-c", "/app/.venv/bin/uvicorn backend.api.main:app --host 0.0.0.0 --port ${PORT}"]
