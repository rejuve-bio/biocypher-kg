# Deployment Guide

This guide covers deploying the full BioCypher-KG system: the pipeline itself, Neo4j, the MORK server, and the kg-service API.

**Related docs:**
- [configuration.md](configuration.md) — all env vars and config fields
- [system-overview.md](../architecture/system-overview.md) — component overview

---

## Prerequisites

- Docker Engine 20.10+
- Docker Compose v2
- Python 3.10+ and [UV](https://github.com/astral-sh/uv)
- 16 GB RAM minimum (32 GB recommended for full human KG)
- 50+ GB disk space for full KG + dbSNP cache

---

## Option 1: Full pipeline via Docker Compose

The root [`docker-compose.yml`](https://github.com/rejuve-bio/biocypher-kg/blob/main/docker-compose.yml) defines 4 services that run sequentially:

| Service | Image | Purpose |
|---|---|---|
| `build` | `biocypher/base:1.2.0` | Run `scripts/build.sh` to generate KG output |
| `import` | `neo4j:4.4-enterprise` | Import generated CSV files into Neo4j |
| `deploy` | `neo4j:4.4-enterprise` | Run Neo4j as the production read-write endpoint |
| `mork` | Built from `biocypher-mork/Dockerfile` | MORK/Rust query server (port `8027`) |

```bash
# 1. Set environment variables
cp docker-variables.env .env   # or set them in shell

# 2. Set MORK data directory
export MORK_DATA_DIR=./output_human   # path to .metta output files

# 3. Start all services
docker compose up

# Start only MORK (if KG files already exist)
docker compose up mork
```

**Service dependencies:** `build` → `import` → `deploy`. MORK is independent.

### MORK port

The MORK container exposes port `8027` internally. The host port defaults to `8027` (configurable via `MORK_HOST_PORT` env var):

```yaml
ports:
  - "${MORK_HOST_PORT:-8027}:8027"
```

---

## Option 2: Production Neo4j via Makefile

The recommended path for production deployments uses the parameterised Neo4j compose file at [`docker/docker-compose.neo4j.yml`](https://github.com/rejuve-bio/biocypher-kg/blob/main/docker/docker-compose.neo4j.yml) with settings in `docker/neo4j.env`.

```bash
# 1. Copy and fill in the env file
cp docker/neo4j.env.example docker/neo4j.env
# Edit docker/neo4j.env: set NEO4J_AUTH, NEO4J_OUTPUT_DIR, NEO4J_ARCHIVE_DIR, passwords

# 2. Start Neo4j
make neo4j-up

# 3. Load KG data (incremental — detects changed datasets)
make neo4j-load

# 4. Load KG data (full reload — skips version check)
make neo4j-load-direct

# 5. View logs
make neo4j-logs

# 6. Stop and remove container
make neo4j-down
```

### Neo4j connection details

| Setting | Default | Description |
|---|---|---|
| Image | `neo4j:5.21` | Configurable via `NEO4J_IMAGE` |
| HTTP port (host) | `7674` | Neo4j Browser: `http://localhost:7674` |
| Bolt port (host) | `7887` | Driver URI: `bolt://localhost:7887` |
| Auth | `neo4j/your_password_here` | Set `NEO4J_AUTH=neo4j/<password>` |
| APOC | Enabled | Required for `LOAD CSV` operations |

### Memory tuning

Defaults in `docker/neo4j.env.example`:

```
NEO4J_PAGECACHE=4G
NEO4J_HEAP_INIT=2G
NEO4J_HEAP_MAX=4G
```

For full human KG (62k genes, 750k variants, 45M edges): increase to at least `8G`/`8G`/`8G`.

---

## Option 3: MORK standalone deployment

Run the MORK server independently from [`biocypher-mork/docker-compose.yml`](https://github.com/rejuve-bio/biocypher-kg/blob/main/biocypher-mork/docker-compose.yml):

```bash
cd biocypher-mork

# 1. Set MORK data path (directory containing .metta files)
export MORK_DATA_DIR=/path/to/metta/output

# 2. Start MORK
docker compose up -d

# 3. Check health
curl http://localhost:8027/status/-
```

**Persistent snapshots:** The `mork_persist/` directory (bind-mounted to `/app/persist`) stores WAL (`wal.metta`) and snapshot paths. The snapshot interval defaults to 300 seconds (`SNAPSHOT_INTERVAL_SECONDS`).

### ⚠️ Port conflict

The MORK container exposes port `8027`. The kg-service `Settings` model in `kg-service/backend/core/config.py` defaults `MORK_URL` to `http://localhost:8432`. **These are incompatible.** You must set:

```bash
# In kg-service/.env
MORK_URL=http://localhost:8027
```

or override `HOST_PORT` in the MORK compose to `8432`.

---

## kg-service deployment

The FastAPI application in `kg-service/` is not containerized — it runs as a Python process.

```bash
# 1. Set environment variables
cat > kg-service/.env << EOF
NEO4J_URI=bolt://localhost:7887
NEO4J_USER=neo4j
NEO4J_PASSWORD=your_password_here
NEO4J_DATABASE=neo4j
ARCHIVE_BASE=/path/to/biocypher-archives/
VERSION_DIFF_SCRIPT=/path/to/biocypher-kg/version_diff.py
MORK_SUMMARY_SCRIPT=/path/to/biocypher-kg/get_mork_summary.py
MORK_URL=http://localhost:8027
EOF

# 2. Install dependencies (from repo root)
uv sync

# 3. Start the service
cd kg-service
uv run uvicorn backend.api.main:app --host 0.0.0.0 --port 8000
```

> **Known issue:** `main.py` imports `meta` and `entities` from `backend.api.routes`, but neither file exists. The service will fail to start. This must be resolved first — either remove those imports or create the modules.

### Health check

```bash
curl http://localhost:8000/health
# {"status": "healthy", "neo4j": "connected"}

curl http://localhost:8000/
# {"name": "BioCypher KG Observatory", "version": "0.1.0", "docs": "/docs"}
```

---

## Running the pipeline (non-Docker)

```bash
# 1. Install dependencies
make setup          # installs UV and runs uv sync

# 2. Set BIOPORTAL_API_KEY (for Disease Ontology adapter)
cp .env.example .env
# Edit .env and fill in the API key

# 3. Sample run (uses bundled sample data, no external downloads)
make run-sample WRITER_TYPE=neo4j

# 4. Full human KG (requires downloading data first)
make download                  # interactive download UI
make run WRITER_TYPE=neo4j     # interactive run UI
```

See the top-level [README.md](https://github.com/rejuve-bio/biocypher-kg/blob/main/README.md) for the complete quickstart.

---

## Deployment checklist

- [ ] Copy `docker/neo4j.env.example` → `docker/neo4j.env`, fill in credentials and paths
- [ ] Copy `.env.example` → `.env`, fill in `BIOPORTAL_API_KEY`
- [ ] Set `MORK_URL=http://localhost:8027` in kg-service `.env` (override the 8432 default)
- [ ] Override `ARCHIVE_BASE`, `VERSION_DIFF_SCRIPT`, `MORK_SUMMARY_SCRIPT` in kg-service `.env`
- [ ] Tune Neo4j memory for dataset size (`NEO4J_PAGECACHE`, `NEO4J_HEAP_MAX`)
- [ ] Resolve the `meta.py`/`entities.py` import error in `kg-service/backend/api/main.py`
- [ ] For full (non-sample) runs: build dbSNP cache with `scripts/update_dbsnp.py` and set `dbsnp_cache_root` in `config/species_config.yaml`
