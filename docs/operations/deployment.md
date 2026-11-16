# Deployment Guide

This is the single guide for deploying BioCypher-KG and everything it depends on: the pipeline host, Neo4j, the MORK server, kg-service, the optional MeTTa consumers, and the external services the adapters connect to. Every command, port and default here was checked against the code on `main`; where a path is currently broken, this guide says so and links to [Known issues](#known-issues).

**Related docs:**
- [configuration.md](configuration.md) — pipeline config files and CLI flags
- [troubleshooting.md](troubleshooting.md) — symptoms and fixes
- [README.md](https://github.com/rejuve-bio/biocypher-kg/blob/main/README.md) — building the KG (`make run`, `make download`)

---

## At a glance

| Component | What it is | How it runs | Host port(s) | Status on `main` |
|---|---|---|---|---|
| [Pipeline host](#pipeline-host) | `create_knowledge_graph.py`, which builds the KG files | `uv` on the host (`make run*`) | — | Works |
| [Neo4j](#neo4j) | Graph database for the `neo4j` writer's output | [`docker/docker-compose.neo4j.yml`](https://github.com/rejuve-bio/biocypher-kg/blob/main/docker/docker-compose.neo4j.yml) via `make neo4j-*` | `7674` HTTP, `7887` Bolt | Works |
| [MORK](#mork) | MeTTa graph server for the `metta` writer's output | [`biocypher-mork/`](https://github.com/rejuve-bio/biocypher-kg/tree/main/biocypher-mork) Docker image | `8027` | Works, with setup caveats |
| [kg-service](#kg-service) | FastAPI service for graph metadata and version history | `uvicorn` on the host, or inside the Web Console image | `8000` | Works; some endpoints have [known issues](#kg-service-issues) |
| [Web Console](#web-console) | Browser UI that downloads data, builds, loads and explores, per species | Docker: [`docker/docker-compose.yml`](https://github.com/rejuve-bio/biocypher-kg/blob/main/docker/docker-compose.yml) or [`docker/docker-compose.dist.yml`](https://github.com/rejuve-bio/biocypher-kg/blob/main/docker/docker-compose.dist.yml) | picked by `up.sh` (`9000` with the dist compose) | Works |
| [MeTTa consumers](#optional-metta-consumers) | Hyperon in-process space, DAS | Host scripts | — | Optional; not project dependencies |
| [External services](#external-services) | Data-source hosts, FlyBase Chado, BioPortal, NCBI | Outbound network | — | — |

```mermaid
flowchart LR
    EXT["External services<br/>data sources, FlyBase Chado,<br/>BioPortal, NCBI"] --> P["Pipeline host<br/>create_knowledge_graph.py"]
    P -- "--writer-type neo4j<br/>CSV + .cypher" --> N[("Neo4j<br/>:7887")]
    P -- "--writer-type metta<br/>.metta" --> M[("MORK<br/>:8027")]
    P -- "prolog, parquet,<br/>KGX, networkx" --> F["Files only"]
    N --> K["kg-service<br/>:8000"]
    M -.-> K
    C["Web Console"] -- "runs builds" --> P
    C -- "per-species loads" --> N
    C -- "per-species loads" --> M
```

Neo4j, the root-compose MORK and the Web Console publish their ports on `127.0.0.1` only. The standalone MORK in `biocypher-mork/` publishes `8027` on all interfaces, so firewall it on shared hosts.

---

## Prerequisites

| Requirement | Needed for | Notes |
|---|---|---|
| Docker Engine with Compose v2 (`docker compose`) | Neo4j, MORK | |
| Python 3.10–3.12 | Pipeline, loaders, kg-service | `pyproject.toml` requires `>=3.10,<3.13`; `.python-version` pins 3.10 |
| [uv](https://github.com/astral-sh/uv) | Pipeline | `make setup` installs it if missing |
| `libpq-dev` (provides `pg_config`) | `uv sync` | `psycopg2` is built from source, not installed as `psycopg2-binary` |
| Outbound HTTPS, and TCP `5432` to `chado.flybase.org` | Full builds | See [External services](#external-services) |

Hardware sizing for full builds is not documented yet. The largest single item is the dbSNP cache for full human builds (see [dbSNP cache](#dbsnp-cache-human-only)).

---

## Pipeline host

```bash
git clone https://github.com/rejuve-bio/biocypher-kg.git
cd biocypher-kg
make setup            # installs uv if needed, then runs uv sync
cp .env.example .env  # optional, see BIOPORTAL_API_KEY below
```

Then build with `make run` (interactive), `make run-sample` or `make run-direct`. The [README](https://github.com/rejuve-bio/biocypher-kg/blob/main/README.md) is the full reference for running builds and downloading data.

- **`.env`** holds one setting, `BIOPORTAL_API_KEY`, and it is optional. The Disease Ontology adapter downloads from OBOLibrary and only falls back to BioPortal when that download fails and a key is set.

### Choose a writer for your backend

| `--writer-type` | Output | Consumed by |
|---|---|---|
| `neo4j` | CSV files plus `nodes_*.cypher` / `edges_*.cypher` load scripts | [Neo4j](#neo4j) |
| `metta` | `*.metta` files plus `type_defs.metta` | [MORK](#mork), [Hyperon or DAS](#optional-metta-consumers) |
| `prolog`, `parquet`, `KGX`, `networkx` | Files | No service; use the files directly |

### dbSNP cache (human only)

Full `hsa` builds need a prebuilt dbSNP cache. Sample builds use the bundled `aux_files/hsa/sample_dbsnp`, and other species don't use dbSNP at all. Build the cache with `scripts/update_dbsnp.py`, then pass `--dbsnp-cache-root` or set `dbsnp_cache_root` in `config/species_config.yaml`. See [README § dbSNP Cache](https://github.com/rejuve-bio/biocypher-kg/blob/main/README.md#-dbsnp-cache) and the [processors README](https://github.com/rejuve-bio/biocypher-kg/blob/main/biocypher_metta/processors/README.md) for build options.

---

## Neo4j

The supported setup is the parameterised compose file [`docker/docker-compose.neo4j.yml`](https://github.com/rejuve-bio/biocypher-kg/blob/main/docker/docker-compose.neo4j.yml), driven by `make neo4j-*` targets. It runs `neo4j:5.21` (Community edition) with APOC, mounts your build output read-only at `/import`, and keeps data in named volumes.

Neo4j's import root is `/`, and `NEO4J_IMPORT_ROOT` is also mounted read-only at the same path inside the container. So a loader can import any build under that root by its absolute path (the [Web Console](#web-console) does). The `.cypher` files use relative `file:///` URLs, so `make neo4j-load` and `make neo4j-load-direct` add the `/import` prefix for you (Make variable `NEO4J_CONTAINER_IMPORT`).

> Don't use the `build`, `import` and `deploy` services in the root `docker-compose.yml`. They are left over from the BioCypher project template and no longer work (see [Known issues](#known-issues)).

### 1. Configure `docker/neo4j.env`

```bash
cp docker/neo4j.env.example docker/neo4j.env   # gitignored
```

| Variable | Example value | Read by | Notes |
|---|---|---|---|
| `NEO4J_IMAGE` | `neo4j:5.21` | compose | |
| `NEO4J_HTTP_PORT` | `7674` | compose | Browser at `http://localhost:7674/browser/` |
| `NEO4J_BOLT_PORT` | `7887` | compose | |
| `NEO4J_AUTH` | `neo4j/your_password_here` | compose | Applied only when the data volume is first created. After changing it, run `make neo4j-reset` |
| `NEO4J_OUTPUT_DIR` | *(required)* | compose, both loaders | Host directory mounted read-only at `/import`. For one species, use that build's `--output-dir`. For several species in one instance, use the parent directory (see [step 4](#4-several-species)) |
| `NEO4J_IMPORT_ROOT` | *(defaults to `NEO4J_OUTPUT_DIR`)* | compose | Absolute host directory mounted read-only at the same path, so builds anywhere under it load by absolute path |
| `NEO4J_ARCHIVE_DIR` | — | `make neo4j-load` | Where versioned loads archive each changed dataset |
| `NEO4J_PAGECACHE` / `NEO4J_HEAP_INIT` / `NEO4J_HEAP_MAX` | `4G` / `2G` / `4G` | compose | See [Memory](#memory) |
| `NEO4J_URI` | `bolt://localhost:7887` | both loaders | Must use the `NEO4J_BOLT_PORT` port |
| `NEO4J_USERNAME` | `neo4j` | both loaders | |
| `NEO4J_PASSWORD` | — | both loaders | Must match the password in `NEO4J_AUTH` |
| `NEO4J_IMPORT_BATCH_SIZE` | `50000` | `make neo4j-load` | APOC batch size |
| `CSV_BASE_PATH` | `/import` | `make neo4j-load-direct` | Prefix for the `.cypher` files' `file:///` URLs. The default suits this compose file; change it only for a Neo4j whose import directory is elsewhere |
| `NEO4J_CONTAINER` | `neo4j_bio_atomspace` | nothing | Ignored. The container is always named `biocypher_neo4j-neo4j-1` |

### 2. Start and manage the container

| Command | Effect |
|---|---|
| `make neo4j-up` | Start the container, or recreate it if its configuration changed. Data is kept |
| `make neo4j-status` | Show container status |
| `make neo4j-logs` | Follow container logs |
| `make neo4j-down` | Stop and remove the container. The data volumes are kept |
| `make neo4j-reset` | **Delete the data volumes**, then start fresh so `NEO4J_AUTH` and other settings are applied again |

To use a different env file, pass `NEO4J_ENV_FILE=docker/my.env` to any target. All targets use the compose project `biocypher_neo4j` (override with `NEO4J_PROJECT_NAME`), which is why the container is named `biocypher_neo4j-neo4j-1`.

### 3. Load data

Build with `--writer-type neo4j` first, then load with one of two targets:

| | `make neo4j-load-direct` | `make neo4j-load` |
|---|---|---|
| Script | [`scripts/neo4j_loader.py`](https://github.com/rejuve-bio/biocypher-kg/blob/main/scripts/neo4j_loader.py) | [`kg-service/neo4j_loader.py`](https://github.com/rejuve-bio/biocypher-kg/blob/main/kg-service/neo4j_loader.py) |
| Loads | Every directory under `NEO4J_OUTPUT_DIR` that contains `.cypher` files: nodes first, then edges | Only the top-level dataset folders whose CSV files changed since the last load (MD5 hash) |
| Before loading | Nothing. Existing data is not removed | Archives each changed folder to `NEO4J_ARCHIVE_DIR/neo4j/<dataset>/<vN>/`, then deletes that dataset's old nodes and edges |
| Version metadata | None | Writes `KGVersion`, `DatasetVersion`, `DatasetHash` and `DatasetMapping` nodes |
| Requires | `NEO4J_OUTPUT_DIR`, `NEO4J_URI`, `NEO4J_PASSWORD` (prompts if unset) | The same, plus `NEO4J_ARCHIVE_DIR` |

Use `neo4j-load-direct` for a clean load into an empty database (fresh volumes or after `make neo4j-reset`). Use `neo4j-load` to reload an instance after rebuilding some datasets.

Both loaders read `docker/neo4j.env` via `--env-file`; flags given on the command line override it.

- `scripts/neo4j_loader.py`: `--env-file`, `--output-dir`, `--uri`, `--username`, `--password`, `--csv-base-path`. Without an env file, `--uri` defaults to `bolt://localhost:7687`, Neo4j's standard port, not the compose port.
- `kg-service/neo4j_loader.py`: `--env-file`, `--output-dir`, `--archive-dir`, `--uri`, `--username`, `--password`, `--database`, `--build-id` (default `build-<UTC timestamp>`), `--import-batch-size`, `--import-dir`. Settings missing from the flags and the env file are read from the process environment (`NEO4J_PASSWORD`, `NEO4J_DATABASE`, …), which keeps a password off the command line. `--import-dir` prefixes the `file:///` URLs; `make neo4j-load` passes `/import`. A dataset is finalized only when every one of its files loaded, so a retry reloads a partly failed dataset instead of skipping it.

### 4. Several species

A Neo4j instance holds whatever you load into it. You can run it in one of two ways:

- **All species in one instance.** Build with `--species all` and set `NEO4J_OUTPUT_DIR` to that run's parent output directory. Each species' `.cypher` files reference `file:///<species>/...` relative to it. Output built before this layout was introduced can be rewritten once with [`scripts/fix_cypher_import_paths.py`](https://github.com/rejuve-bio/biocypher-kg/blob/main/scripts/fix_cypher_import_paths.py).
- **One species at a time (sample builds).** `scripts/switch_species.sh <hsa|dmel|mmu|rno|cel>` points `docker/neo4j.env` at `output_<species>_sample/`, runs `make neo4j-reset` and loads with `scripts/neo4j_loader.py`. This wipes the current graph.

### 5. Without make

```bash
docker compose -p biocypher_neo4j --env-file docker/neo4j.env \
  -f docker/docker-compose.neo4j.yml up -d
```

Keep `-p biocypher_neo4j`. Without it, compose uses a different project name and creates new, empty volumes. (The usage comment at the top of the compose file leaves it out.)

To run the container without compose, mount the output at Neo4j's default import directory and enable APOC file access:

```bash
docker run -d --name neo4j_bio \
  -p 127.0.0.1:7674:7474 -p 127.0.0.1:7887:7687 \
  -v /path/to/output:/var/lib/neo4j/import:ro \
  -e NEO4J_AUTH=neo4j/<password> \
  -e NEO4J_PLUGINS='["apoc"]' \
  -e NEO4J_apoc_import_file_enabled=true \
  -e NEO4J_apoc_import_file_use__neo4j__config=false \
  -e NEO4J_dbms_security_procedures_unrestricted='apoc.*' \
  neo4j:5.21
```

Without the `/var/lib/neo4j/import` mount, `LOAD CSV` fails with `NoSuchFileException`.

### Memory

The defaults (`4G` page cache, `2G`/`4G` heap) suit sample builds. For full builds, raise them in `docker/neo4j.env` and run `make neo4j-up`; compose recreates the container and keeps the data. To get a starting point sized to the host, run:

```bash
docker exec biocypher_neo4j-neo4j-1 neo4j-admin server memory-recommendation
```

### Verify the database

```bash
make neo4j-status
docker exec biocypher_neo4j-neo4j-1 cypher-shell -u neo4j -p '<password>' \
  'MATCH (n) RETURN count(n) AS nodes;'
```

In the browser, open `http://localhost:7674/browser/` and connect to `bolt://localhost:7887`. The compose file defines no health check, so `make neo4j-status` shows "running" rather than "healthy". To tell when Neo4j is ready, wait for `Started.` in `make neo4j-logs`.

---

## MORK

MORK serves the `metta` writer's output over HTTP on port `8027`. The image is built from [`biocypher-mork/Dockerfile`](https://github.com/rejuve-bio/biocypher-kg/blob/main/biocypher-mork/Dockerfile). It compiles [PathMap](https://github.com/Adam-Vandervorst/PathMap) and the `server` branch of [Abdu1964/MORK](https://github.com/Abdu1964/MORK) on `rustlang/rust:nightly`. Neither repository is pinned to a commit, so images built on different days can differ, and the first build is slow because both are compiled in release mode.

### 1. Run the server

Two compose files can start the image, and both name the container `mork-biocypher`, so run only one of them. Use the one in `biocypher-mork/`: its defaults match the loader's and the persistence layer's.

**Standalone (recommended).** Run from `biocypher-mork/`:

```bash
cd biocypher-mork
cat > .env <<'EOF'
# Directory holding the .metta build output (absolute, or relative to biocypher-mork/)
MORK_DATA_DIR=/path/to/output
# Host port; the container always listens on 8027
HOST_PORT=8027
# Seconds between snapshots
SNAPSHOT_INTERVAL_SECONDS=300
EOF
sudo chown -R 1000:1000 mork_persist   # the container runs as UID 1000 and must write here
docker compose up -d --build
```

- **`.env` is required.** The compose file declares `env_file: .env` and refuses to start without it. The repo has no template for it, and it is gitignored.
- **`mork_persist/` must be writable by UID 1000.** If it isn't, the container exits at start with `ERROR: /app/persist is not writable`. Skip the `chown` if your user ID (`id -u`) is already 1000.

**From the repo root.** The root `docker-compose.yml` defines the same server as service `mork`:

```bash
mkdir -p mork_persist && sudo chown 1000:1000 mork_persist
MORK_DATA_DIR=./output MORK_HOST_PORT=8027 docker compose up -d mork
```

This variant differs from the standalone one in four ways:
- The host-port variable is `MORK_HOST_PORT`, not `HOST_PORT`.
- It publishes on `127.0.0.1` only.
- Data defaults to `./output_human`.
- Persistence lives in the repo-root `mork_persist/`, which doesn't exist until you create it. When loading data, export `SNAPSHOT_DIR=$PWD/mork_persist`.

| Variable | Read by | Default | Purpose |
|---|---|---|---|
| `MORK_DATA_DIR` | both compose files | `./data` (in `biocypher-mork/`), `./output_human` (root) | Host directory mounted at `/app/data` |
| `MORK_IMPORT_ROOT` | `biocypher-mork/docker-compose.yml` | *(unset: `MORK_DATA_DIR` at `/app/import-root`)* | Absolute host directory mounted read-only at the same path, so a loader can import any build under it by absolute path (`kg-service/mork_loader.py` does). Must be absolute |
| `HOST_PORT` | `biocypher-mork/docker-compose.yml`, `data_loader.py` | `8027` | Host port |
| `MORK_HOST_PORT` | root `docker-compose.yml` | `8027` | Host port |
| `SNAPSHOT_INTERVAL_SECONDS` | both compose files | `300` | Seconds between snapshots |
| `SNAPSHOT_DIR` | `data_loader.py`, `wal_client.py` (host side) | `biocypher-mork/mork_persist` | Where the host-side client appends to the WAL. Inside the container it is always `/app/persist` |
| `DATASET_PATH` | `data_loader.py` | `./data` | Directory to load |

### 2. Persistence

| File | Written by | Contents |
|---|---|---|
| `mork_persist/snapshot.paths` | The container | A full binary snapshot. Written every `SNAPSHOT_INTERVAL_SECONDS` while the server is idle, and again on shutdown. The WAL is emptied after each successful snapshot |
| `mork_persist/wal.metta` | The Python client on the host ([`wal_client.py`](https://github.com/rejuve-bio/biocypher-kg/blob/main/biocypher-mork/wal_client.py)) | Writes made since the last snapshot |

On start, the container imports the snapshot, then replays the WAL.

- **File loads are not written to the WAL.** `data_loader.py` asks the server to import files by their container path (`/app/data/...`), and its WAL client can't read that path from the host. You'll see `[WalMORK] WARNING: could not tee ...`, and the loaded data survives a restart only after the next snapshot. After a load, wait one snapshot interval, or stop the container cleanly so it snapshots on the way down.
- **Allow enough time for the final snapshot.** Compose's default stop timeout is 10 seconds, but the final snapshot waits up to 240 seconds for a large export. Stop with `docker compose stop -t 300` (or `down -t 300`) so the snapshot isn't killed part-way.

To start over with an empty graph:

```bash
docker compose down -t 300
sudo truncate -s 0 mork_persist/snapshot.paths mork_persist/wal.metta
docker compose up -d
```

### 3. Load MeTTa data

Build with `--writer-type metta`, then run [`biocypher-mork/data_loader.py`](https://github.com/rejuve-bio/biocypher-kg/blob/main/biocypher-mork/data_loader.py) from the repo root:

```bash
DATASET_PATH=/path/to/output HOST_PORT=8027 uv run python biocypher-mork/data_loader.py
```

- **Set `DATASET_PATH` to the same directory as `MORK_DATA_DIR`.** The loader sends the server each file's path under `/app/data`, and the server reads the file from its own mount.
- It loads every `.metta` and `.paths` file under that directory, recursively, into the `annotation` namespace, then prints a sample of the loaded facts.
- It doesn't clear existing data first. To reload from scratch, empty the graph as shown above.
- With the root-compose variant, also set `SNAPSHOT_DIR=$PWD/mork_persist`.

[`kg-service/mork_loader.py`](https://github.com/rejuve-bio/biocypher-kg/blob/main/kg-service/mork_loader.py) is the loader the [Web Console](#web-console) uses. It also works on its own, with these differences from `data_loader.py`:

- **It replaces the namespace with one build.** If any dataset changed since its last load, it clears the `annotation` namespace and imports every `.metta` file of the build; if nothing changed, it does nothing. Data from other builds is removed.
- **It imports by absolute path.** The server must see the build at the same path, so set `MORK_IMPORT_ROOT` to a directory that contains it.
- **It checks before clearing.** A wrong or empty `--data-dir` fails before anything is cleared. If a load fails after the clear, it removes its version metadata so the next load is a full reload.
- **Pass `--port` and `--archive-dir`.** Its defaults (`8432` and a team-server path) don't match this setup: `--data-dir /path/to/output --port 8027 --archive-dir /path/to/archives`. Version metadata goes to `<archive-dir>/mork/version_metadata.json`.

`scripts/mork_loader.py` and `scripts/load_paths.py` still fail (see [Known issues](#known-issues)).

### 4. Verify the server

```bash
docker compose ps                      # "healthy" once the 60 s start period has passed
docker compose logs -f mork-biocypher  # service name in biocypher-mork/; it is "mork" in the root compose
curl http://localhost:8027/status/-
```

### 5. MORK CLI tools (ACT, `.paths`, REPL): currently unavailable

The repo has three scripts that work with MORK's command-line kernel instead of the server:

- [`scripts/build_act.sh`](https://github.com/rejuve-bio/biocypher-kg/blob/main/scripts/build_act.sh) `<dir>` compiles every `.metta` file in dependency order (types, then nodes, then edges) into a single memory-mapped `annotation.act`.
- [`scripts/build_paths.sh`](https://github.com/rejuve-bio/biocypher-kg/blob/main/scripts/build_paths.sh) converts each `.metta` file to binary `.paths`, skipping files it has already converted.
- [`scripts/mork_repl.py`](https://github.com/rejuve-bio/biocypher-kg/blob/main/scripts/mork_repl.py) `--format act|paths|metta` is an interactive query loop. In `paths` and `metta` modes it reloads the data for every query, so it can be used to compare load time against query time.

All three run `docker compose run --rm mork` from the repo root and call `/app/MORK/target/release/mork`. The current image doesn't build that binary (the build step is commented out in the Dockerfile), so all three fail until it's restored.

---

## kg-service

kg-service ("BioCypher KG Observatory") is a FastAPI app that serves graph metadata and version history. It reads from Neo4j and from the archive directory that `make neo4j-load` writes. This section runs it on its own, on a host that can reach Neo4j. The [Web Console](#web-console) runs the same app in a Docker image, along with its own routes and frontend. Some endpoints have problems; see [Known issues](#kg-service-issues).

### 1. Install

kg-service's dependencies are in the project's `pyproject.toml`, so `make setup` (`uv sync`) installs them with the rest of the project. `kg-service/requirements.txt` also lists `streamlit` and `plotly`, which the API doesn't use.

### 2. Configure `kg-service/.env`

Settings are read from `.env` in the working directory, so always start the service from `kg-service/`. Every default in [`config.py`](https://github.com/rejuve-bio/biocypher-kg/blob/main/kg-service/backend/core/config.py) is either a placeholder or a path on one team server, so set all of these:

```env
NEO4J_URI=bolt://localhost:7887
NEO4J_USER=neo4j
NEO4J_PASSWORD=<same as NEO4J_PASSWORD in docker/neo4j.env>
NEO4J_DATABASE=neo4j
ARCHIVE_BASE=/path/to/biocypher-archives
VERSION_DIFF_SCRIPT=/path/to/biocypher-kg/kg-service/version_diff.py
MORK_URL=http://localhost:8027
```

Don't copy `docker/neo4j.env` into this file. Unknown keys fail validation (`extra_forbidden`), and the username key here is `NEO4J_USER`, not `NEO4J_USERNAME`.

| Setting | Code default | Set to |
|---|---|---|
| `NEO4J_URI` | *(none: the observatory's Neo4j is off until set)* | Your Bolt URI, e.g. `bolt://localhost:7887` |
| `NEO4J_USER` | *(none)* | `neo4j` |
| `NEO4J_PASSWORD` | *(none)* | The Neo4j password |
| `NEO4J_DATABASE` | *(none)* | `neo4j` (Community edition serves only this database) |
| `ARCHIVE_BASE` | *(none)* | The same directory as `NEO4J_ARCHIVE_DIR` |
| `VERSION_DIFF_SCRIPT` | `/home/abdum/services/biocypher-kg/version_diff.py` | Absolute path to `kg-service/version_diff.py` |
| `MORK_SUMMARY_SCRIPT` | `/home/abdum/services/biocypher-kg/get_mork_summary.py` | No working value; that script isn't in the repo |
| `MORK_URL` | *(none)* | `http://localhost:8027` |
| `MORK_LIVE_STATS_ENABLED`, `CACHE_TTL`, `API_HOST`, `API_PORT`, `DEBUG` | — | Unused; no code reads them. The port comes from the `uvicorn` command |
| `APP_NAME`, `APP_VERSION` | `BioCypher KG Observatory`, `0.1.0` | Returned by `GET /` |

**Archive directory.** The version endpoints read `ARCHIVE_BASE/neo4j/<dataset>/<vN>/` (written by `make neo4j-load`) and `ARCHIVE_BASE/mork/`. Point `ARCHIVE_BASE` at the same directory as `NEO4J_ARCHIVE_DIR`.

### 3. Run

```bash
cd kg-service
uv run uvicorn backend.api.main:app --host 127.0.0.1 --port 8000
```

The service has no sign-in, so it is meant for this machine only:

- It answers only requests whose `Host` is `localhost`, `127.0.0.1` or `[::1]`.
- It refuses cross-site browser requests and sends no CORS headers.
- It can't be shown in a frame on another site.

Behind a reverse proxy or under another name, list the extra names (comma-separated) in `CONSOLE_ALLOWED_HOSTS` and `CONSOLE_ALLOWED_ORIGINS`, and put a sign-in in front of it.

On startup the service schedules a `graph_info` refresh every 72 hours. If the cache file is missing, it also runs one refresh 5 seconds after start. The cache file's path is hardcoded (see [Known issues](#known-issues)).

### 4. Verify the API

```bash
curl http://localhost:8000/        # {"name": "BioCypher KG Observatory", "version": "0.1.0", "docs": "/docs"}
curl http://localhost:8000/health  # {"status": "healthy", "neo4j": "connected"}
```

The interactive API docs (`/docs`, `/redoc`, `/openapi.json`) are off unless you start the service with `CONSOLE_API_DOCS=1`; `GET /` lists `/docs` either way. The endpoint reference is in [endpoints.md](../api/endpoints.md).

---

## Web Console

A browser UI for the whole flow, one species at a time:

1. Pick a species and its data sources, and download what's missing.
2. Build.
3. Load the build into that species' own Neo4j and MORK.
4. Explore the live schema, and edit the configs.

It is kg-service plus a React frontend in one Docker image, served at `http://localhost:<port>/console/`. Researchers set everything in its **Settings** page; they don't need the repo, an env file or Docker commands.

### 1. Run it

| | From a checkout | Without a checkout |
|---|---|---|
| Compose file | `docker/docker-compose.yml` (builds the image) | `docker/docker-compose.dist.yml` (pulls the published image) |
| Start | `./docker/up.sh` | `CONSOLE_IMAGE=ghcr.io/<owner>/biocypher-kg-console:latest docker compose -f docker-compose.dist.yml up -d` |

There's no env file and no defaults. Everything a researcher sets is in **Settings**: each species' databases, the data and Builds folders, Neo4j memory, and how builds run. The console writes only to folders and connects only to databases chosen there:

- builds, downloads and dbSNP prepares wait until the folders they use are set (New Build lists what's missing, and validation refuses the build);
- loads go only to the build's species' own database, with the login saved for it;
- a console-run Neo4j waits for Neo4j memory.

- **Images.** `.github/workflows/publish-console-image.yml` publishes the image to GHCR on every push to `console`. `up.sh --no-build` uses a pulled image.
- **`up.sh`.** It builds the MORK image once when `biocypher-mork/` exists (*Run here* MORK uses it) and starts the console. It creates no folders.
- **Where Settings are kept.** Settings and build history live in the `console_builds` volume (`console_settings.json`, owner-only).
- **On a server.** `./docker/up.sh --host <address>` (the server's IP or name) listens on the network instead of `127.0.0.1`, accepts that address, and asks for a password before anything but its own page files. `up.sh` creates the password once (24 random characters) and keeps it owner-only in `docker/.console-password`; it isn't printed (so it doesn't end up in pasted output or logs), `cat docker/.console-password` shows it, and deleting the file before the next `up.sh` makes a new one; the address is kept in `docker/.console-host`, so a plain `./docker/up.sh` keeps it. `--host localhost` goes back to this computer only. Sign-in sets an HttpOnly, SameSite=Strict cookie for 30 days, derived from the password, so a new password signs everyone out. Neo4j Browser links are hidden there, since console-run Neo4j stays on the server's `127.0.0.1`. Some servers block Docker's published ports themselves (rules in the `DOCKER-USER` chain); there the port has to be opened the way that server does it, or the console put behind its reverse proxy.
- **Port.** `up.sh` picks a random free port (20000 to 39999) the first time, so it never collides with another service, and keeps it in `docker/.console-port` so the address stays the same. It picks a new one only if that port gets taken, and prints the address each time. `CONSOLE_PORT=<port> ./docker/up.sh` chooses it yourself; the dist compose uses 9000 unless you set it.
- **Set before it starts.** Only what Docker needs before the console runs: the port (above) and the image (`CONSOLE_IMAGE`), as shell variables when you start it.

### 2. Settings

- **Species databases.** Each species has its own Neo4j and MORK, chosen per database:
  - **Run here:** the console creates and starts `neo4j-<species>` / `mork-<species>` containers through the Docker socket.
    - Neo4j uses `NEO4J_IMAGE` (default `neo4j:5.21`) and gets a generated password, shown under *In use*.
    - MORK needs a local image: `MORK_IMAGE`, or what `up.sh` built (`biocypher-console-mork[-<species>]`).
    - Remove deletes the container but keeps its data volume and password, so Set up brings it back.
  - **Use existing:** your own database's address and, for Neo4j, its username, password and database, all required (nothing is filled in for you) and checked before they're saved.
    - A database on this computer is entered as `localhost`; the console connects through `host.docker.internal`.
    - An unencrypted address on another computer (`bolt://`, `http://`) is flagged; use `neo4j+s://`, `bolt+s://` or `https://` where the server offers it.
- **Data locations.** *Browse…* opens the computer's own Finder. Browsers don't give the path, so the console finds the chosen folder by name and contents, then checks it:
  - a species folder must hold some of the sources that species' adapters read, and the files must really be what their names say;
  - a dbSNP cache must be a real SQLite cache from `scripts/update_dbsnp.py` (pickle caches are refused);
  - the console data folder must be empty or already laid out as one.

  Saving Settings and validating a build run the same checks.
- **Build settings.** The first three are required and have no defaults.
  - **Builds folder:** where builds are saved, each in `<folder>/<writer>/<species>-<dataset>-<time>`. It can be any shared folder, including an external drive for big builds. It must be empty or already hold builds, since the console removes older builds it made there (only folders it made: dated, in a writer folder, holding build files).
  - **Console data folder:** downloaded source data (`input/<species>`), the prepared dbSNP cache and load archives. A folder an adapters config names (a server's `input_dir`, say) is only used once chosen under Data locations.
  - **Neo4j memory:** page cache and heap for each Neo4j the console runs.
  - Builds at the same time, finished builds kept, and builds listed in History.

  The Settings page shows a folder as chosen only when you chose it; otherwise the field is blank and says what it falls back to. New Build shows where the next build will be saved, and each build's page shows where it was.

### Build size

A build can be smaller than the full graph. Step 3 of New Build has one **Size** control with two units:

- **Rows per adapter** passes `--limit`.
- **Percent** of each source's full size becomes per-adapter caps, written to `adapter_limits.json` in the job folder and passed as `--adapter-limits`. The caps are kept in the build's params, so Resume uses the same ones.

The full sizes come from the console's own builds:

- Each build records what every adapter wrote in `graph_info.json` (`adapter_counts`), and whether a cap stopped it early.
- When a build succeeds, its complete counts are saved in `sizes.json` in the console's builds folder, per species and dataset. Capped counts are never saved. Deleting old builds doesn't lose them.
- New Build shows each source's size and a preview of what the build will write. Percent stays off until every selected source has a known size; building those sources once without a limit teaches the console their size.
- The build page shows the build's size and the rows each adapter wrote, with ✂️ on adapters the limit cut short. History notes each build that wasn't full.

Each adapter keeps its first rows, so a capped graph is a sample of each source, not a random subset of it.

### Config

Each console instance has its own copy of the config: what the Config page edits and its builds read. The image carries the shipped config (`/app/.config-shipped`, read-only); the instance's copy lives in the `console_config` volume at `/app/config`. So:

- edits stay with that instance, across restarts and image updates, and never change a repo or the image;
- at start-up, every file the instance hasn't edited is brought up to the shipped version (new files added, ones no longer shipped removed); edited files keep the instance's version (`config/.console-edits.json` records which);
- the Config page says whether the open file is the shipped version or edited on this console, flags it when the shipped version changed since the edit, and offers *reset to shipped*;
- each build still records the exact config it used (`build_config/`).

Run from a checkout without Docker, there is no shipped copy: `config/` is the repo's own and edits go there.

### 3. Folders the console can see

The console runs in Docker, so it only sees shared folders, at the same path inside the container. `up.sh` picks them for the computer it runs on and writes them to `docker/.console-shares.yml`, sharing only folders that exist (so Docker never creates empty ones):

| Computer | Shared |
|---|---|
| Mac | `~/Desktop`, `~/Documents`, `~/Downloads`, `/Volumes` |
| Linux (a server) | the home folder, and `/mnt`, `/media`, `/data`, `/srv` where present |
| Either | any folders in `CONSOLE_SHARES` (space-separated), e.g. `CONSOLE_SHARES="/scratch" ./docker/up.sh` |

These are what Browse… shows on a server, and where the data and Builds folders can be. They're shared writable for those two; the console writes only into them. Folders chosen as source data or a dbSNP cache are only read, and can't be chosen as a place to save.

On a Mac, don't share the whole home folder. Docker Desktop for Mac passes every file change in a shared folder to its VM, and a home folder changes so often that the engine stops (`service fs failed: injecting event blocked for 60s`). Linux has no such problem. `docker-compose.dist.yml` (no checkout, no `up.sh`) shares the Mac set.

### 4. Loading

| | Neo4j | MORK |
|---|---|---|
| Loader | `kg-service/neo4j_loader.py`, with the species' own login. The password goes in through the environment, never the command line or the job log | `kg-service/mork_loader.py`. A MORK load replaces the namespace with that build (the UI warns before loading) |
| Archives and version metadata | `<data folder>/archives/<species>/neo4j/` | `<data folder>/archives/<species>/mork/` |
| Reads the build output from | the same absolute path. Console-run instances mount the data and Builds folders there; your own needs `NEO4J_IMPORT_ROOT` (or equivalent) | the same absolute path. Console-run instances mount the data and Builds folders there; your own needs `MORK_IMPORT_ROOT` |

**All-species builds.** A build with species *all species* writes one folder per species (`<build>/<species>/`). Loading it starts one load per species, each folder into that species' own Neo4j or MORK; species without one are skipped and listed, so nothing goes into another species' database. Each species' load has its own status, log and Retry, and the build page and History show them per species.

When the data folder, the Builds folder or Neo4j memory changes, the console re-creates the instances it runs to match, keeping their data volumes and logins. It does the same at start-up for any made with older settings. A load into an instance that can't see the build yet waits for that and says so.

Console-run instances publish ports on `127.0.0.1` only:

- **Neo4j:** Bolt `7687+i` and HTTP `7474+i`, where `i` is the species' position in `config/species_config.yaml` (hsa 0, dmel 1, …). A taken port block shifts by 10.
- **MORK:** no host port. It takes no password, so only the console reaches it, over their shared Docker network.

### 5. Security

On your own computer the console listens on `127.0.0.1` only and has no sign-in. On a server (`up.sh --host`) it listens on the network and asks for its password first; allow its port only from your own IP in the server's firewall (Hetzner's Cloud Firewall, say: `ufw` doesn't cover ports Docker publishes), since plain http doesn't encrypt the password.

- **It stops other websites using the browser against it:**
  - The `Host` must be this machine, which blocks DNS rebinding.
  - Cross-site requests are refused (`Sec-Fetch-Site`, `Origin`).
  - It can't be framed, and it sends a strict Content-Security-Policy and `Cache-Control: no-store`.
  - Settings can't change the program that runs builds. The data and Builds folders can't be a system, home or drive's top folder, or hold source data the console only reads.
- **Containers.** The console, and the Neo4j and MORK containers it creates, run with `no-new-privileges`.
- **Image.** Env files are excluded from the image (`.dockerignore`).
- **The Docker socket.** The console controls Docker on its host so it can run databases, which is why it asks for a password whenever it's on the network. Behind a reverse proxy under another name, add that name to `CONSOLE_ALLOWED_HOSTS` (and `CONSOLE_ALLOWED_ORIGINS`) when you start it. These stay outside Settings on purpose: a page can't widen who may reach the console.
- **Shared folders.** Desktop, Documents, Downloads and drives are writable to the console so you can save builds there; that's the trade for choosing folders in Settings instead of an env file.

---

## Optional MeTTa consumers

Neither of these is a project dependency; install what you need yourself.

**Hyperon (in-process).** [`scripts/metta_space_import.py`](https://github.com/rejuve-bio/biocypher-kg/blob/main/scripts/metta_space_import.py) loads a build's `.metta` files into an in-memory Hyperon space, runs two hard-coded human example queries, and logs load time and memory use. It's a benchmark rather than a service: the space exists only while the script runs.

```bash
uv run --with hyperon python scripts/metta_space_import.py \
  --input-dir ./output --type-def-path ./output/type_defs.metta --log hyperon_import.log
```

**DAS (Distributed AtomSpace).** [`scripts/das_metta_loader.py`](https://github.com/rejuve-bio/biocypher-kg/blob/main/scripts/das_metta_loader.py) `<dir>` runs `das-cli metta load` on every `.metta` file under `<dir>`. Before running it, install `das-cli`, then start and check a DAS server with `das-cli server start` and `das-cli server ping`. See the [das-cli README](https://github.com/singnet/das-toolbox/blob/master/das-cli/README.md).

---

## External services

| Service | Used by | Access | Notes |
|---|---|---|---|
| Data-source hosts (EBI, NCBI, UniProt, …) | `make download` | HTTPS | URLs are in `config/<species>/<species>_data_source_config.yaml` |
| FlyBase Chado PostgreSQL, `chado.flybase.org:5432` | dmel adapters (allele, genotype–phenotype, RNA-Seq library, expression value) and `reactome_edges_adapter` | Outbound TCP `5432`; public read-only account (credentials are in the adapters) | A firewall that allows only HTTPS breaks these adapters |
| BioPortal API | Disease Ontology adapter, as a fallback | HTTPS; `BIOPORTAL_API_KEY` | Optional |
| NCBI dbSNP | `scripts/update_dbsnp.py` | HTTPS | Full human builds only |
| Ontology hosts (OBOLibrary and others) | Ontology adapters | HTTPS | |

---

## Port reference

| Host port | Service | Bound to | Set by |
|---|---|---|---|
| `7674` | Neo4j HTTP and Browser | `127.0.0.1` | `NEO4J_HTTP_PORT` |
| `7887` | Neo4j Bolt | `127.0.0.1` | `NEO4J_BOLT_PORT` |
| `8027` | MORK HTTP | all interfaces | `HOST_PORT` (`biocypher-mork/`) or `MORK_HOST_PORT` (root compose) |
| `8000` | kg-service | the `uvicorn --host` value | `uvicorn --port` |
| random, 20000 to 39999 (`9000` with the dist compose) | Web Console | `127.0.0.1`, or every interface with `up.sh --host` (password required) | `docker/.console-port`, or `CONSOLE_PORT` |
| `7687+i`, `7474+i` | Console-run Neo4j for the species at position `i` | `127.0.0.1` | the console ([Loading](#4-loading)) |
| `5432` (outbound) | FlyBase Chado | — | — |

Other port numbers in the code are leftover defaults, not services you need to run:
- `7687`: the `--uri` default in `scripts/neo4j_loader.py`
- `27688`: `version_manager.py`
- `8432`: `kg-service/mork_loader.py` and `test_mork_stats.py`
- `7474`/`7687`: the legacy root-compose `deploy` service

---

## Known issues

These are code issues. This guide documents current behaviour; fixes belong in separate PRs.

### kg-service issues

| Issue | Effect | Workaround | Where |
|---|---|---|---|
| `MORK_SUMMARY_SCRIPT` defaults to `get_mork_summary.py`, which isn't in the repo | `/api/databases/mork/summary` fails | None | `backend/core/config.py:12` |
| The `graph_info` cache path is hardcoded to `/mnt/hdd_1/abdu_md/...` | Cache refresh fails on other hosts | Edit the path | `backend/core/graph_info_cache.py:11` |
| The `databases` router is never registered | Its endpoints return 404 | None | `backend/api/main.py` |
| `version_diff.py` parses CSVs as comma-delimited, but the writer uses `\|` | Version comparisons are unreliable | None | `kg-service/version_diff.py`, `biocypher_metta/neo4j_csv_writer.py:17` |

### MORK issues

| Issue | Effect | Workaround | Where |
|---|---|---|---|
| `scripts/mork_client.py` reuses quotes inside an f-string, which needs Python 3.12 | `scripts/mork_loader.py` and `scripts/load_paths.py` fail with `SyntaxError` on 3.10 and 3.11 | Use `biocypher-mork/data_loader.py` | `scripts/mork_client.py:524` |
| `kg-service/mork_loader.py` defaults to port `8432` and a team-server `--archive-dir` | A plain run targets the wrong port and path | Pass `--port` and `--archive-dir` (the Web Console does) | `kg-service/mork_loader.py`, `main()` |
| The image doesn't build the MORK kernel CLI | `build_act.sh`, `build_paths.sh` and `mork_repl.py` fail | None | `biocypher-mork/Dockerfile:25-27` |
| `data_loader.py` doesn't pass `host_data_dir` to the WAL client | File loads aren't written to the WAL and survive a restart only after a snapshot | Wait one snapshot interval, or `docker compose stop -t 300` | `biocypher-mork/data_loader.py:24` |
| The container runs as UID 1000 and exits if `/app/persist` isn't writable; the root compose's `./mork_persist` doesn't exist | Container exits at start | `chown 1000:1000` the persist directory | `biocypher-mork/entrypoint.sh:20-23` |
| `biocypher-mork/docker-compose.yml` requires `.env`, but there is no template | Compose refuses to start | Create `.env` ([Run the server](#1-run-the-server)) | `biocypher-mork/docker-compose.yml:7-8` |
| `biocypher-mork/mork_persist/snapshot.paths` and `wal.metta` are tracked in git | A running server leaves the working tree dirty | Don't commit them | `biocypher-mork/mork_persist/` |
| The Dockerfile clones MORK and PathMap without pinning a commit | Builds aren't reproducible | None | `biocypher-mork/Dockerfile:17,22` |

### Web Console issues

| Issue | Effect | Workaround | Where |
|---|---|---|---|
| No MORK image is published; *Run here* needs one on the machine | Without one, setting up MORK says it can't run on this computer | Run `docker/up.sh` with `biocypher-mork/` present (it builds the image once), or use *Use existing* | `kg-service/backend/core/console/instances.py` |
| Folders outside Desktop, Documents, Downloads and `/Volumes` aren't shared | Choosing one says the console can't open it | Move or copy the data into a shared folder | `docker/docker-compose*.yml` |

### Neo4j and pipeline issues

| Issue | Effect | Workaround | Where |
|---|---|---|---|
| The root-compose `build`, `import` and `deploy` services are broken: `scripts/build.sh` runs `poetry install` and calls the pipeline with no arguments, and `scripts/import.sh` expects a `neo4j-admin` import script the writer never produces | No graph is produced | Use [Neo4j](#neo4j) | root `docker-compose.yml`, `scripts/build.sh`, `scripts/import.sh` |
| The Neo4j compose uses 4.x setting names (`NEO4JLABS_PLUGINS`, `dbms.memory.*`) on a 5.x image | Works, but logs deprecation warnings | None needed | `docker/docker-compose.neo4j.yml:20,35-37` |

---

## Deployment checklist

- [ ] Docker with Compose v2, uv and `libpq-dev` installed; outbound HTTPS and TCP `5432` allowed
- [ ] `make setup`; optionally `.env` with `BIOPORTAL_API_KEY`
- [ ] Full human builds: dbSNP cache built and `--dbsnp-cache-root` set
- [ ] **Neo4j:** `docker/neo4j.env` filled in, with `NEO4J_AUTH` matching `NEO4J_PASSWORD` and `NEO4J_OUTPUT_DIR` pointing at a `neo4j` build. Then `make neo4j-up`, load, and verify
- [ ] **MORK:** `biocypher-mork/.env` created and `mork_persist/` writable by UID 1000. Then `docker compose up -d --build`, load with `data_loader.py` (`DATASET_PATH` set to the `MORK_DATA_DIR` directory), and wait for a snapshot
- [ ] **kg-service:** `kg-service/.env` with every setting above, `ARCHIVE_BASE` set to `NEO4J_ARCHIVE_DIR` and `MORK_URL=http://localhost:8027`; then `uv run uvicorn` from `kg-service/`
- [ ] **Web Console:** started with `docker/up.sh` or `docker-compose.dist.yml` (no env file); the Builds folder, console data folder and Neo4j memory set in Settings; each species' Neo4j and MORK set up or connected there; your own data folders, if any, chosen there
