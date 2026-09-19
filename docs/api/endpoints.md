# API Endpoints Reference

The `kg-service` FastAPI application exposes REST endpoints for querying knowledge graph metadata, version history, and summary statistics.

**Source:** [`kg-service/backend/api/main.py`](https://github.com/rejuve-bio/biocypher-kg/blob/main/kg-service/backend/api/main.py)  
**Default port:** `8000` (configured via `API_PORT` in `kg-service/.env`)  
**OpenAPI docs:** `http://localhost:8000/docs` (Swagger UI, available when running)

> **Known issue:** `kg-service/backend/api/main.py` line 8 imports `meta` and `entities` from `backend.api.routes`, but neither file exists in `kg-service/backend/api/routes/`. The service **cannot start** until this is resolved.
>
> **Requires maintainer clarification:** Are `meta.py` and `entities.py` planned modules not yet committed, or deleted dead code? Until resolved, the two router registrations at lines 78–79 are non-functional.

---

## Base URL

All endpoints use base URL `http://<host>:8000`. The service runs `uvicorn` on `0.0.0.0:8000` by default (configured in `API_HOST` and `API_PORT`).

## Authentication

No authentication is configured. `main.py` registers `CORSMiddleware` with `allow_origins=["*"]`, `allow_credentials=True`, `allow_methods=["*"]`, `allow_headers=["*"]` — the service is fully open.

---

## Root and health endpoints

### `GET /`

Returns basic service metadata.

**Response:**
```json
{
    "name": "BioCypher KG Observatory",
    "version": "0.1.0",
    "docs": "/docs"
}
```

### `GET /health`

Neo4j connectivity check.

**Response:**
```json
{
    "status": "healthy",
    "neo4j": "connected"
}
```
`status` is `"unhealthy"` and `neo4j` is `"disconnected"` if `neo4j_client.verify_connection()` returns `False`.

---

## Summary router (`/api`)

**Source:** [`kg-service/backend/api/routes/summary.py`](https://github.com/rejuve-bio/biocypher-kg/blob/main/kg-service/backend/api/routes/summary.py)

### `GET /api/summary`

Returns a live summary of the Neo4j knowledge graph — node counts, edge counts, dataset list, schema labels, and database size. Queries Neo4j directly on every call (not cached).

**Response:**
```json
{
    "node_count": 1500000,
    "edge_count": 45000000,
    "last_updated_at": "2026-06-13",
    "dataset_count": 35,
    "top_entities": [{"label": "gene", "count": 62000}, ...],
    "top_connections": [{"label": "interacts_with", "count": 4500000}, ...],
    "datasets": [["gencode", "v49", "2026-01-15", "checksum"], ...],
    "database_size_gb": 12.4,
    "schema": {
        "node_types": ["gene", "protein", "disease", ...],
        "relationship_types": ["interacts_with", "expressed_in", ...]
    }
}
```

---

## Updates router (`/api`)

**Source:** [`kg-service/backend/api/routes/updates.py`](https://github.com/rejuve-bio/biocypher-kg/blob/main/kg-service/backend/api/routes/updates.py)

### `GET /api/updates`

Returns entities updated in Neo4j since a given timestamp.

**Query parameters:**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `since` | str (ISO timestamp) | — | Filter entities updated after this timestamp |
| `hours` | int | — | Filter entities updated in the last N hours |

If neither `since` nor `hours` is provided, defaults to the last 24 hours.

**Response:** Array of updated entity records (shape depends on `neo4j_client.get_updates_since()`).

---

## Graph-info router (`/api`)

**Source:** [`kg-service/backend/api/routes/graph_info.py`](https://github.com/rejuve-bio/biocypher-kg/blob/main/kg-service/backend/api/routes/graph_info.py)

The graph-info endpoints serve a cached version of `graph_info.json` produced by the pipeline. The cache is refreshed every **72 hours** by a background scheduler (`APScheduler`) registered in `main.py`. On startup, if no cache file exists, an initial generation is triggered 5 seconds later (non-blocking).

### `GET /api/graph-info`

Returns comprehensive graph information from the cache.

**Query parameters:**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `force_refresh` | bool | `false` | Force immediate regeneration instead of serving the cache |

**Response:** The full `graph_info.json` structure — node counts, edge counts, schema, dataset metadata. See `gather_graph_info()` in `create_knowledge_graph.py` for the exact shape.

**Cache behavior:**
- Default: serves `graph_info.json` from disk (fast — no Neo4j query)
- `?force_refresh=true`: calls `graph_info_cache.refresh()` (slower — regenerates from Neo4j)
- If no cache file exists: regenerates automatically

### `GET /api/graph-info/status`

Returns metadata about the cache file itself.

**Response:**
```json
{
    "cache_file": "/path/to/graph_info.json",
    "exists": true,
    "age_minutes": 45,
    "last_generated": "2026-06-13T10:00:00",
    "next_refresh_in_minutes": 15
}
```

---

## Versions and databases router (`/api`)

**Source:** [`kg-service/backend/api/routes/versions.py`](https://github.com/rejuve-bio/biocypher-kg/blob/main/kg-service/backend/api/routes/versions.py)

This is the most complex router. It manages version history for both Neo4j and MORK databases and reads archive directories from `ARCHIVE_BASE` (configured in `settings`).

> **⚠️ Configuration dependency:** Many endpoints in this router read from `settings.ARCHIVE_BASE`, `settings.VERSION_DIFF_SCRIPT`, and `settings.MORK_SUMMARY_SCRIPT` — all of which default to paths on a specific server. Override these in `kg-service/.env` for other deployments. See [configuration.md](../operations/configuration.md).

### `GET /api/databases`

Lists available databases.

**Response:**
```json
[
    {"name": "neo4j", "display_name": "Neo4j", "type": "graph", "enabled": true},
    {"name": "mork", "display_name": "MORK", "type": "atomspace", "enabled": true}
]
```

Note: This endpoint exists in **both** `versions.py` and `databases.py` with the same path — the registration order in `main.py` determines which one serves requests.

### `GET /api/databases/{db_type}/versions/latest`

Get the latest version record for a database.

| Path param | Values |
|---|---|
| `db_type` | `neo4j` or `mork` |

**Response (Neo4j):**
```json
{
    "database": "neo4j",
    "version": "v12",
    "build_id": "build-20260613",
    "created_at": "2026-06-13T10:00:00",
    "changed_datasets": ["gencode", "uniprot"],
    "unchanged_datasets": ["reactome", "hpo"],
    "dataset_versions": {"gencode": "v49", "uniprot": "2026_01"}
}
```

Reads from `(:KGVersion {db_type: "neo4j"})` nodes in Neo4j, ordered by `created_at DESC`.

### `GET /api/databases/{db_type}/versions`

List all version records for a database.

### `GET /api/databases/{db_type}/versions/compare/{version1}/{version2}`

Compare two archived versions by running `settings.VERSION_DIFF_SCRIPT`.

| Path param | Description |
|---|---|
| `db_type` | `neo4j` or `mork` |
| `version1` | Earlier version string (e.g. `v11`) |
| `version2` | Later version string (e.g. `v12`) |

**Query parameter:** `dataset` (optional) — limit comparison to one dataset.

**Returns:** JSON output of `version_diff.py`.

### `GET /api/databases/{db_type}/archives`

List all archived dataset versions under `ARCHIVE_BASE/{db_type}/`.

### `GET /api/databases/{db_type}/archives/{dataset}/{version}/stats`

Get size and file count for a specific archive.

### `GET /api/databases/{db_type}/archives/{dataset}/{version}/files`

List files in a specific archive.

### `GET /api/databases/{db_type}/stats/current`

Get live node/edge counts for the current database.

### `GET /api/databases/mork/summary`

Get MORK statistics (calls `settings.MORK_SUMMARY_SCRIPT`).

### Legacy endpoints (backward compatibility)

| Endpoint | Equivalent to |
|---|---|
| `GET /api/versions/latest` | `GET /api/databases/neo4j/versions/latest` |
| `GET /api/versions` | `GET /api/databases/neo4j/versions` |
| `GET /api/versions/compare/{v1}/{v2}` | `GET /api/databases/neo4j/versions/compare/{v1}/{v2}` |
| `GET /api/versions/archives` | `GET /api/databases/neo4j/archives` |

---

## Databases router (`/api`)

**Source:** [`kg-service/backend/api/routes/databases.py`](https://github.com/rejuve-bio/biocypher-kg/blob/main/kg-service/backend/api/routes/databases.py)

Note: `databases.py` imports `from backend.core.mork_client import MORKClient`. The module exists at `kg-service/backend/core/mork_client.py`, but the `GET /api/databases/mork/status` endpoint may still fail at runtime if MORK is unreachable due to the port mismatch (see [troubleshooting.md](../operations/troubleshooting.md)).

### `GET /api/databases`

Same as in `versions.py` — lists Neo4j and MORK.

### `GET /api/databases/{db_type}/status`

Get connection status for a database.

**Response (Neo4j, online):**
```json
{
    "database": "neo4j",
    "status": "online",
    "message": "Connected to Neo4j"
}
```

> **Note:** The MORK status endpoint will return a connection error if MORK is not reachable. The most common cause is the port mismatch between the MORK container (`8027`) and the kg-service default (`8432`). Set `MORK_URL=http://localhost:8027` in `kg-service/.env` to resolve.

---

## Background scheduler

`main.py` registers an `APScheduler BackgroundScheduler` that refreshes `graph_info.json` every **72 hours**:

```python
scheduler.add_job(refresh_graph_info, 'interval', hours=72, id='graph_info_refresh')
```

On startup, if the cache file does not exist, a one-time job runs 5 seconds after startup. On shutdown, the scheduler is stopped cleanly via the `shutdown` event handler.

There are two `@app.on_event("startup")` handlers in `main.py` (at lines 48 and 85) — FastAPI executes both in order. The first starts the scheduler; the second verifies the Neo4j connection.
