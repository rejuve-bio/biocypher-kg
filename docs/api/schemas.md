# API Data Schemas

This document describes the data models and response shapes used by the kg-service API.

**Source files:**
- [`kg-service/backend/core/config.py`](https://github.com/rejuve-bio/biocypher-kg/blob/main/kg-service/backend/core/config.py) — `Settings` Pydantic model
- [`kg-service/backend/core/graph_info_cache.py`](https://github.com/rejuve-bio/biocypher-kg/blob/main/kg-service/backend/core/graph_info_cache.py) — cache reader
- [`kg-service/backend/api/routes/`](https://github.com/rejuve-bio/biocypher-kg/tree/main/kg-service/backend/api/routes/) — route handlers
- [`biocypher_dataset_downloader/versioning/base.py`](https://github.com/rejuve-bio/biocypher-kg/blob/main/biocypher_dataset_downloader/versioning/base.py) — `VersionInfo` dataclass

---

## Settings model

**Source:** `kg-service/backend/core/config.py`  
Loaded via `pydantic_settings.BaseSettings` — reads from environment variables or a `.env` file.

```python
class Settings(BaseSettings):
    NEO4J_URI: str = "bolt://localhost:27688"
    NEO4J_USER: str
    NEO4J_PASSWORD: str
    NEO4J_DATABASE: str
    ARCHIVE_BASE: str = "/mnt/hdd_1/biocypher-kg/output/human/biocypher-archives/"
    VERSION_DIFF_SCRIPT: str = "/home/abdum/services/biocypher-kg/version_diff.py"
    MORK_SUMMARY_SCRIPT: str = "/home/abdum/services/biocypher-kg/get_mork_summary.py"
    MORK_URL: str = "http://localhost:8432"
    MORK_LIVE_STATS_ENABLED: bool = False
    CACHE_TTL: int = 300
    API_HOST: str = "0.0.0.0"
    API_PORT: int = 8000
    DEBUG: bool = True
    APP_NAME: str = "BioCypher KG Observatory"
    APP_VERSION: str = "0.1.0"
```

See [configuration.md](../operations/configuration.md) for full field documentation and required overrides.

---

## `graph_info.json` schema

Produced by `gather_graph_info()` in `create_knowledge_graph.py` and served by `GET /api/graph-info`. The `GraphInfoCache` reads this file from disk.

```json
{
    "node_count": 1500000,
    "edge_count": 45000000,
    "dataset_count": 35,
    "last_updated_at": "2026-06-13",
    "kg_format": "neo4j",
    "data_size": "12.40 GB",
    "top_entities": [
        {"name": "gene", "count": 62000},
        {"name": "sequence_variant", "count": 750000}
    ],
    "top_connections": [
        {"name": "interacts_with", "count": 4500000}
    ],
    "frequent_relationships": [
        {"entities": ["protein", "protein"], "count": 4500000}
    ],
    "schema": {
        "nodes": [
            {"data": {"id": "gene", "properties": ["gene_name", "gene_type", "chr", "start", "end"]}}
        ],
        "edges": [
            {"data": {"source": "protein", "target": "protein", "possible_connections": ["interacts_with"]}}
        ]
    },
    "datasets": [
        {
            "name": "gencode",
            "version": "v49",
            "source_url": "https://www.gencodegenes.org/",
            "checksum": "sha256:abc123...",
            "import_date": "2026-06-13"
        }
    ]
}
```

---

## `VersionInfo` dataclass

**Source:** `biocypher_dataset_downloader/versioning/base.py`

```python
@dataclass
class VersionInfo:
    version: str              # resolved version string (e.g. "v49", "b151", "2026-01-15")
    signature: str            # ETag / Last-Modified / Content-Length (for http_head strategy)
    vtype: str                # "sequential", "semver", "date", "other"
    strategy: str             # "static", "url_regex", "http_head"
    resolved_at: datetime     # when this version was resolved
```

---

## `download_manifest.json` schema

Written by `DownloadManager` to `<input_dir>/download_manifest.json`.

```json
{
    "manifest_version": 1,
    "config_file": "config/hsa/hsa_data_source_config.yaml",
    "generated_at": "2026-06-13T10:00:00",
    "sources": {
        "gencode": {
            "name": "GENCODE v49",
            "version": "v49",
            "vtype": "sequential",
            "strategy": "url_regex",
            "source_url": "https://www.gencodegenes.org/",
            "files": [
                {
                    "rel_path": "gencode/gencode.v49.annotation.gtf.gz",
                    "sha256": "abc123...",
                    "size_bytes": 1234567890
                }
            ]
        }
    }
}
```

---

## `KGVersion` Neo4j node schema

Written by `Neo4jLoader` when loading data into Neo4j.

```cypher
// Schema of (:KGVersion) nodes
{
    db_type: "neo4j",            // or "mork"
    version: "v12",
    build_id: "build-20260613",
    created_at: "2026-06-13T10:00:00",
    changed_datasets: ["gencode", "uniprot"],
    unchanged_datasets: ["reactome", "hpo"],
    dataset_versions_json: "{...}",   // JSON-encoded VersionInfo per dataset
    source_provenance_json: "{...}"   // Full provenance from download_manifest.json
}
```

---

## `GET /api/summary` response shape

```json
{
    "node_count": 1500000,
    "edge_count": 45000000,
    "last_updated_at": "2026-06-13",
    "dataset_count": 35,
    "top_entities": [{"label": "gene", "count": 62000}],
    "top_connections": [{"label": "interacts_with", "count": 4500000}],
    "datasets": [["gencode", "v49", "2026-01-15", "checksum"]],
    "database_size_gb": 12.4,
    "schema": {
        "node_types": ["gene", "protein", "disease"],
        "relationship_types": ["interacts_with", "expressed_in"]
    }
}
```

---

## `GET /api/databases/{db_type}/versions/latest` response shape

```json
{
    "database": "neo4j",
    "version": "v12",
    "build_id": "build-20260613",
    "created_at": "2026-06-13T10:00:00",
    "changed_datasets": ["gencode", "uniprot"],
    "unchanged_datasets": ["reactome"],
    "dataset_versions": {
        "gencode": "v49",
        "uniprot": "2026_01"
    }
}
```

---

## `GET /api/graph-info/status` response shape

```json
{
    "cache_file": "/path/to/graph_info.json",
    "exists": true,
    "age_minutes": 45,
    "last_generated": "2026-06-13T10:00:00",
    "next_refresh_in_minutes": 15
}
```
