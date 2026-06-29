# Configuration Reference

This document is the complete reference for every configuration file, environment variable, and runtime option in BioCypher-KG.

---

## Overview

Configuration is spread across four layers, evaluated in this order (later layers override earlier ones):

1. **`config/biocypher_config.yaml`** — BioCypher framework settings (offline mode, Biolink model)
2. **`config/species_config.yaml`** — Species registry mapping species codes to config paths
3. **`config/<species>/<species>_adapters_config.yaml`** — Which adapters to run and their file paths
4. **CLI flags / environment variables** — Runtime overrides

---

## `config/biocypher_config.yaml`

**Source:** [`config/biocypher_config.yaml`](../../config/biocypher_config.yaml)  
Used by `BioCypher()` and all writer constructors (passed as `biocypher_config="config/biocypher_config.yaml"`).

| Field | Type | Default | Description |
|---|---|---|---|
| `biocypher.offline` | bool | `true` | Disable remote ontology lookups — required for air-gapped environments |
| `biocypher.debug` | bool | `false` | Enable verbose BioCypher debug logging |
| `biocypher.head_ontology.url` | str | `config/biolink-model.owl.ttl` | Path to the Biolink OWL model (local file, not URL) |
| `biocypher.head_ontology.root_node` | str | `entity` | Root node of the ontology graph |
| `biocypher.bmt_model_path` | str | `config/biolink-model.yaml` | Path to the Biolink YAML model |
| `neo4j.delimiter` | str | `'\t'` | Field delimiter for Neo4j CSV output |
| `neo4j.array_delimiter` | str | `'\|'` | Array field delimiter for Neo4j CSV output |
| `neo4j.skip_duplicate_nodes` | bool | `true` | Silently skip duplicate node IDs during writing |
| `neo4j.skip_bad_relationships` | bool | `true` | Silently skip edges with missing source/target nodes |

A Docker-specific variant exists at [`config/biocypher_docker_config.yaml`](../../config/biocypher_docker_config.yaml) with identical structure.

---

## `config/species_config.yaml`

**Source:** [`config/species_config.yaml`](../../config/species_config.yaml)  
Loaded by `load_species_config()` in `create_knowledge_graph.py`. Maps each species code and dataset type to its config paths.

### Structure

```yaml
<species_code>:          # e.g. hsa, dmel
  <dataset_type>:        # sample | full
    adapters_config: config/<species>/<species>_adapters_config_sample.yaml
    schema_config:   config/<species>/<species>_schema_config.yaml
    dbsnp_cache_root: ""   # path or "" (see notes)
    dbsnp_variant:    ""   # "common", "full", or ""
```

### Species codes

| Code | Species | `sample` | `full` |
|---|---|---|---|
| `hsa` | *Homo sapiens* | Yes (ships with repo) | Yes (requires local data) |
| `dmel` | *Drosophila melanogaster* | Yes | Yes |
| `cel` | *C. elegans* | Yes | Yes (ontology-only adapters) |
| `mmu` | *Mus musculus* | Yes | Yes (ontology-only adapters) |
| `rno` | *Rattus norvegicus* | No | Declared, but adapter/schema configs are missing |

### `dbsnp_cache_root` notes

- For `sample` runs: defaults to `aux_files/hsa/sample_dbsnp` (bundled ~150 KB cache). Leave blank to use the default.
- For `full` runs: **required** — set to the root of the dbSNP cache built by `scripts/update_dbsnp.py`.
  - Expected layout: `<root>/common/dbsnp_mapping.db` and `<root>/full/dbsnp_mapping.db`
- For Drosophila and non-dbSNP species: leave blank.

### `dbsnp_variant` notes

| Value | Size | Use case |
|---|---|---|
| `common` | ~1–2 GB (~15–25M variants, MAF ≥1%) | Development / beta |
| `full` | ~35–50 GB (~800M rsIDs) | Rare-variant coverage |
| `""` | N/A | Sample runs or species without dbSNP |

---

## Adapter config YAML structure

**Example:** [`config/hsa/hsa_adapters_config_sample.yaml`](../../config/hsa/hsa_adapters_config_sample.yaml)

Each entry declares one adapter invocation:

```yaml
<adapter_key>:              # unique key used for checkpointing and --include-adapters
  adapter:
    module: biocypher_metta.adapters.<module_name>   # Python module path
    cls: <ClassName>                                  # Class to instantiate
    args:                                             # Passed as kwargs to __init__
      filepath: ./samples/hsa/gencode_sample.gtf.gz  # Path args resolved against input_dir
      label: gene
      taxon_id: 9606
  outdir: gencode/gene     # Subdirectory under output_dir for this adapter's output
  nodes: True              # Whether to call get_nodes() on this adapter
  edges: False             # Whether to call get_edges() on this adapter
```

### Adapter config fields

| Field | Required | Description |
|---|---|---|
| `adapter.module` | Yes | Python dotted module path (e.g., `biocypher_metta.adapters.gencode_gene_adapter`) |
| `adapter.cls` | Yes | Class name to instantiate |
| `adapter.args` | Yes | Dict of keyword arguments passed to the class constructor |
| `outdir` | Yes | Output subdirectory under `--output-dir` |
| `nodes` | No | Default `True` — whether to call `get_nodes()` |
| `edges` | No | Default `False` — whether to call `get_edges()` |

### Path resolution in `args`

Args whose key ends in `filepath`, `dirpath`, `_file`, `_path`, `_tsv`, `_pkl`, or matches `filepaths`, `data_filepaths`, `aux_filepaths`, `feature_files`, `enhancer_gene_link` are treated as paths. Rules:

- Paths starting with `/` — absolute, not modified
- Paths starting with `./` or `../` — relative to the repository root, not modified  
- All other bare paths — prepended with `input_dir` (from the config's `input_dir:` field or `--input-dir` CLI flag)

The full adapter config may declare a top-level `input_dir:` field:

```yaml
input_dir: /mnt/hdd_1/biocypher-kg/input/hsa

gencode_gene:
  adapter:
    args:
      filepath: gencode/gencode.v49.annotation.gtf.gz  # → /mnt/hdd_1/.../gencode/...
```

---

## Environment variables

### Pipeline (`create_knowledge_graph.py`)

| Variable | Required | Description |
|---|---|---|
| `BIOPORTAL_API_KEY` | Yes (for Disease Ontology) | API key for BioPortal ontology downloads. Register free at [bioportal.bioontology.org/account](https://bioportal.bioontology.org/account). Set in `.env` file or shell. |

Loading: `python-dotenv` reads `.env` (if present) at startup. If `python-dotenv` is not installed, set variables manually in the shell. The `.env` file is gitignored.

**`.env.example`** ([`../../.env.example`](../../.env.example)):
```env
BIOPORTAL_API_KEY=your_bioportal_api_key_here
```

### kg-service (`kg-service/backend/core/config.py`)

Loaded via `pydantic_settings.BaseSettings` from a `.env` file in the `kg-service/` directory (or environment variables).

| Variable | Default | Description |
|---|---|---|
| `NEO4J_URI` | `bolt://localhost:27688` | Neo4j Bolt URI for the kg-service connection |
| `NEO4J_USER` | *(required)* | Neo4j username |
| `NEO4J_PASSWORD` | *(required)* | Neo4j password |
| `NEO4J_DATABASE` | *(required)* | Neo4j database name |
| `ARCHIVE_BASE` | `/mnt/hdd_1/biocypher-kg/output/human/biocypher-archives/` | **⚠️ Hardcoded to the Bizon server path.** Must be overridden for any other deployment. Base directory for KG version archives used by `Neo4jLoader`. |
| `VERSION_DIFF_SCRIPT` | `/home/abdum/services/biocypher-kg/version_diff.py` | **⚠️ Absolute path to Bizon server.** Path to `version_diff.py` — must be overridden. |
| `MORK_SUMMARY_SCRIPT` | `/home/abdum/services/biocypher-kg/get_mork_summary.py` | **⚠️ Absolute path to Bizon server.** Must be overridden. |
| `MORK_URL` | `http://localhost:8432` | **⚠️ Port conflict.** MORK container exposes port `8027` (see [`biocypher-mork/docker-compose.yml`](../../biocypher-mork/docker-compose.yml)). Set this to `http://localhost:8027` when running the MORK container with defaults. |
| `MORK_LIVE_STATS_ENABLED` | `false` | Enable live MORK statistics in API responses |
| `CACHE_TTL` | `300` | Cache TTL in seconds for API responses |
| `API_HOST` | `0.0.0.0` | FastAPI bind host |
| `API_PORT` | `8000` | FastAPI bind port |
| `DEBUG` | `true` | Enable FastAPI debug mode |
| `APP_NAME` | `BioCypher KG Observatory` | Application name (shown in API root response) |
| `APP_VERSION` | `0.1.0` | Application version |

> **Requires maintainer clarification:** `ARCHIVE_BASE`, `VERSION_DIFF_SCRIPT`, and `MORK_SUMMARY_SCRIPT` are hardcoded to paths on a specific server (`/mnt/hdd_1/biocypher-kg/...` and `/home/abdum/...`). These must be set via environment variable for any other deployment but have no per-environment documentation.

---

## Neo4j deployment environment (`docker/neo4j.env.example`)

**Source:** [`docker/neo4j.env.example`](../../docker/neo4j.env.example)  
Copy to `docker/neo4j.env` (gitignored) and fill in values.

| Variable | Default | Description |
|---|---|---|
| `NEO4J_IMAGE` | `neo4j:5.21` | Docker image for Neo4j |
| `NEO4J_CONTAINER` | `neo4j_bio_atomspace` | Container name |
| `NEO4J_HTTP_PORT` | `7674` | Host HTTP port (Neo4j browser at `http://localhost:7674`) |
| `NEO4J_BOLT_PORT` | `7887` | Host Bolt port (drivers connect here) |
| `NEO4J_AUTH` | `neo4j/your_password_here` | Auth in `username/password` format |
| `NEO4J_OUTPUT_DIR` | *(required)* | Host path mounted as `/import` inside Neo4j |
| `NEO4J_ARCHIVE_DIR` | *(required)* | Host path for KG version archives |
| `NEO4J_PAGECACHE` | `4G` | Neo4j page cache memory |
| `NEO4J_HEAP_INIT` | `2G` | JVM initial heap size |
| `NEO4J_HEAP_MAX` | `4G` | JVM maximum heap size |
| `NEO4J_URI` | `bolt://localhost:7887` | Bolt URI for the loader (must match `NEO4J_BOLT_PORT`) |
| `NEO4J_USERNAME` | `neo4j` | Loader username |
| `NEO4J_PASSWORD` | *(required)* | Loader password |
| `NEO4J_IMPORT_BATCH_SIZE` | `50000` | APOC batch size for LOAD CSV operations |

**Memory sizing guidance:**
- Full human KG (hsa) requires at minimum `NEO4J_HEAP_MAX=8G` and `NEO4J_PAGECACHE=8G`
- For development with sample data, the defaults (`4G`/`4G`) are sufficient

---

## MORK server environment (`biocypher-mork/docker-compose.yml`)

| Variable | Default | Description |
|---|---|---|
| `HOST_PORT` | `8027` | Host port for the MORK server. Exposed on container port `8027`. |

> **⚠️ Port conflict with kg-service:** `kg-service/backend/core/config.py` defaults `MORK_URL` to port `8432`. When running both services with defaults, set `MORK_URL=http://localhost:8027` in the kg-service environment.

---

## `config/yaml_loader.py` — YAML includes

**Source:** [`config/yaml_loader.py`](../../config/yaml_loader.py)

The `load_yaml_with_includes()` function extends standard YAML with a `!include <path>` directive that allows splitting large configs into separate files. Used by `load_species_config()` in `create_knowledge_graph.py`.

```yaml
# Example usage in a YAML file:
adapters:
  !include config/hsa/hsa_adapters_config_sample.yaml
```

---

## CLI flags (`create_knowledge_graph.py`)

All flags are defined by `typer.Option()` in `main()`. Run `uv run python create_knowledge_graph.py --help` for the live reference.

| Flag | Type | Default | Description |
|---|---|---|---|
| `--species` | str | — | Species code (`hsa`, `dmel`, `cel`, `mmu`, `rno`, `all`) |
| `--dataset` | str | `full` | `sample` or `full` |
| `--output-dir` | Path | — | **Required.** Output directory |
| `--adapters-config` | Path | — | Manual mode: path to adapter registry YAML |
| `--schema-config` | Path | — | Manual mode: path to schema config YAML |
| `--writer-type` | str | `metta` | One of: `metta`, `prolog`, `neo4j`, `parquet`, `networkx`, `KGX` |
| `--write-properties` / `--no-write-properties` | bool | `True` | Include node/edge properties in output |
| `--add-provenance` / `--no-add-provenance` | bool | `True` | Add dataset provenance metadata |
| `--include-taxon-id` / `--no-taxon-id` | bool | `True` | Include `taxon_id` on nodes/edges |
| `--include-curie` / `--no-curie` | bool | `False` | Keep CURIE namespace prefixes in IDs |
| `--dbsnp-cache-root` | Path | — | Root directory of dbSNP SQLite cache |
| `--dbsnp-variant` | str | — | `common` or `full` |
| `--include-adapters` | str[] | — | Space-separated list of adapter keys to run (default: all) |
| `--no-checkpoint` | bool | `False` | Disable checkpoint writes |
| `--resume` / `--restart` | bool | — | Resume from or delete existing checkpoint |
| `--skip-preflight` | bool | `False` | Skip pre-flight file path validation |
| `--check-only` | bool | `False` | Validate config paths and exit (no adapters run) |
| `--input-dir` | Path | — | Override `input_dir:` from adapter config YAML |
| `--manifest` | Path | — | Path to `download_manifest.json` for provenance |
| `--generate-data-source-schemas` / `--no-generate-data-source-schemas` | bool | `True` | Generate per-source schema YAML files |
| `--data-source-schema-output-dir` | Path | — | Override output directory for generated schemas |
| `--buffer-size` | int | `10000` | Parquet writer buffer size |
| `--overwrite` | bool | `True` | Overwrite existing Parquet files |
| `--species-config-path` | Path | `config/species_config.yaml` | Override species config location |

---

## Data source config (`config/<species>/<species>_data_source_config.yaml`)

Used by the download manager and versioning system. Not used directly by `create_knowledge_graph.py`.

**Example entry:**
```yaml
gencode:
  name: GENCODE v49
  version:
    strategy: url_regex
    pattern: 'gencode\.(v\d+)\.'
    vtype: sequential
  source_url: https://www.gencodegenes.org/
  url: https://ftp.ebi.ac.uk/pub/databases/gencode/Gencode_human/release_49/gencode.v49.annotation.gtf.gz
  license: CC BY 4.0
  checksum: true     # Set false for multi-GB files to skip SHA256

dbsnp:
  name: dbSNP
  version:
    strategy: http_head    # default: detect change via ETag/Last-Modified
  url: https://ftp.ncbi.nih.gov/snp/organisms/human_9606_b151_GRCh38p7/VCF/00-All.vcf.gz
  checksum: false          # too large to checksum
```

### Version strategies

| Strategy | Class | Behavior |
|---|---|---|
| `static` | `StaticGetter` | Fixed version string (`value: v11`) |
| `url_regex` | `UrlRegexGetter` | Extract version from URL via `pattern` regex |
| `http_head` | `HttpHeadGetter` | Detect changes via `ETag` / `Last-Modified` / `Content-Length` HTTP headers (default) |

See [dataset-versioning.md](../knowledge-graph/dataset-versioning.md) for the complete versioning specification.

---

## Configuration checklist for a new deployment

1. Copy `.env.example` → `.env` and fill in `BIOPORTAL_API_KEY`
2. Copy `docker/neo4j.env.example` → `docker/neo4j.env` and set:
   - `NEO4J_AUTH`, `NEO4J_OUTPUT_DIR`, `NEO4J_ARCHIVE_DIR`, `NEO4J_PASSWORD`
   - Tune memory: `NEO4J_PAGECACHE`, `NEO4J_HEAP_INIT`, `NEO4J_HEAP_MAX`
3. Create `kg-service/.env` (or set environment variables) with:
   - `NEO4J_USER`, `NEO4J_PASSWORD`, `NEO4J_DATABASE`
   - `ARCHIVE_BASE` — update from `/mnt/hdd_1/biocypher-kg/...` to your path
   - `VERSION_DIFF_SCRIPT`, `MORK_SUMMARY_SCRIPT` — update from `/home/abdum/...` to your path
   - `MORK_URL=http://localhost:8027` (match MORK container port)
4. For full (non-sample) runs: set `dbsnp_cache_root` in `config/species_config.yaml` or pass `--dbsnp-cache-root`
5. Run `make setup` to install dependencies via UV
