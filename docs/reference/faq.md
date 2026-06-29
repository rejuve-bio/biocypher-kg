# Frequently Asked Questions

---

## General

**What is BioCypher-KG?**

BioCypher-KG is a pipeline that constructs a unified biological knowledge graph from 40+ data sources. It normalizes entities and relationships from genomics, proteomics, pathways, disease, expression, and regulatory data into a common schema based on the Biolink semantic model. See [system-overview.md](../architecture/system-overview.md).

**What species are supported?**

Human (`hsa`) and Drosophila melanogaster (`dmel`) are production-ready. Mouse (`mmu`) and C. elegans (`cel`) have ontology-only adapter configurations. Rat (`rno`) is declared but adapter/schema configs are missing and will raise `FileNotFoundError` if selected. See the species matrix in [system-overview.md](../architecture/system-overview.md).

**What output formats are available?**

MeTTa (OpenCog Hyperon), Neo4j CSV (bulk import), Neo4j direct (Bolt driver), Prolog, Apache Parquet, KGX JSON, and NetworkX. See [writers.md](../knowledge-graph/writers.md).

---

## Setup and installation

**Why do I need UV? Can I use pip?**

UV is required — the project uses `uv sync` and `uv run` throughout the Makefile and documentation. UV is faster and produces deterministic installs from the `uv.lock` file. Install UV from https://github.com/astral-sh/uv.

**What is the minimum Python version?**

Python 3.10 or higher (specified in `pyproject.toml` and `.python-version`).

**Do I need all 106 dependencies installed?**

`uv sync` installs the full dependency set. Some dependencies are only needed for specific adapters (e.g., `pysam` for genomic file handling, `psycopg2` for Reactome's PostgreSQL database). There is no optional dependency grouping.

---

## Running the pipeline

**What's the fastest way to verify the pipeline works?**

```bash
make setup
make run-sample WRITER_TYPE=metta
```

This uses bundled sample data and takes ~2 minutes.

**How do I resume an interrupted run?**

If the pipeline is interrupted, a `kg_checkpoint.json` file is left in `<output_dir>/`. On the next run, you'll be prompted to resume or restart. Or use `--resume` / `--restart` flags to skip the prompt.

**Why is the pipeline slow?**

Ontology adapters (GO, ChEBI, UBERON, etc.) fetch large OWL files from the internet and can take 10–30 minutes each. Use `--include-adapters` to run only the adapters you need, or ensure ontology files are cached locally.

**How do I run only a subset of adapters?**

```bash
uv run python create_knowledge_graph.py \
    --adapters-config config/hsa/hsa_adapters_config_sample.yaml \
    --schema-config config/hsa/hsa_schema_config.yaml \
    --output-dir ./output \
    --include-adapters gencode_gene uniprotkb_sprot string_ppi
```

**The pre-flight check fails with "missing file paths" — what do I do?**

Your adapter config declares input files that don't exist at the expected paths. Either:
1. Download the data with `make download`
2. Pass `--input-dir /your/data/path` to redirect paths
3. Use the sample config (`hsa_adapters_config_sample.yaml`) which uses bundled sample data

**How do I skip the BIOPORTAL_API_KEY requirement?**

The Disease Ontology adapter requires a BioPortal API key. If you don't need disease ontology data, exclude that adapter:
```bash
--include-adapters <adapters_without_disease_ontology>
```
Or register for a free key at https://bioportal.bioontology.org/account.

---

## Data sources

**How are dataset versions tracked?**

Each data source in `*_data_source_config.yaml` declares a version strategy. The downloader resolves the current version and writes a `download_manifest.json`. The pipeline reads this for provenance. See [dataset-versioning.md](../knowledge-graph/dataset-versioning.md).

**How do I know if my data sources are out of date?**

The CI workflow `check-dataset-versions.yml` runs every Monday and creates a GitHub issue if any source has changed. Locally: `uv run python -m biocypher_dataset_downloader.versioning.cli --species hsa`.

**Does the pipeline download data automatically?**

No — downloading and building the KG are separate steps. Download first with `make download`, then run the pipeline. The pipeline does not fetch data files during execution (except ontology adapters, which fetch OWL files at runtime).

---

## Schema and knowledge graph

**Where are the node and edge types defined?**

In `config/primer_schema_config.yaml` (shared) and `config/<species>/<species>_schema_config.yaml`. See [data-model.md](../knowledge-graph/data-model.md) for the human-readable reference.

**How do I add a new biological entity type?**

Add it to the schema YAML following the existing pattern, then create or update an adapter that yields that label. See [coding-standards.md](../development/coding-standards.md).

**What are the CURIE prefixes for each entity type?**

See the CURIE prefix column in [data-model.md](../knowledge-graph/data-model.md#node-types--primer-schema). CURIE prefixes can be stripped with `--no-curie` (the default).

---

## Neo4j and kg-service

**Why can't I connect to the kg-service?**

Check if the service started: `curl http://localhost:8000/health`. If the service won't start at all, see the known import error in [troubleshooting.md](../operations/troubleshooting.md#kg-service-startup-failure).

**Why is graph_info.json stale?**

The kg-service caches `graph_info.json` and refreshes it every 72 hours. Force a refresh: `curl "http://localhost:8000/api/graph-info?force_refresh=true"`. Check the cache age: `curl http://localhost:8000/api/graph-info/status`.

**The MORK endpoint returns connection errors.**

Check the port: `MORK_URL` in `kg-service/.env` defaults to `8432` but the MORK container uses `8027`. Set `MORK_URL=http://localhost:8027`. See [troubleshooting.md](../operations/troubleshooting.md#kg-service-cannot-reach-mork).

---

## Contributing

**Where do I find the adapter development guide?**

[CONTRIBUTING.md](../../CONTRIBUTING.md) covers the full workflow. [coding-standards.md](../development/coding-standards.md) covers the adapter contract and style conventions.

**How do I run CI locally?**

```bash
# Simulate PR smoke mode
uv run pytest test/test.py --adapter-test-mode smoke

# Simulate full CI (slow — downloads ontologies)
uv run pytest test/test.py --adapter-test-mode full
```

**Where are known issues tracked?**

GitHub Issues at https://github.com/rejuve-bio/biocypher-kg/issues. Key open issues to be aware of:
- `meta.py` and `entities.py` missing from `kg-service/backend/api/routes/` (service won't start)
- MORK port mismatch: container exposes `8027`, kg-service defaults to `8432` — set `MORK_URL=http://localhost:8027` in `kg-service/.env`
- Species `rno` declared in `species_config.yaml` but `config/rno/rno_adapters_config.yaml` and `config/rno/rno_schema_config.yaml` are missing
