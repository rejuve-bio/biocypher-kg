# Local Development Setup

This document covers getting the project running locally for development. For operator deployment, see [deployment.md](../operations/deployment.md).

> This document wraps and extends the quickstart in the top-level [README.md](../../README.md) — check there for the authoritative make command reference.

---

## Prerequisites

- Python 3.10 or higher (check: `python3 --version`)
- [UV](https://github.com/astral-sh/uv) package manager
- Git

UV is required — `pip` is not used. Install UV if missing:
```bash
curl -LsSf https://astral.sh/uv/install.sh | sh
```

The `Makefile` will auto-install UV if it's missing when you run `make setup`.

---

## Initial setup

```bash
# 1. Clone
git clone https://github.com/rejuve-bio/biocypher-kg.git
cd biocypher-kg

# 2. Install dependencies (creates .venv/)
make setup
# equivalent to: uv sync

# 3. Verify installation
uv run python -c "import biocypher; print('OK')"
```

---

## Environment variables

The only required secret for most development work:

```bash
cp .env.example .env
# Edit .env and fill in BIOPORTAL_API_KEY
# (Only needed for DiseaseOntologyAdapter — you can skip this if not running that adapter)
```

The `.env` file is gitignored. `create_knowledge_graph.py` loads it via `python-dotenv` at startup.

---

## Running a sample build

The fastest way to verify your setup — uses the bundled sample data files in `samples/`:

```bash
# MeTTa format
make run-sample WRITER_TYPE=metta

# Neo4j CSV format
make run-sample WRITER_TYPE=neo4j

# With interactive prompts
make run
```

Output goes to `./output/` by default. The sample run uses `config/hsa/hsa_adapters_config_sample.yaml` and does not require downloading any external data.

---

## Running tests

```bash
# Run all tests (full mode — downloads ontologies, may be slow)
make test

# Run in smoke mode (skips heavy ontology adapters — faster, used in PRs)
uv run pytest test/test.py --adapter-test-mode smoke

# Run a specific test file
uv run pytest test/test_versioning.py -v

# Run with coverage
uv run pytest test/ --cov=biocypher_metta --cov-report=term-missing
```

See [testing.md](testing.md) for more detail on test modes and CI configuration.

---

## Validating your adapter config

Before a full run, check that all file paths in your adapter config exist:

```bash
# Sample config (uses bundled sample data)
make check-paths ADAPTERS_CONFIG=./config/hsa/hsa_adapters_config_sample.yaml

# Full config (requires data to be downloaded first)
make check-paths ADAPTERS_CONFIG=./config/hsa/hsa_adapters_config.yaml \
                 INPUT_DIR=/your/data/directory
```

---

## Working with the interactive CLI

```bash
# Interactive organism + config selector (uses questionary TUI)
uv run python biocypher_cli/cli.py
```

Supports Human and Drosophila melanogaster with default or custom configuration workflows.

---

## Downloading real data

For a full (non-sample) run, download the data sources first:

```bash
# Interactive download UI
make download

# Direct download
make download-direct SPECIES=hsa DATASET=full OUTPUT_DIR=/data/hsa
```

Full human data requires ~50+ GB disk space. The download writes `download_manifest.json` to the output directory, which the pipeline reads for version provenance.

---

## Developing a new adapter

See [coding-standards.md](coding-standards.md) for the complete adapter development guide. Quick reference:

1. Create `biocypher_metta/adapters/<name>_adapter.py`
2. Implement `get_nodes()` and/or `get_edges()` following the `Adapter` ABC
3. Add to `config/hsa/hsa_adapters_config_sample.yaml` for testing
4. Add sample data to `samples/hsa/`
5. Run `make run-sample` to validate

---

## IDE setup

Any IDE that supports Python works. For VS Code, the project uses a `.venv/` virtual environment created by UV. Point the Python interpreter to `.venv/bin/python`.

BioCypher-KG does not use a formatter or linter configuration file (no `.flake8`, `pyproject.toml` linter section, or `.pre-commit-config.yaml` — as of audit date). See [coding-standards.md](coding-standards.md) for style guidance.
