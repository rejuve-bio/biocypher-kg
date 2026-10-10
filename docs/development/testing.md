# Testing

This document covers the test suite, test modes, CI integration, and how to add tests for new adapters.

---

## Test structure

```
test/
├── conftest.py           # pytest options, path setup
├── test.py               # Main adapter + schema + writer validation tests
├── test_provenance.py    # ProvenanceLookup tests
└── test_versioning.py    # VersionInfo / strategy tests
```

Configuration in `pyproject.toml`:
```toml
[tool.pytest.ini_options]
testpaths = ["test"]
python_files = ["test.py", "test_*.py", "*_test.py"]
```

---

## Running tests

```bash
# Default (full mode)
make test
# equivalent to: uv run pytest

# Smoke mode (skips heavy ontology adapters, faster)
uv run pytest test/test.py --adapter-test-mode smoke

# Custom adapters config
uv run pytest test/test.py \
    --adapters-config config/hsa/hsa_adapters_config_sample.yaml \
    --adapter-test-mode full

# With coverage
uv run pytest test/ --cov=biocypher_metta --cov-report=term-missing

# Specific test file
uv run pytest test/test_versioning.py -v
```

---

## pytest options

All custom options are defined in `test/conftest.py`:

| Option | Default | Description |
|---|---|---|
| `--adapters-config` | `config/hsa/hsa_adapters_config_sample.yaml` | Path to the adapter registry YAML |
| `--primer-schema-config` | `config/primer_schema_config.yaml` | Path to the primer schema |
| `--species-schema-config` | `config/hsa/hsa_schema_config.yaml` | Path to the species schema |
| `--adapter-test-mode` | `full` | `smoke` or `full` |
| `--adapter-max-adapters` | `25` | Max adapters to test in smoke mode |
| `--adapter-profile` / `--no-adapter-profile` | enabled | Enable per-adapter runtime profiling |

---

## Test modes

### Full mode

Runs all adapters in the config, including heavy ontology adapters that fetch large OWL files from the internet. Used for pushes to `main`.

### Smoke mode

Skips adapters listed in `SMOKE_SKIP_MODULE_PATTERNS` in `test/test.py`. These are typically ontology adapters that require 100–500 MB downloads. Smoke mode is used for all PRs in CI to keep test time under ~5 minutes.

Smoke mode also limits the number of adapters sampled (default: 25 per node/edge test).

---

## CI test workflows

### `test-adapters.yml`

Triggers on pushes and PRs that touch adapters, writers, or configs.

**Smart matrix selection:**
- PRs: detects changed adapter files, tests only those adapters + all writers
- `main` push: tests all adapters

The `prepare_config.py` script in `.github/scripts/` generates a filtered adapter config containing only the changed adapters for PR runs.

**Caching:**
- `~/.cache/uv` — Python package cache (key: `requirements.txt`)
- `ontology_dataset_cache/` — OBO/OWL files (key: ontology file checksums)

### `test-schema.yml`

Triggers on PRs and pushes that touch schema configs or adapter files.

**Modes:**
- PR: smoke mode (skips heavy adapters), unless `detect_heavy_adapter_changes.py` finds that a heavy adapter changed
- `main` push: full mode

Runs `pytest test/test.py` with schema validation fixtures.

---

## What `test.py` validates

For each adapter in the config:

1. The adapter can be instantiated without errors
2. `get_nodes()` yields items that match the schema (node labels exist in schema YAML)
3. `get_edges()` yields items with source/target node types matching the schema edge definition
4. Property types match the declared schema types (e.g., `int` properties are not strings)

Writers are validated by running a full sample KG generation for each writer type.

---

## `test_provenance.py`

Tests `ProvenanceLookup` — reads a sample `download_manifest.json` and verifies that provenance is correctly attached to adapters.

## `test_versioning.py`

Tests the `VersionInfo` dataclass and all three version strategy classes (`StaticGetter`, `UrlRegexGetter`, `HttpHeadGetter`). Does not make external HTTP calls by default.

---

## Adding tests for a new adapter

There is no per-adapter test file needed. Adding the adapter to `config/hsa/hsa_adapters_config_sample.yaml` with a sample data file in `samples/hsa/` is sufficient — `test.py` picks it up automatically.

If the adapter requires a heavyweight download (ontologies, large databases), add its module name to `SMOKE_SKIP_MODULE_PATTERNS` in `test/test.py` so it is skipped in PR smoke mode.

---

## Coverage

Coverage is tracked by `pytest-cov`. The coverage report includes `biocypher_metta/` package.

```bash
uv run pytest test/ --cov=biocypher_metta --cov-report=html
open htmlcov/index.html
```
