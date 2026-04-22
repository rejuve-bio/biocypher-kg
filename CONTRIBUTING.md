# Contributing to biocypher-kg

This guide covers how to add a new adapter for a biological dataset, update the schema, and get your changes through CI. Read the relevant sections for your contribution type.

---

## Table of contents

1. [Setup](#1-setup)
2. [Project structure](#2-project-structure)
3. [Implementing a new adapter](#3-implementing-a-new-adapter)
4. [Registering the adapter](#4-registering-the-adapter)
5. [Updating the schema](#5-updating-the-schema)
6. [Adding sample data for tests](#6-adding-sample-data-for-tests)
7. [Running tests locally](#7-running-tests-locally)
8. [Opening a PR](#8-opening-a-pr)

---

## 1. Setup

```bash
git clone https://github.com/rejuve-bio/biocypher-kg.git
cd biocypher-kg
pip install uv
uv sync
```

---

## 2. Project structure

```
biocypher_metta/
  adapters/               # One file per adapter
    __init__.py           # Base Adapter class
    helpers.py            # Shared utilities (check_genomic_location, to_float, …)
    gencode_gene_adapter.py
    string_ppi_adapter.py
    …
  processors/             # Reusable ID-mapping processors (HGNC, dbSNP, Ensembl↔UniProt, …)
  *_writer.py             # Output writers (metta, neo4j, prolog, parquet, kgx, networkx)

config/
  primer_schema_config.yaml          # Species-agnostic node/edge schema (shared base)
  hsa/
    hsa_schema_config.yaml           # Human-specific schema overrides
    hsa_adapters_config_sample.yaml  # Adapter registry used by tests
    hsa_adapters_config.yaml         # Full adapter registry used in production
    hsa_data_source_config.yaml      # Data source names and download URLs

samples/hsa/                         # Small sample files used by tests (committed to repo)
aux_files/hsa/                       # Larger generated/cached assets (not committed)

test/
  test.py                            # Test suite (schema validation + adapter smoke tests)
  conftest.py                        # Pytest fixtures and CLI options
```

---

## 3. Implementing a new adapter

### 3.1 Create the adapter file

Create `biocypher_metta/adapters/<datasource>_adapter.py`. Every adapter inherits from `Adapter` and implements `get_nodes()`, `get_edges()`, or both.

```python
from biocypher_metta.adapters import Adapter

class MyDatasetAdapter(Adapter):
    def __init__(self, filepath, label, write_properties, add_provenance):
        self.filepath = filepath
        self.label = label
        self.source = 'MyDataset'
        self.source_url = 'https://mydataset.org/'
        self.version = 'v1.0'
        super().__init__(write_properties, add_provenance)

    def get_nodes(self):
        # yield (node_id, label, props_dict)
        ...

    def get_edges(self):
        # yield (source_id, target_id, label, props_dict)
        ...
```

### 3.2 Node adapter — yield format

```python
def get_nodes(self):
    for record in self.parse_file():
        node_id = f"PREFIX:{record['id']}"   # use a CURIE prefix (ENSEMBL:, UniProtKB:, …)
        props = {}
        if self.write_properties:
            props = {
                'name': record['name'],
                'description': record['description'],
            }
            if self.add_provenance:
                props['source'] = self.source
                props['source_url'] = self.source_url
        yield node_id, self.label, props
```

### 3.3 Edge adapter — yield format

```python
def get_edges(self):
    for record in self.parse_file():
        source_id = f"UniProtKB:{record['protein_a']}"
        target_id = f"UniProtKB:{record['protein_b']}"
        props = {}
        if self.write_properties:
            props = {'score': record['score']}
            if self.add_provenance:
                props['source'] = self.source
                props['source_url'] = self.source_url
        yield source_id, target_id, self.label, props
```

### 3.4 Key conventions

| Convention | Detail |
|---|---|
| **Node IDs** | Always use CURIE format: `PREFIX:id` (e.g. `ENSEMBL:ENSG00000139618`, `UniProtKB:P04637`) |
| **`write_properties`** | Gate all property collection behind this flag — the KG pipeline controls it |
| **`add_provenance`** | Gate `source` and `source_url` behind this flag — always nest it inside `write_properties` |
| **`self.source` / `self.source_url`** | Set these in `__init__` so provenance is consistent across all output formats |
| **File parsing** | Prefer streaming (generators, line-by-line) over loading the full file into memory |
| **Processors** | Reuse existing processors from `biocypher_metta/processors/` for common ID mappings (HGNC, Ensembl↔UniProt, dbSNP, …) instead of rolling your own |

### 3.5 Heavy ontology adapters

If your adapter downloads and parses OWL/ontology files at runtime it is considered a **heavy adapter**. These are skipped during PR smoke tests to keep CI fast.

Add your adapter's module name to `SMOKE_SKIP_MODULE_PATTERNS` in `test/test.py` — that is the **single source of truth**. CI workflows and helper scripts read this list at runtime, so no other files need updating.

Only touch `.github/scripts/detect_heavy_adapter_changes.py` or the workflow files if the pattern-parsing mechanism itself needs to change.

---

## 4. Registering the adapter

### 4.1 Adapters config (`hsa_adapters_config_sample.yaml`)

Add an entry so CI can find and test your adapter. The key is a short descriptive name used throughout the pipeline.

```yaml
my_dataset:
  adapter:
    module: biocypher_metta.adapters.my_dataset_adapter
    cls: MyDatasetAdapter
    args:
      filepath: ./samples/hsa/my_dataset_sample.tsv
      label: my_node_label
  outdir: my_dataset
  nodes: true
  edges: false
```

For adapters that use dbSNP mappings:

```yaml
args:
  filepath: ./samples/hsa/my_dataset_sample.tsv
  dbsnp_rsid_map: null   # injected automatically by the test harness
  dbsnp_pos_map: null    # injected automatically by the test harness
```

### 4.2 Data source config (`hsa_data_source_config.yaml`)

Add an entry with the canonical download URL. This is the source of truth for where the full data comes from.

```yaml
my_dataset:
  name: My Dataset
  url: https://mydataset.org/download/my_dataset_v1.tsv.gz
```

If the source has multiple files:

```yaml
my_dataset:
  name: My Dataset
  url:
    - https://mydataset.org/download/file1.tsv.gz
    - https://mydataset.org/download/file2.tsv.gz
```

---

## 5. Updating the schema

Node and edge labels your adapter yields must be declared in the schema. Species-agnostic labels go in `config/primer_schema_config.yaml`; human-specific ones go in `config/hsa/hsa_schema_config.yaml`.

### 5.1 Adding a node

```yaml
my entity:
  represented_as: node
  is_a: biological entity          # must be a valid Biolink parent class
  mixins:
    - biolink:BiologicalEntity
  biolink_category: biolink:BiologicalEntity
  input_label: my_node_label       # must match the label you yield in get_nodes()
  inherit_properties: true
  properties:
    name:
      type: str
    source:
      type: str
      biolink: knowledge_source
    source_url:
      type: str
      biolink: source_web_page
```

### 5.2 Adding an edge

```yaml
my relationship:
  represented_as: edge
  is_a: related to
  label_as_edge: my_relationship   # used in graph databases
  input_label: my_relationship     # must match the label you yield in get_edges()
  source: my entity                # must match a declared node input_label
  target: gene                     # must match a declared node input_label
  properties:
    score:
      type: float
    source:
      type: str
    source_url:
      type: str
```

### 5.3 Finding valid Biolink parent classes

Browse the [Biolink model](https://biolink.github.io/biolink-model/) or look at existing entries in `primer_schema_config.yaml` for examples of how other biological entities are mapped.

---

## 6. Adding sample data for tests

Tests run against small sample files committed under `samples/hsa/`. The sample must be representative enough that `get_nodes()` or `get_edges()` yields at least one record.

- Keep sample files small — a few hundred rows is enough
- Match the exact format the adapter expects (same columns, same compression)
- Point `filepath` in `hsa_adapters_config_sample.yaml` to `./samples/hsa/your_sample_file`
- Large generated or cached assets (e.g. downloaded OWL files) go under `aux_files/` and are **not** committed

---

## 7. Running tests locally

**Smoke test** (fast — skips heavy ontology adapters and uses the configured smoke cap):
```bash
uv run pytest test/test.py --adapter-test-mode=smoke -v -s
```

**Full test** (required if you changed a heavy ontology adapter or the schema broadly):
```bash
uv run pytest test/test.py -v -s
```

**Test only your adapter** by temporarily limiting the config:
```bash
uv run pytest test/test.py \
  --adapters-config=config/hsa/hsa_adapters_config_sample.yaml \
  --adapter-test-mode=smoke \
  -v -s -k "test_adapter_nodes_in_schema or test_adapter_edges_in_schema"
```

**Run the full KG pipeline** for your adapter:
```bash
uv run python create_knowledge_graph.py \
  --output-dir output \
  --adapters-config config/hsa/hsa_adapters_config_sample.yaml \
  --schema-config config/hsa/hsa_schema_config.yaml \
  --writer-type metta \
  --dbsnp-mapping-path aux_files/hsa/sample_dbsnp/dbsnp_mapping.pkl
```

---

## 8. Opening a PR

**Branch naming:**
```
feat/add-<datasource>-adapter
fix/<adapter-name>-<short-description>
optimize/<area>
```

**Before pushing:**
- All items in the PR template checklist are complete
- `uv run pytest test/test.py --adapter-test-mode=smoke` passes locally
- Sample file is committed and `filepath` in the config points to it
- `hsa_data_source_config.yaml` has an entry with the download URL

**CI behaviour:**
- Pushing to a PR cancels any in-progress run for that branch automatically
- `test-schema` and `test-adapters` are separate workflows with different scopes
- CI may narrow the adapter/config set it runs based on detected file changes
- Writer tests may still run as a matrix, depending on which files changed
- Merge-to-`main` runs are intentionally broader than PR validation runs

When in doubt, open the current workflow files for the exact behavior:
- `.github/workflows/test-schema.yml`
- `.github/workflows/test-adapters.yml`
