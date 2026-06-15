# Ingestion Pipeline

This document describes how data moves from raw sources through the adapter layer into the output knowledge graph, and how dataset versions are tracked through that journey.

For the execution sequence diagram, see [data-flow.md](../architecture/data-flow.md). For the catalogue of all data sources, see [adapter-catalog.md](adapter-catalog.md).

---

## Overview

The ingestion pipeline has three separable stages:

1. **Download** — Fetch raw files from external sources, resolve versions, write manifest
2. **Adapt** — Read raw files via adapter classes, yield typed nodes/edges
3. **Write** — Serialize nodes/edges to the chosen output format

These stages are logically independent: you can download once and generate the KG multiple times in different formats, or run the pipeline against pre-existing data files without downloading.

---

## Stage 1: Download

**Entry point:** `biocypher_dataset_downloader/download_manager.py::DownloadManager`

```bash
# Interactive download
make download

# Direct download with parameters
make download-direct SPECIES=hsa DATASET=full OUTPUT_DIR=/data/hsa
```

The `DownloadManager` reads `config/<species>/<species>_data_source_config.yaml` and for each source:

1. Resolves the current version using one of three strategies (see below)
2. Downloads the file(s) via HTTP/FTP
3. Computes SHA256 checksum (unless `checksum: false` is set for large files)
4. Writes `download_manifest.json` to the output directory

### Version strategies

All three strategies are implemented in `biocypher_dataset_downloader/versioning/strategies.py`:

| Strategy | Class | How it resolves |
|---|---|---|
| `static` | `StaticGetter` | Returns a fixed `value:` string — no HTTP call needed |
| `url_regex` | `UrlRegexGetter` | Extracts version from the URL using a `pattern:` regex |
| `http_head` | `HttpHeadGetter` | Sends HTTP HEAD request; uses ETag/Last-Modified/Content-Length as version signature |

`http_head` is the default for sources using `current/` symlinks (e.g., Bgee) — it detects that the URL changed without knowing the new version string.

See [dataset-versioning.md](dataset-versioning.md) for the complete specification.

### `download_manifest.json`

Written to `<input_dir>/download_manifest.json` after a successful download run. The pipeline reads this automatically (via `ProvenanceLookup` in `biocypher_metta/provenance.py`) to attach version/checksum provenance to each adapter's output.

---

## Stage 2: Adapt

**Entry point:** `biocypher_metta/adapters/__init__.py::Adapter` (abstract base class)

Every adapter implements two generator methods:

```python
class GencodeGeneAdapter(Adapter):
    def get_nodes(self) -> Iterator[Tuple[str, str, Dict]]:
        # yields (node_id, label, properties_dict)
        # e.g. ("ENSEMBL:ENSG00000000003", "gene", {"gene_name": "FTSJ2", "chr": "X", ...})

    def get_edges(self) -> Iterator[Tuple[str, str, str, Dict]]:
        # yields (source_id, target_id, label, properties_dict)
        # e.g. ("ENSEMBL:ENST00000373020", "ENSEMBL:ENSG00000000003", "part_of_gene", {...})
```

**Key adapter properties:**
- `source` (str) — dataset name, used in `graph_info.json`
- `version` (str) — dataset version string
- `source_url` (str) — canonical URL for provenance

Adapters are instantiated dynamically by `process_adapters()` in `create_knowledge_graph.py`:

```python
module = importlib.import_module(adapter_entry["adapter"]["module"])
cls = getattr(module, adapter_entry["adapter"]["cls"])
instance = cls(**adapter_entry["adapter"]["args"])
```

### ID mapping processors

Five processors pre-build lookup dictionaries that adapters use to normalize IDs:

| Processor | Input | Output | Used by |
|---|---|---|---|
| `DBSNPProcessor` | dbSNP VCF | rsID → `{chr, pos, ref, alt}` | `dbsnp_adapter`, variant adapters |
| `EnsemblUniProtProcessor` | EBI Ensembl | ENSP → UniProt AC | `string_ppi_adapter` |
| `EntrezEnsemblProcessor` | NCBI Entrez | Entrez Gene ID → ENSEMBL | `gencode_gene_adapter`, others |
| `GOSubontologyProcessor` | OBO GO | GO term → subontology | `gaf_adapter` |
| `HGNCProcessor` | HGNC API | HGNC ID → gene symbol + xrefs | various gene adapters |

Processor caches are stored as pickle files in `aux_files/` and are rebuilt automatically when the source data changes.

### Ontology adapters

Ontology adapters (in `biocypher_metta/adapters/` — classes ending in `*OntologyAdapter`) follow a different pattern: they fetch OWL/OBO files via HTTP (or BioPortal API) rather than reading local files. They are typically the slowest adapters and are skipped in CI smoke mode.

---

## Stage 3: Write

**Entry point:** `biocypher_metta/__init__.py::BaseWriter` (abstract base class)

Writers are instantiated by `get_writer()` in `create_knowledge_graph.py`. All writers share the same interface:

```python
writer.write_nodes(nodes_generator, path_prefix)  # called once per adapter
writer.write_edges(edges_generator, path_prefix)  # called once per adapter
writer.finalize()                                  # called once after all adapters
```

Each adapter's output goes to a subdirectory determined by `outdir:` in the adapter config. For example, `outdir: gencode/gene` writes to `<output_dir>/gencode/gene/`.

See [writers.md](writers.md) for format-specific details.

---

## Post-processing

After all adapters complete:

1. **`gather_graph_info()`** — Aggregates node/edge counts, schema, and dataset metadata into `graph_info.json`
2. **`SchemaGenerator.generate()`** — Writes per-source YAML schemas to `data_source_schemas/<species>/` (auto-generated, do not edit manually)
3. **`CheckpointManager.delete()`** — Removes `kg_checkpoint.json` on success

---

## Provenance chain

Every node and edge written by an adapter that has `add_provenance=True` (the default) carries:

- `source` — dataset name (e.g., `"GENCODE"`)
- `source_url` — canonical source URL
- `version` — resolved version string (from `download_manifest.json`)

These are stored in the output files and in `graph_info.json`. When loaded into Neo4j, they are summarized in a `(:KGVersion).source_provenance_json` node.

---

## Resumability

The pipeline writes a checkpoint file after each adapter completes. If interrupted, the next run detects the checkpoint and offers to resume from the last completed adapter. All accumulated counts and dataset metadata are restored from the checkpoint.

See the checkpoint state machine in [data-flow.md](../architecture/data-flow.md#checkpoint-state-machine).

---

## Running a subset of adapters

```bash
# Run only specific adapters
make run-direct \
    ADAPTERS_CONFIG=./config/hsa/hsa_adapters_config.yaml \
    SCHEMA_CONFIG=./config/hsa/hsa_schema_config.yaml \
    OUTPUT_DIR=./output \
    INCLUDE_ADAPTERS="gencode_gene uniprotkb_sprot string_ppi"

# CLI equivalent
uv run python create_knowledge_graph.py \
    --adapters-config config/hsa/hsa_adapters_config.yaml \
    --schema-config config/hsa/hsa_schema_config.yaml \
    --output-dir ./output \
    --include-adapters gencode_gene uniprotkb_sprot string_ppi
```
