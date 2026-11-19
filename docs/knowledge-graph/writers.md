# Output Writers

This document describes the output writer classes in BioCypher-KG. All writers subclass `BaseWriter` defined in `biocypher_metta/__init__.py`. Six writers are selectable via `create_knowledge_graph.py --writer-type` (metta, prolog, neo4j, parquet, kgx, networkx); `Neo4jWriter` exists as a class for custom scripts but is not currently wired to the pipeline CLI.

---

## BaseWriter interface

```python
class BaseWriter:
    def __init__(self, schema_config: str, biocypher_config: str,
                 output_dir: Path, include_curie: bool = False):
        ...

    def write_nodes(self, nodes: Iterator, path_prefix: str) -> None:
        """Write a generator of (node_id, label, props) tuples."""

    def write_edges(self, edges: Iterator, path_prefix: str) -> None:
        """Write a generator of (src, tgt, label, props) tuples."""

    def finalize(self) -> None:
        """Post-processing after all adapters complete."""
```

---

## Neo4j CSV (`neo4j_csv_writer.py`)

**Class:** `Neo4jCSVWriter`  
**Output:** TSV files + Cypher import scripts

The primary production writer. Generates:
- `nodes_<label>.csv` — tab-delimited node files
- `edges_<label>.csv` — tab-delimited edge files  
- `cypher/` — Cypher scripts for `LOAD CSV` import

Used by `make neo4j-load` and `make neo4j-load-direct` via `Neo4jLoader.load()`.

**Delimiter:** `\t` (tab), array delimiter: `|` (configured in `biocypher_config.yaml`).

---

## Neo4j direct (`neo4j_writer.py`)

**Class:** `Neo4jWriter`  
**Output:** Directly writes to a running Neo4j instance via the Bolt driver

Requires Neo4j to be running and accessible. Useful for incremental updates to an existing graph.

> **Note:** `Neo4jWriter` is not currently exposed via `create_knowledge_graph.py --writer-type`. Use it directly in custom scripts or through `kg-service`.

---

## MeTTa (`metta_writer.py`)

**Class:** `MeTTaWriter`  
**Output:** `.metta` files for the OpenCog Hyperon / MORK query engine

Generates MeTTa (Meta Type Talk) representation — a declarative logic format used by the Hyperon/OpenCog AI framework. Output is loaded into the MORK server for symbolic AI reasoning.

---

## Prolog (`prolog_writer.py`)

**Class:** `PrologWriter`  
**Output:** `.pl` files for logic programming systems

Generates Prolog facts representing nodes and edges. Suitable for logic-based reasoning and inference.

---

## Apache Parquet (`parquet_writer.py`)

**Class:** `ParquetWriter`  
**Output:** `.parquet` columnar files

Writes Apache Parquet format for analytics workloads. Supports batched writing via `buffer_size` (default 10,000 rows) and `overwrite` flag.

Constructor extra parameters:
```python
ParquetWriter(
    schema_config=...,
    biocypher_config=...,
    output_dir=...,
    buffer_size=10000,    # flush buffer every N rows
    overwrite=True,       # overwrite existing files
    include_curie=False
)
```

---

## KGX-style CSV (`kgx_writer.py`)

**Class:** `KGXWriter`  
**Output:** comma-separated CSV files with KGX/Biolink columns, plus a Cypher load script for each file. It does **not** write KGX JSON or JSON Lines.

In the adapter's output directory: for each node label, `<label>_nodes.csv` and `<label>_nodes.cypher`; for each edge label and source/target type, `<label>_<source>_<target>_edges.csv` and `..._edges.cypher`.

- **Node columns:** `id`, `label`, `category` (from the schema's `kgx_properties`, falling back to the label), any other `kgx_properties`, and the node's schema-declared properties.
- **Edge columns:** `id` (`<subject>_<label>_<object>`), `subject`, `object`, `label`, `source_type`, `target_type`, `predicate` (the schema's `biolink_predicate`, when the schema declares it), any `kgx_properties`, and the edge's schema-declared properties.
- Columns are sorted alphabetically. Array values are written as JSON arrays (CSV-quoted); in plain string values, commas and quotes are removed and `;` becomes a space.
- The `.cypher` scripts load the CSVs into Neo4j with `LOAD CSV` and APOC.

The writer stages records in temporary `temp_*.jsonl` files and deletes them once the CSVs are written.

The column names follow [KGX](https://github.com/biolink/kgx), but the files are comma-separated, not the tab-separated format that KGX's TSV reader expects by default, so check the delimiter before importing them with KGX tools.

---

## NetworkX (`networkx_writer.py`)

**Class:** `NetworkXWriter`  
**Output:** `networkx_graph.pkl`, a pickled `networkx.DiGraph`

Builds the KG as a NetworkX graph in memory and saves it with `pickle` (protocol 4) as `networkx_graph.pkl` in the adapter's output directory. Load it with `pickle.load` for Python-based graph analysis.

---

## Selecting a writer

Pass `--writer-type` to the CLI (case-insensitive):

```bash
uv run python create_knowledge_graph.py \
    --species hsa --dataset sample \
    --output-dir ./output \
    --writer-type metta   # or: neo4j, prolog, parquet, networkx, KGX
```

Or use `make run-sample WRITER_TYPE=neo4j`.
