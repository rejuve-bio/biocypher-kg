# Output Writers

This document describes the seven output format writers available in BioCypher-KG. All writers subclass `BaseWriter` defined in `biocypher_metta/__init__.py`.

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

## KGX JSON (`kgx_writer.py`)

**Class:** `KGXWriter`  
**Output:** KGX (Knowledge Graph Exchange) JSON format

Generates standard KGX JSON for interoperability with the [KGX toolkit](https://github.com/biolink/kgx) and downstream knowledge graph tools.

---

## NetworkX (`networkx_writer.py`)

**Class:** `NetworkXWriter`  
**Output:** In-memory `networkx.MultiDiGraph` object

Builds the KG as a NetworkX graph in memory. Useful for Python-based graph analysis without persisting to disk.

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
