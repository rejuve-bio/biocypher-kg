# Graph Construction

This document describes how the BioCypher framework validates and constructs the knowledge graph from adapter output. For the full execution sequence, see [data-flow.md](../architecture/data-flow.md).

---

## BioCypher integration

BioCypher is the semantic validation layer. It reads the merged schema YAML, loads the Biolink ontology, and validates that every node label and edge label from adapters maps to a known Biolink class.

**Key BioCypher call** in `create_knowledge_graph.py`:

```python
bcy = BioCypher(
    schema_config_path=str(schema_config_path),
    biocypher_config_path="config/biocypher_config.yaml",
)
schema = bcy._get_ontology_mapping()._extend_schema()
```

This returns the extended schema — a dict mapping each node/edge type to its Biolink class, properties, and validation rules.

---

## Schema merging

Before construction, the primer schema is merged with the species-specific schema:

```python
def merge_schemas(primer_path: str, species_path: Path) -> Path:
    # Reads both YAMLs, merges them, writes a temp file
    # Returns path to the merged schema
```

The merged schema is passed to both the writer (for output formatting) and to `BioCypher()` (for validation).

---

## Schema config format

Each entry in the schema YAML defines how BioCypher maps from adapter output labels to Biolink classes. See [data-model.md](data-model.md) for the full node/edge type reference.

Key fields used during graph construction:

| Field | Purpose |
|---|---|
| `input_label` | What the adapter yields as the label string |
| `output_label` | What is written to the output file (if different) |
| `represented_as` | `node` or `edge` |
| `is_a` | Parent type for property inheritance |
| `source` / `target` | Source and target node types (edges only) |
| `properties` | Typed property declarations |

---

## Node ID conventions

Node IDs follow CURIE (Compact URI) format:

| Entity | Prefix | Example |
|---|---|---|
| Gene (Ensembl) | `ENSEMBL:` | `ENSEMBL:ENSG00000000003` |
| Protein (UniProt) | `UniProtKB:` | `UniProtKB:P12345` |
| Transcript | `ENSEMBL:` | `ENSEMBL:ENST00000373020` |
| SNP | `dbSNP:` | `dbSNP:rs1234567` |
| Disease (DOID) | `DOID:` | `DOID:9351` |
| HPO phenotype | `HP:` | `HP:0001250` |
| GO term | `GO:` | `GO:0008150` |
| Reactome pathway | `REACTOME:` | `REACTOME:R-HSA-109581` |
| ChEBI compound | `CHEBI:` | `CHEBI:15422` |

CURIE prefixes can be optionally stripped from output using `--no-curie` (the default). With `--include-curie`, the prefix is preserved in the output.

---

## `preprocess_schema()` — edge-node type mapping

Before the adapter loop, `preprocess_schema()` builds a dict mapping every edge label to its source and target node types:

```python
edge_node_types = preprocess_schema(schema_config_path)
# e.g. {"interacts_with": {"source": "protein", "target": "protein", "output_label": "interacts_with"}}
```

This is passed to `gather_graph_info()` after the run to populate the `schema.edges` section of `graph_info.json`.

---

## Offline mode

`config/biocypher_config.yaml` sets `biocypher.offline: true`. This prevents BioCypher from making remote ontology calls at runtime. The Biolink model is loaded from the local files:

- `config/biolink-model.owl.ttl` — OWL representation
- `config/biolink-model.yaml` — YAML model specification

---

## `graph_info.json` output

After all adapters complete, `gather_graph_info()` writes:

```json
{
    "node_count": 1500000,
    "edge_count": 45000000,
    "schema": {
        "nodes": [{"data": {"id": "gene", "properties": [...]}}],
        "edges": [{"data": {"source": "protein", "target": "protein", "possible_connections": [...]}}]
    },
    "datasets": [...]
}
```

This file is the primary output metadata for the kg-service API.
