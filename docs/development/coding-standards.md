# Coding Standards

This document describes patterns and conventions for contributing to BioCypher-KG. It extends [CONTRIBUTING.md](../../CONTRIBUTING.md) — read that first for the PR workflow and schema update procedures.

---

## Adapter contract

Every adapter must subclass `Adapter` (defined in `biocypher_metta/adapters/__init__.py`):

```python
class Adapter:
    def __init__(self, write_properties: bool, add_provenance: bool):
        self.write_properties = write_properties
        self.add_provenance = add_provenance

    def get_nodes(self):
        pass

    def get_edges(self):
        pass
```

**What `get_nodes()` must yield:** 3-tuples `(node_id: str, label: str, properties: dict)`
- `node_id` — CURIE-style identifier (e.g., `"ENSEMBL:ENSG00000000003"`, `"UniProtKB:P12345"`)
- `label` — matches an `input_label` in the schema YAML (e.g., `"gene"`, `"protein"`)
- `properties` — dict of property values; must include `source` and `source_url` at minimum

**What `get_edges()` must yield:** 4-tuples `(source_id: str, target_id: str, label: str, properties: dict)`
- `source_id` / `target_id` — node IDs as above
- `label` — matches an `input_label` in the schema YAML (e.g., `"interacts_with"`)
- `properties` — dict; include `source`, `source_url`, and any edge-specific properties

**Constructor pattern:** adapters typically receive `filepath`, `label`, `taxon_id`, and any processor instances as kwargs. These come from the `args:` block in the adapter config YAML.

### Minimal example

```python
# biocypher_metta/adapters/my_source_adapter.py
from biocypher_metta.adapters import Adapter

class MySourceAdapter(Adapter):
    source = "MySource"
    version = "v1.0"
    source_url = "https://example.com/my-source"

    def __init__(self, filepath: str, label: str = "gene",
                 write_properties: bool = True, add_provenance: bool = True):
        super().__init__(write_properties, add_provenance)
        self.filepath = filepath
        self.label = label

    def get_nodes(self):
        with open(self.filepath) as f:
            for line in f:
                parts = line.strip().split("\t")
                node_id = f"ENSEMBL:{parts[0]}"
                props = {
                    "gene_name": parts[1],
                    "source": self.source,
                    "source_url": self.source_url,
                }
                yield node_id, self.label, props
```

---

## Adding an adapter to the config

In `config/hsa/hsa_adapters_config_sample.yaml`:

```yaml
my_source_nodes:
  adapter:
    module: biocypher_metta.adapters.my_source_adapter
    cls: MySourceAdapter
    args:
      filepath: ./samples/hsa/my_source_sample.tsv
      label: gene
      taxon_id: 9606
  outdir: my_source
  nodes: True
  edges: False
```

---

## Processor pattern

ID-mapping processors extend `BaseMappingProcessor` (defined in `biocypher_metta/processors/base_mapping_processor.py`). See `biocypher_metta/processors/README.md` for the full contract and cache file conventions.

---

## Schema updates

When adding a new node or edge type:

1. Add the type to `config/primer_schema_config.yaml` (shared) or the species-specific schema
2. Use the exact format from existing entries — `represented_as`, `input_label`, `is_a`, `properties`
3. Verify the new label appears in `input_label` of the schema and matches what `get_nodes()` / `get_edges()` yields
4. Run `make run-sample` to validate

---

## Style conventions

- Use `snake_case` for all Python identifiers (variables, functions, methods, module names)
- Class names use `PascalCase` with an `Adapter` suffix (e.g., `GencodeGeneAdapter`)
- Processor classes use `PascalCase` with a `Processor` suffix (e.g., `HGNCProcessor`)
- Keep adapter constructors flat — avoid nesting config logic inside `__init__`
- Use generators (`yield`) rather than building large lists in memory
- Validate `write_properties` before adding properties to the yielded dict:
  ```python
  props = {"source": self.source, "source_url": self.source_url}
  if self.write_properties:
      props["gene_name"] = row["name"]
  ```

---

## Testing a new adapter

```bash
# 1. Add sample data to samples/hsa/
# 2. Run the sample config
make run-sample WRITER_TYPE=neo4j

# 3. Run pytest in smoke mode
uv run pytest test/test.py --adapter-test-mode smoke \
    --adapters-config config/hsa/hsa_adapters_config_sample.yaml

# 4. Validate the output schema
uv run pytest test/test.py --adapters-config config/hsa/hsa_adapters_config_sample.yaml
```

---

## PR checklist (from CONTRIBUTING.md)

- [ ] Adapter yields correct node/edge labels matching the schema
- [ ] `source` and `source_url` properties are set on all yielded items
- [ ] Sample data added to `samples/`
- [ ] Adapter entry added to `hsa_adapters_config_sample.yaml` (and full config if applicable)
- [ ] Data source entry added to `hsa_data_source_config.yaml`
- [ ] `make run-sample` passes without errors
- [ ] `pytest` passes (at minimum in smoke mode)
