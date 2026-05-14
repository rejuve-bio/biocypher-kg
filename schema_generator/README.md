# Data Source Schema Generator

Automatically generates YAML schema files for each data source by analyzing BioCypher adapters.

## Overview

This tool analyzes adapter Python files to extract metadata and properties, cross-references them with the schema configuration, and generates one YAML schema file per data source.

## Features

- **Automatic Property Extraction**: Analyzes adapter code to find all properties using AST parsing
- **Parent Class Support**: Extracts properties from parent classes (e.g., OntologyAdapter)
- **Label Resolution**: Gets labels from adapter config with fallback to adapter metadata
- **URL Extraction**: Handles static URLs, f-strings, and dynamic URLs
- **Strict Validation**: Only includes properties defined in schema_config.yaml
- **Grouping**: Groups adapters by data source (provenance)
- **Property Inheritance**: Supports schema property inheritance chains

## Usage

### Automatic Generation During KG Creation

Datasource schemas are generated automatically when you run `create_knowledge_graph.py`.
The KG pipeline calls this generator after all selected adapters finish successfully and
after `graph_info.json` is written.

```bash
uv run python create_knowledge_graph.py \
  --species hsa \
  --dataset sample \
  --output-dir output/hsa_sample
```

For a full run:

```bash
uv run python create_knowledge_graph.py \
  --species hsa \
  --dataset full \
  --output-dir output/hsa_full \
  --dbsnp-cache-root /path/to/dbsnp \
  --dbsnp-variant common
```

The datasource schema generator uses the same adapter config selected by the KG run:

- `--dataset sample` uses `config/<species>/<species>_adapters_config_sample.yaml`
- `--dataset full` uses `config/<species>/<species>_adapters_config.yaml`
- `--include-adapters` narrows both the KG generation and datasource schema generation
- If no `--include-adapters` filter is passed, all adapters in the selected dataset config are analyzed

By default, species-mode output is written to:

```text
data_source_schemas/<species>
```

Manual mode writes to:

```text
<output-dir>/data_source_schemas
```

To override the schema output directory:

```bash
uv run python create_knowledge_graph.py \
  --species hsa \
  --dataset sample \
  --output-dir output/hsa_sample \
  --data-source-schema-output-dir data_source_schemas/hsa
```

To disable automatic schema generation:

```bash
uv run python create_knowledge_graph.py \
  --species hsa \
  --dataset sample \
  --output-dir output/hsa_sample \
  --no-generate-data-source-schemas
```

Checkpointing is still responsible for the expensive KG adapter work. Datasource schema
generation is a final post-processing step. If an adapter fails, schemas are not generated.
If schema generation fails, the checkpoint remains available so the KG run can be resumed.

### Standalone Script

You can still run the generator directly when you want to regenerate schemas without
building a KG.

### Generate All Schemas

```bash
uv run python schema_generator/generate_data_source_schemas.py \
  --schema-config config/hsa/hsa_schema_config.yaml \
  --adapter-config config/hsa/hsa_adapters_config.yaml \
  --adapters-dir biocypher_metta/adapters \
  --output-dir data_source_schemas/hsa
```

### Generate Schema for Specific Adapter(s)

Filter by adapter config name (only generates schema for that specific config entry):

```bash
# Single adapter
uv run python schema_generator/generate_data_source_schemas.py \
  --schema-config config/hsa/hsa_schema_config.yaml \
  --adapter-config config/hsa/hsa_adapters_config.yaml \
  --adapters-dir biocypher_metta/adapters \
  --output-dir data_source_schemas/hsa \
  --adapter promoter_ccre

# Multiple adapters
uv run python schema_generator/generate_data_source_schemas.py \
  --schema-config config/hsa/hsa_schema_config.yaml \
  --adapter-config config/hsa/hsa_adapters_config.yaml \
  --adapters-dir biocypher_metta/adapters \
  --output-dir data_source_schemas/hsa \
  --adapter promoter_ccre \
  --adapter proximal_enhancer_ccre
```

### Generate Schema by Adapter Module (Recommended for Complete Schemas)

Filter by Python adapter module (generates schema for ALL configs using that adapter class):

```bash
# Single module - includes all nodes and edges
uv run python schema_generator/generate_data_source_schemas.py \
  --schema-config config/hsa/hsa_schema_config.yaml \
  --adapter-config config/hsa/hsa_adapters_config.yaml \
  --adapters-dir biocypher_metta/adapters \
  --output-dir data_source_schemas/hsa \
  --module candidate_cis_regulatory_promoter_adapter

# Multiple modules
uv run python schema_generator/generate_data_source_schemas.py \
  --schema-config config/hsa/hsa_schema_config.yaml \
  --adapter-config config/hsa/hsa_adapters_config.yaml \
  --adapters-dir biocypher_metta/adapters \
  --output-dir data_source_schemas/hsa \
  --module candidate_cis_regulatory_promoter_adapter \
  --module uniprot_protein_adapter
```

**Note**: Use `--module` when you want a complete schema with all nodes AND edges from an adapter. The `--adapter` filter only includes the specific config entry, which may only have nodes or only edges.

### Incremental Schema Generation

The generator supports **incremental/append mode**. If a schema file already exists for a data source, new nodes and relationships will be merged with existing ones:

```bash
# Step 1: Generate schema for promoter adapter
uv run python schema_generator/generate_data_source_schemas.py \
  --schema-config config/hsa/hsa_schema_config.yaml \
  --adapter-config config/hsa/hsa_adapters_config.yaml \
  --adapters-dir biocypher_metta/adapters \
  --output-dir data_source_schemas/hsa \
  --module candidate_cis_regulatory_promoter_adapter

# Result: ENCODE.yaml with 1 node (promoter) + 1 relationship

# Step 2: Add enhancer adapter to existing ENCODE.yaml
uv run python schema_generator/generate_data_source_schemas.py \
  --schema-config config/hsa/hsa_schema_config.yaml \
  --adapter-config config/hsa/hsa_adapters_config.yaml \
  --adapters-dir biocypher_metta/adapters \
  --output-dir data_source_schemas/hsa \
  --module candidate_cis_regulatory_enhancer_adapter

# Result: ENCODE.yaml now has 2 nodes (promoter, enhancer) + 2 relationships
```

**Merging behavior**:
- New nodes are appended to the `nodes` section
- New relationships are appended to the `relationships` section
- If a node/relationship already exists, its properties are merged (not overwritten)
- Existing entries remain in place

### Generate Schema for Specific Data Source(s)

```bash
# Single data source
uv run python schema_generator/generate_data_source_schemas.py \
  --schema-config config/hsa/hsa_schema_config.yaml \
  --adapter-config config/hsa/hsa_adapters_config.yaml \
  --adapters-dir biocypher_metta/adapters \
  --output-dir data_source_schemas/hsa \
  --source "REACTOME"

# Multiple data sources
uv run python schema_generator/generate_data_source_schemas.py \
  --schema-config config/hsa/hsa_schema_config.yaml \
  --adapter-config config/hsa/hsa_adapters_config.yaml \
  --adapters-dir biocypher_metta/adapters \
  --output-dir data_source_schemas/hsa \
  --source "REACTOME" \
  --source "UniProt"
```

### Arguments

- `--schema-config`: Path to schema configuration YAML file
- `--adapter-config`: Path to adapters configuration YAML file
- `--adapters-dir`: Directory containing adapter Python files
- `--output-dir`: Output directory for generated schema files
- `--adapter`: (Optional) Generate schema only for specific adapter config name(s). Can be used multiple times.
- `--module`: (Optional) Generate schema for all adapters using specific Python module(s). Recommended for complete schemas with all nodes and edges. Can be used multiple times.
- `--source`: (Optional) Generate schema only for specific data source(s). Can be used multiple times.

## Output

Generates one YAML file per data source with the following structure:

```yaml
name: Data Source Name
website: https://datasource.org
nodes:
  node_type:
    url: https://datasource.org/download
    input_label: node_label
    description: Node description
    properties:
      property_name: type
relationships:
  relationship_type:
    url: https://datasource.org/download
    input_label: edge_label
    description: Edge description
    output_label: biolink_predicate
    source: source_node_type
    target: target_node_type
    properties:
      property_name: type
```

## How It Works

When invoked by `create_knowledge_graph.py`, the generator receives the already-loaded
adapter dictionary from the KG pipeline. This ensures it analyzes the same adapters that
were used to build the graph. When run as a standalone script, it loads `--adapter-config`
from disk.

### 1. Adapter Analysis

The tool uses AST (Abstract Syntax Tree) parsing to analyze adapter Python files:

- **Metadata Extraction**: Extracts `source`, `source_url`, `version`, `label` from `__init__` methods
- **Property Extraction**: Finds properties in `get_nodes()` and `get_edges()` methods
- **Pattern Detection**: Recognizes multiple property assignment patterns:
  - `props = {'key': value}`
  - `props['key'] = value`
  - `props.update({'key': value})`
  - `properties = {'key': value}`

### 2. Label Resolution

Labels are resolved in the following order:
1. **Config Args**: `adapter.args.label` in adapters_config.yaml
2. **Init Signature**: Default parameter values in `__init__`
3. **Init Assignment**: `self.label = 'value'` in `__init__`
4. **Dynamic Label Candidates**: Schema-valid string values from adapter args
5. **Adapter Label Maps**: String values from class-level dictionaries used as label maps
6. **Adapter Literals**: Schema-valid string literals in adapter code
7. **Yielded Labels**: Literal string labels yielded from `get_nodes()` or `get_edges()`
8. **Adapter Name**: Falls back to adapter name as last resort

Dynamic candidates are filtered against `schema_config.yaml` and the adapter mode. Node
adapters only keep schema entries represented as nodes, and edge adapters only keep
schema entries represented as edges. This lets one adapter config produce multiple
schema entries when the adapter emits several labels from a map or branch logic.

### 3. Property Validation

All extracted properties are validated against schema_config.yaml:
- Properties defined in schema → Use schema type (int, float, str, etc.)
- Properties not in schema → **Excluded** (strict validation)
- Inherited properties → Included via schema inheritance chain

### 4. Data Source Grouping

Adapters are grouped by their `source` metadata:
- Most adapters → Grouped by `self.source` value
- Ontology adapters → Grouped under "OBO Foundry"

Adapter modules in subpackages, such as `biocypher_metta.adapters.hsa.gwas_adapter`
or `biocypher_metta.adapters.dmel.gene_group_adapter`, are resolved relative to
`--adapters-dir`.

## Special Cases

### Ontology Adapters

Ontology adapters inherit from `OntologyAdapter` parent class:
- Properties extracted from **both** child and parent classes
- Parent class properties: `term_name`, `synonym`, `description`, `alternative_ids`, `rel_type`
- Default labels extracted from `__init__` signature defaults

### Reactome Pathway to GO

Special handling for adapters with dynamic label generation:
- Respects `label` parameter from config
- Falls back to dynamic generation if not provided
- Supports `properties` variable in addition to `props`

### Dynamic URLs

Handles f-string URLs by extracting constant parts:
```python
# In adapter:
self.source_url = f'https://www.bgee.org/download?id={self.taxon_id}'

# Extracted as:
url: 'https://www.bgee.org/download?id='
```

## Property Extraction Patterns

The analyzer recognizes these patterns in `get_nodes()` and `get_edges()` methods:

```python
# Pattern 1: Dictionary assignment
props = {'property1': value1, 'property2': value2}

# Pattern 2: Subscript assignment
props['property1'] = value1
props['property2'] = value2

# Pattern 3: Dictionary update
props.update({'property1': value1, 'property2': value2})

# Pattern 4: Alternative variable names
properties = {'property1': value1}
_props = {'property1': value1}
```

## Requirements

- Python 3.10+
- Project dependencies installed with `uv sync`
- BioCypher adapters with standard structure

## Example Output

```bash
✓ Schema generation complete! Output directory: data_source_schemas_generated
  Generated 30 schema files

Generated schemas:
- ABC.yaml
- CADD.yaml
- OBO_Foundry.yaml
- REACTOME.yaml
- STRING.yaml
...
```

## Validation

After generation, ensure:
1. **All adapters processed**: Check for warnings about missing schema configs
2. **Properties included**: Verify all expected properties appear in output
3. **No duplicate entries**: Each type appears once per data source
4. **URLs present**: Check URLs are extracted (may be partial for dynamic URLs)

## Troubleshooting

### Missing Properties

If properties are missing from output:
1. Check if property is defined in `schema_config.yaml`
2. Verify property is in `get_nodes()` or `get_edges()` method
3. Ensure property name doesn't match exclusion list (`source`, `source_url`)

### Missing Adapter

If adapter doesn't appear in output:
1. Check adapter has valid `source` metadata
2. Verify adapter is listed in `adapters_config.yaml`
3. Ensure label exists in `schema_config.yaml`

### Wrong Data Source Grouping

If adapter appears under wrong data source:
1. Check `self.source` value in adapter `__init__`
2. Verify `source_url` is set correctly
3. For ontology adapters, ensure they inherit from `OntologyAdapter`

## Contributing

When adding new adapter patterns:
1. Update `AdapterAnalyzer.get_properties_from_method()` for new property patterns
2. Update `AdapterAnalyzer.get_metadata_from_init()` for new metadata patterns
3. Add test case to verify extraction works correctly
4. Update this README with new pattern documentation
