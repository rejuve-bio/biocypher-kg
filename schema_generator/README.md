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

### Generate All Schemas

```bash
python schema_generator/generate_data_source_schemas.py \
  --schema-config config/schema_config.yaml \
  --adapter-config config/adapters_config.yaml \
  --adapters-dir biocypher_metta/adapters \
  --output-dir data_source_schemas_generated
```

### Generate Schema for Specific Adapter(s)

Filter by adapter config name (only generates schema for that specific config entry):

```bash
# Single adapter
python schema_generator/generate_data_source_schemas.py \
  --schema-config config/schema_config.yaml \
  --adapter-config config/adapters_config.yaml \
  --adapters-dir biocypher_metta/adapters \
  --output-dir data_source_schemas_generated \
  --adapter promoter_ccre

# Multiple adapters
python schema_generator/generate_data_source_schemas.py \
  --schema-config config/schema_config.yaml \
  --adapter-config config/adapters_config.yaml \
  --adapters-dir biocypher_metta/adapters \
  --output-dir data_source_schemas_generated \
  --adapter promoter_ccre \
  --adapter proximal_enhancer_ccre
```

### Generate Schema by Adapter Module (Recommended for Complete Schemas)

Filter by Python adapter module (generates schema for ALL configs using that adapter class):

```bash
# Single module - includes all nodes and edges
python schema_generator/generate_data_source_schemas.py \
  --schema-config config/schema_config.yaml \
  --adapter-config config/adapters_config.yaml \
  --adapters-dir biocypher_metta/adapters \
  --output-dir data_source_schemas_generated \
  --module candidate_cis_regulatory_promoter_adapter

# Multiple modules
python schema_generator/generate_data_source_schemas.py \
  --schema-config config/schema_config.yaml \
  --adapter-config config/adapters_config.yaml \
  --adapters-dir biocypher_metta/adapters \
  --output-dir data_source_schemas_generated \
  --module candidate_cis_regulatory_promoter_adapter \
  --module uniprot_protein_adapter
```

**Note**: Use `--module` when you want a complete schema with all nodes AND edges from an adapter. The `--adapter` filter only includes the specific config entry, which may only have nodes or only edges.

### Incremental Schema Generation

The generator supports **incremental/append mode**. If a schema file already exists for a data source, new nodes and relationships will be merged with existing ones:

```bash
# Step 1: Generate schema for promoter adapter
python schema_generator/generate_data_source_schemas.py \
  --schema-config config/schema_config.yaml \
  --adapter-config config/adapters_config.yaml \
  --adapters-dir biocypher_metta/adapters \
  --output-dir data_source_schemas_generated \
  --module candidate_cis_regulatory_promoter_adapter

# Result: ENCODE.yaml with 1 node (promoter) + 1 relationship

# Step 2: Add enhancer adapter to existing ENCODE.yaml
python schema_generator/generate_data_source_schemas.py \
  --schema-config config/schema_config.yaml \
  --adapter-config config/adapters_config.yaml \
  --adapters-dir biocypher_metta/adapters \
  --output-dir data_source_schemas_generated \
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
python schema_generator/generate_data_source_schemas.py \
  --schema-config config/schema_config.yaml \
  --adapter-config config/adapters_config.yaml \
  --adapters-dir biocypher_metta/adapters \
  --output-dir data_source_schemas_generated \
  --source "REACTOME"

# Multiple data sources
python schema_generator/generate_data_source_schemas.py \
  --schema-config config/schema_config.yaml \
  --adapter-config config/adapters_config.yaml \
  --adapters-dir biocypher_metta/adapters \
  --output-dir data_source_schemas_generated \
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
4. **Adapter Name**: Falls back to adapter name as last resort

### 3. Property Validation

All extracted properties are validated against schema_config.yaml:
- Properties defined in schema → Use schema type (int, float, str, etc.)
- Properties not in schema → **Excluded** (strict validation)
- Inherited properties → Included via schema inheritance chain

### 4. Data Source Grouping

Adapters are grouped by their `source` metadata:
- Most adapters → Grouped by `self.source` value
- Ontology adapters → Grouped under "OBO Foundry"

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

- Python 3.7+
- PyYAML
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
