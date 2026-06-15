# Data Flow and Pipeline Diagrams

This document describes the execution sequence of the BioCypher-KG pipeline using Mermaid diagrams. For component descriptions and file locations, see [system-overview.md](system-overview.md).

---

## Pipeline execution sequence

The following diagram shows the top-level execution flow of `create_knowledge_graph.py::main()`.

```mermaid
sequenceDiagram
    autonumber
    actor User
    participant CLI as create_knowledge_graph.py
    participant SC as species_config.yaml
    participant AC as adapters_config.yaml
    participant CHK as CheckpointManager
    participant PF as Pre-flight check
    participant ADP as Adapter (×N)
    participant WRT as Writer
    participant SG as SchemaGenerator
    participant OUT as Output files

    User->>CLI: uv run python create_knowledge_graph.py --species hsa ...
    CLI->>CLI: load_dotenv() (BIOPORTAL_API_KEY etc.)
    CLI->>SC: load_species_config()
    SC-->>CLI: adapters_config path, schema_config path, dbsnp settings

    CLI->>CLI: merge_schemas(primer + species schema)
    CLI->>WRT: get_writer(writer_type, output_dir, schema_config)
    WRT-->>CLI: writer instance (Neo4jCSVWriter / MeTTaWriter / ...)

    CLI->>CLI: _load_dbsnp() — build rsID lookup dicts (if species uses dbSNP)
    CLI->>AC: _load_adapters_config() — resolve paths, attach provenance
    AC-->>CLI: adapters_dict (ordered dict of adapter entries)

    CLI->>CHK: CheckpointManager.load()
    alt Checkpoint exists
        CHK-->>CLI: completed_adapters list, restored counts
        CLI->>User: "Resume from checkpoint? [Y/N]"
        User-->>CLI: choice
    else No checkpoint
        CHK-->>CLI: empty state
    end

    CLI->>PF: _check_adapter_file_paths(adapters_dict)
    PF-->>CLI: missing paths dict
    alt Missing paths
        CLI->>User: ERROR: Pre-flight check failed — N adapters have missing file paths
        CLI->>CLI: Exit(1)
    end

    loop For each adapter_key in adapters_dict
        alt adapter_key in completed_adapters (checkpoint)
            CLI->>CLI: skip — already done
        else
            CLI->>ADP: importlib.import_module(adapter.module)
            CLI->>ADP: AdapterClass(**adapter.args)

            opt adapter.nodes == True
                ADP-->>CLI: get_nodes() → (node_id, label, props) generator
                CLI->>WRT: write_nodes(nodes, path_prefix)
                WRT-->>CLI: nodes written
                CLI->>CLI: accumulate nodes_count, nodes_props
            end

            opt adapter.edges == True
                ADP-->>CLI: get_edges() → (src, tgt, label, props) generator
                CLI->>WRT: write_edges(edges, path_prefix)
                WRT-->>CLI: edges written
                CLI->>CLI: accumulate edges_count
            end

            CLI->>CHK: CheckpointManager.save(completed_adapters, counts, elapsed)
            CHK->>OUT: kg_checkpoint.json (atomic write via .tmp)
        end
    end

    CLI->>WRT: writer.finalize()
    WRT->>OUT: merged/indexed output files

    CLI->>CLI: gather_graph_info(counts, schema_dict, output_dir)
    CLI->>OUT: graph_info.json

    CLI->>SG: SchemaGenerator.generate(adapters_dict, output_dir)
    SG->>OUT: data_source_schemas/<species>/*.yaml

    CLI->>CHK: CheckpointManager.delete() (success)
    CLI->>User: Done — total time, N nodes, M edges, slowest adapters
```

---

## Checkpoint state machine

The `CheckpointManager` class in [`checkpoint_manager.py`](../../checkpoint_manager.py) persists state to `<output_dir>/kg_checkpoint.json` after each adapter completes. This enables interrupted runs to resume from the last successfully completed adapter.

```mermaid
stateDiagram-v2
    [*] --> Fresh: No checkpoint file exists\n(or first run)
    
    Fresh --> Running: Pipeline starts\n(no checkpoint.json yet)
    
    Running --> Checkpointed: CheckpointManager.save()\ncalled after each adapter
    
    Checkpointed --> Running: Next adapter starts\n(still same run)
    
    Checkpointed --> Interrupted: Process killed / crash
    
    Interrupted --> Resuming: User runs pipeline again,\ncheckpoint.json detected,\n--resume flag or Y at prompt
    
    Interrupted --> Fresh: User chooses --restart\nor N at prompt\n→ checkpoint.json deleted
    
    Resuming --> Running: completed_adapters restored,\ncounts restored,\nelapsed_seconds restored
    
    Running --> Complete: All adapters finish,\nwriter.finalize() succeeds
    
    Complete --> [*]: CheckpointManager.delete()\nremoves checkpoint.json
```

### Checkpoint file schema

Written atomically (via `.tmp` → rename) to `<output_dir>/kg_checkpoint.json`:

```json
{
    "pipeline_id": "<output_dir>::<adapters_config>",
    "created_at": "2026-06-13T10:00:00",
    "updated_at": "2026-06-13T11:30:00",
    "completed_adapters": ["gencode_gene", "uniprotkb_sprot", ...],
    "failed_adapter": null,
    "nodes_count": {"gene": 62000, "protein": 20000, ...},
    "nodes_props": {"gene": ["gene_name", "gene_type"], ...},
    "edges_count": {"interacts_with|protein|protein": 4500000, ...},
    "datasets_dict": {...},
    "elapsed_seconds": 5400.0
}
```

`pipeline_id` is checked on resume — a mismatch (different output dir or different adapters config) silently discards the checkpoint and starts fresh.

---

## Versioning and download flow

This diagram shows how data source versions are tracked from configuration to the Neo4j provenance node.

```mermaid
flowchart TD
    A["config/hsa/hsa_data_source_config.yaml\n(URL, version strategy, license)"]
    B["biocypher_dataset_downloader/\ndownload_manager.py\nDownloadManager"]
    C["versioning/strategies.py\nStaticGetter / UrlRegexGetter / HttpHeadGetter"]
    D["VersionInfo dataclass\n(version, signature, vtype, resolved_at)"]
    E["<input_dir>/download_manifest.json\n(all sources, versions, SHA256 checksums)"]
    F["biocypher_metta/provenance.py\nProvenanceLookup"]
    G["adapter.args + provenance metadata\n(attached to each adapter entry)"]
    H["create_knowledge_graph.py\ngather_graph_info()"]
    I["<output_dir>/graph_info.json\n.datasets[] array"]
    J["kg-service/neo4j_loader.py\nNeo4jLoader.load()"]
    K["Neo4j\n(:KGVersion) node\nsource_provenance_json"]

    A --> B
    B --> C
    C --> D
    D --> E
    E --> F
    F --> G
    G --> H
    H --> I
    I --> J
    J --> K
```

For the full versioning specification, see [dataset-versioning.md](../knowledge-graph/dataset-versioning.md).

---

## Neo4j loading sequence

The `Neo4jLoader` in [`kg-service/neo4j_loader.py`](../../kg-service/neo4j_loader.py) supports incremental updates via surgical deletion of changed datasets.

```mermaid
sequenceDiagram
    participant User
    participant Loader as Neo4jLoader
    participant VM as VersionManager
    participant Neo4j

    User->>Loader: Neo4jLoader(uri, username, password, output_dir, archive_dir)
    Loader->>Neo4j: verify_connection()
    Neo4j-->>Loader: OK

    User->>Loader: load() or delete_changed_datasets() + load_new()

    Loader->>VM: hash_dataset_folder(source_name)
    VM-->>Loader: MD5 hash of output_dir/<source>

    Loader->>VM: get_source_from_csv(nodes_*.csv)
    VM-->>Loader: source name detected from CSV header

    alt Hash changed since last load
        Loader->>Neo4j: DELETE nodes/edges for changed source\n(surgical — only changed datasets)
        Neo4j-->>Loader: deleted
        Loader->>Neo4j: LOAD CSV / neo4j-admin import for changed source
        Neo4j-->>Loader: loaded
    else No change
        Loader->>Loader: skip source
    end

    Loader->>Neo4j: Create (:KGVersion) node with source_provenance_json
```

---

## Component hierarchy

```mermaid
classDiagram
    class Adapter {
        +source: str
        +version: str
        +source_url: str
        +get_nodes() Iterator[tuple]
        +get_edges() Iterator[tuple]
    }

    class BaseWriter {
        +write_nodes(nodes, path_prefix)
        +write_edges(edges, path_prefix)
        +finalize()
    }

    class BaseMappingProcessor {
        +fetch_data() Any
        +process_data(raw) dict
        +load_or_update() dict
    }

    Adapter <|-- GencodeGeneAdapter
    Adapter <|-- UniProtAdapter
    Adapter <|-- StringPPIAdapter
    Adapter <|-- ReactomeAdapter
    Adapter <|-- GeneOntologyAdapter
    Adapter <|-- "... 75 more adapters"

    BaseWriter <|-- Neo4jCSVWriter
    BaseWriter <|-- MeTTaWriter
    BaseWriter <|-- PrologWriter
    BaseWriter <|-- ParquetWriter
    BaseWriter <|-- KGXWriter
    BaseWriter <|-- NetworkXWriter
    BaseWriter <|-- Neo4jWriter

    BaseMappingProcessor <|-- DBSNPProcessor
    BaseMappingProcessor <|-- EnsemblUniProtProcessor
    BaseMappingProcessor <|-- EntrezEnsemblProcessor
    BaseMappingProcessor <|-- GOSubontologyProcessor
    BaseMappingProcessor <|-- HGNCProcessor

    Adapter --> BaseMappingProcessor: uses (constructor arg)
    GencodeGeneAdapter --> "config/species_config.yaml": reads
    StringPPIAdapter --> EnsemblUniProtProcessor: uses
```

---

## Pre-flight path validation flow

Before any adapter runs, `_check_adapter_file_paths()` validates all declared file paths:

```mermaid
flowchart TD
    A[Load adapters_dict] --> B[For each adapter entry]
    B --> C{arg key is a path key?\nfilepath, dirpath, _file, _path...}
    C -- No --> B
    C -- Yes --> D{Path.exists?}
    D -- Yes --> B
    D -- No --> E[Add to missing dict]
    E --> B
    B --> F{All adapters checked}
    F --> G{missing is empty?}
    G -- Yes --> H[Pre-flight passed → continue]
    G -- No --> I[Print grouped error report\nERROR: N adapter(s) have missing file paths]
    I --> J[Exit 1]
```

To skip this check: `--skip-preflight` flag or `make run-sample SKIP_PREFLIGHT=yes`.

To validate paths without running adapters: `make check-paths ADAPTERS_CONFIG=...` or `--check-only` flag.
