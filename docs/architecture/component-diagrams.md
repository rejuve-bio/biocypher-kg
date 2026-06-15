# Component Diagrams

This document contains Mermaid class and hierarchy diagrams for the major component groups. For execution sequence and state diagrams, see [data-flow.md](data-flow.md).

---

## Adapter hierarchy

```mermaid
classDiagram
    class Adapter {
        <<abstract>>
        +source: str
        +version: str
        +source_url: str
        +get_nodes() Iterator~tuple~
        +get_edges() Iterator~tuple~
    }

    class OntologyAdapter {
        +owl_url: str
        +get_nodes() Iterator
    }

    Adapter <|-- OntologyAdapter
    OntologyAdapter <|-- GeneOntologyAdapter
    OntologyAdapter <|-- DiseaseOntologyAdapter
    OntologyAdapter <|-- ChEBIOntologyAdapter
    OntologyAdapter <|-- CellOntologyAdapter
    OntologyAdapter <|-- CellLineOntologyAdapter
    OntologyAdapter <|-- UberonAdapter
    OntologyAdapter <|-- HsapDvOntologyAdapter
    OntologyAdapter <|-- OmimOntologyAdapter
    OntologyAdapter <|-- BrendaTissueOntologyAdapter
    OntologyAdapter <|-- ExperimentalFactorOntologyAdapter
    OntologyAdapter <|-- MolecularInteractionsOntologyAdapter
    OntologyAdapter <|-- SequenceOntologyAdapter

    Adapter <|-- GencodeGeneAdapter
    Adapter <|-- GencodeTranscriptAdapter
    Adapter <|-- GencodeExonAdapter
    Adapter <|-- UniProtAdapter
    Adapter <|-- UniProtProteinAdapter
    Adapter <|-- StringPPIAdapter
    Adapter <|-- StringCoexpressionAdapter
    Adapter <|-- ReactomeAdapter
    Adapter <|-- ReactomeEdgesAdapter
    Adapter <|-- BgeeAdapter
    Adapter <|-- GAFAdapter
    Adapter <|-- TFLinkAdapter

    note for Adapter "Defined in biocypher_metta/adapters/__init__.py\nget_nodes() yields (id, label, props)\nget_edges() yields (src, tgt, label, props)"
```

---

## Writer hierarchy

```mermaid
classDiagram
    class BaseWriter {
        <<abstract>>
        +schema_config: str
        +biocypher_config: str
        +output_dir: Path
        +write_nodes(nodes, path_prefix)
        +write_edges(edges, path_prefix)
        +finalize()
    }

    BaseWriter <|-- Neo4jCSVWriter
    BaseWriter <|-- Neo4jWriter
    BaseWriter <|-- MeTTaWriter
    BaseWriter <|-- PrologWriter
    BaseWriter <|-- ParquetWriter
    BaseWriter <|-- KGXWriter
    BaseWriter <|-- NetworkXWriter

    class Neo4jCSVWriter {
        +output: TSV files + Cypher
        +finalize(): merge files for neo4j-admin import
    }
    class MeTTaWriter {
        +output: .metta files
        +finalize(): write MeTTa representation
    }
    class ParquetWriter {
        +buffer_size: int
        +overwrite: bool
        +output: .parquet columnar files
    }

    note for BaseWriter "Defined in biocypher_metta/__init__.py\nAll writers take schema_config + biocypher_config"
```

---

## Processor hierarchy

```mermaid
classDiagram
    class BaseMappingProcessor {
        <<abstract>>
        +cache_path: Path
        +version_path: Path
        +fetch_data() Any
        +process_data(raw) dict
        +load_or_update() dict
    }

    BaseMappingProcessor <|-- DBSNPProcessor
    BaseMappingProcessor <|-- EnsemblUniProtProcessor
    BaseMappingProcessor <|-- EntrezEnsemblProcessor
    BaseMappingProcessor <|-- GOSubontologyProcessor
    BaseMappingProcessor <|-- HGNCProcessor

    class DBSNPProcessor {
        +cache: SQLite
        +load_mapping() dict
    }
    class EnsemblUniProtProcessor {
        +mapping: ENSP → UniProt AC
    }
    class EntrezEnsemblProcessor {
        +mapping: Entrez Gene ID → ENSEMBL
    }

    note for BaseMappingProcessor "Defined in biocypher_metta/processors/base_mapping_processor.py\nProcessors build ID mapping dicts, cached as pickle files\nin aux_files/{species}/"
```

---

## Download and versioning component map

```mermaid
classDiagram
    class DownloadManager {
        +config_path: str
        +output_dir: str
        +download_all()
        +download_source(name)
    }

    class VersionInfo {
        +version: str
        +signature: str
        +vtype: str
        +strategy: str
        +resolved_at: datetime
    }

    class VersionGetter {
        <<abstract>>
        +get(source_config) VersionInfo
    }

    VersionGetter <|-- StaticGetter
    VersionGetter <|-- UrlRegexGetter
    VersionGetter <|-- HttpHeadGetter

    DownloadManager --> VersionGetter: uses
    VersionGetter --> VersionInfo: returns
    DownloadManager --> VersionInfo: persists to download_manifest.json

    note for VersionInfo "Defined in biocypher_dataset_downloader/versioning/base.py"
    note for VersionGetter "Defined in biocypher_dataset_downloader/versioning/strategies.py"
```

---

## KG service architecture

```mermaid
graph TD
    subgraph FastAPI["kg-service FastAPI (port 8000)"]
        MAIN[main.py\nCORS + scheduler\n72h refresh]
        SR[/api/summary]
        UR[/api/updates]
        GIR[/api/graph-info\n/api/graph-info/status]
        VR[/api/databases/*\n/api/versions/*]
        DR[/api/databases\n/api/databases/{db_type}/status]
        ROOT["GET /\nGET /health"]
    end

    subgraph Core["core/"]
        CFG[config.py\nSettings Pydantic model]
        NC[neo4j_client.py\nNeo4j driver wrapper]
        GIC[graph_info_cache.py\n72h cached reader]
    end

    subgraph DB["External services"]
        NEO4J[Neo4j\nbolt://localhost:7887]
        MORK_SVC[MORK\nhttp://localhost:8027]
        FS[Local filesystem\nARCHIVE_BASE + graph_info.json]
    end

    MAIN --> SR
    MAIN --> UR
    MAIN --> GIR
    MAIN --> VR
    MAIN --> DR
    MAIN --> ROOT

    SR --> NC
    UR --> NC
    GIR --> GIC
    VR --> CFG
    VR --> FS

    NC --> NEO4J
    GIC --> FS
    VR --> MORK_SVC

    CFG --> NEO4J
    CFG --> MORK_SVC
```

> **Known issue:** `main.py` line 8 imports `meta` and `entities` from `backend.api.routes` but neither file exists. The FastAPI app cannot start until this is resolved. See [endpoints.md](../api/endpoints.md).

---

## Configuration inheritance

```mermaid
graph TD
    BIOLINK[config/biolink-model.owl.ttl\nBiolink ontology] --> BC
    BC[config/biocypher_config.yaml\nframework settings] --> WRITER[Writers]
    BC --> BIOCYPHER_CLS[BioCypher class]

    PRIMER[config/primer_schema_config.yaml\n36 nodes · 108 edges] --> MERGE
    HSA_SCHEMA[config/hsa/hsa_schema_config.yaml\nhuman extensions] --> MERGE
    DMEL_SCHEMA[config/dmel/dmel_schema_config.yaml\nDrosophila extensions] --> MERGE

    MERGE[merge_schemas()\nin create_knowledge_graph.py] --> WRITER
    MERGE --> BIOCYPHER_CLS

    SC[config/species_config.yaml] --> MAIN[create_knowledge_graph.py main()]
    HSA_AC[config/hsa/hsa_adapters_config.yaml] --> MAIN
    DMEL_AC[config/dmel/dmel_adapters_config.yaml] --> MAIN
    ENV[.env file\nBIOPORTAL_API_KEY] --> MAIN
