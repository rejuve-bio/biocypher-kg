# BioCypher-KG Documentation

Welcome to the BioCypher-KG documentation. This index maps every document to its audience and purpose.

---

## Start here

| Audience | First doc to read |
|---|---|
| New contributor | [project-structure.md](development/project-structure.md) → [local-development.md](development/local-development.md) → [coding-standards.md](development/coding-standards.md) |
| Developer integrating the API | [system-overview.md](architecture/system-overview.md) → [endpoints.md](api/endpoints.md) → [schemas.md](api/schemas.md) |
| Operator deploying the system | [deployment.md](operations/deployment.md) → [configuration.md](operations/configuration.md) → [monitoring.md](operations/monitoring.md) |
| Researcher querying the KG | [data-model.md](knowledge-graph/data-model.md) → [adapter-catalog.md](knowledge-graph/adapter-catalog.md) → [faq.md](reference/faq.md) |

---

## Architecture

| Document | Purpose |
|---|---|
| [system-overview.md](architecture/system-overview.md) | End-to-end system diagram, species/format matrices, key source files |
| [architecture.md](architecture/architecture.md) | Component dependency graph, architectural principles, known gaps |
| [data-flow.md](architecture/data-flow.md) | Pipeline sequence diagram, checkpoint state machine, versioning flow |
| [component-diagrams.md](architecture/component-diagrams.md) | Mermaid class hierarchy for adapters, writers, processors, services |

## Development

| Document | Purpose |
|---|---|
| [local-development.md](development/local-development.md) | Setup, sample run, environment variables |
| [project-structure.md](development/project-structure.md) | Every directory and file with purpose annotations |
| [coding-standards.md](development/coding-standards.md) | Adapter contract, schema patterns, PR checklist |
| [testing.md](development/testing.md) | Test modes, CI integration, adding tests |

## Operations

| Document | Purpose |
|---|---|
| [deployment.md](operations/deployment.md) | Docker deployment of Neo4j, MORK, and kg-service |
| [configuration.md](operations/configuration.md) | All config files, env vars, CLI flags — complete reference |
| [monitoring.md](operations/monitoring.md) | Dataset staleness, SonarQube, cache refresh, health checks |
| [troubleshooting.md](operations/troubleshooting.md) | Common failures and resolutions |

## API

| Document | Purpose |
|---|---|
| [endpoints.md](api/endpoints.md) | All REST endpoints with parameters and response shapes |
| [schemas.md](api/schemas.md) | Pydantic models, `graph_info.json`, `VersionInfo` dataclass |

## Knowledge Graph

| Document | Purpose |
|---|---|
| [data-model.md](knowledge-graph/data-model.md) | All 36+ node types and 108+ edge types — complete reference |
| [adapter-catalog.md](knowledge-graph/adapter-catalog.md) | All 40+ data sources: biology, species, node/edge types, URLs |
| [ontology.md](knowledge-graph/ontology.md) | 16 integrated ontologies, CURIE prefixes, fetch behavior |
| [ingestion-pipeline.md](knowledge-graph/ingestion-pipeline.md) | Download → adapt → write flow, provenance chain |
| [graph-construction.md](knowledge-graph/graph-construction.md) | BioCypher integration, schema merging, ID conventions |
| [writers.md](knowledge-graph/writers.md) | All 7 output formats — Neo4j CSV, MeTTa, Prolog, Parquet, KGX, NetworkX |

## Reference

| Document | Purpose |
|---|---|
| [glossary.md](reference/glossary.md) | Technical and biological term definitions |
| [faq.md](reference/faq.md) | Common questions across all audiences |

---

## Existing documentation (do not duplicate)

These files exist in the repository and should be linked to, not restated:

| File | Content |
|---|---|
| [README.md](../README.md) | Quickstart, make commands, Neo4j deployment |
| [CONTRIBUTING.md](../CONTRIBUTING.md) | Adapter development guide, PR workflow |
| [docs/knowledge-graph/dataset-versioning.md](knowledge-graph/dataset-versioning.md) | Complete versioning specification (moved from doc/) |
| [biocypher_metta/processors/README.md](../biocypher_metta/processors/README.md) | Processor usage and cache files |
| [schema_generator/README.md](../schema_generator/README.md) | Schema generation tool |
| [biocypher_dataset_downloader/versioning/README.md](../biocypher_dataset_downloader/versioning/README.md) | VersionInfo/VersionGetter contract |
