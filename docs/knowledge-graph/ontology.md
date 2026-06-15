# Ontology Integration

This document describes the 16 biological ontologies integrated into BioCypher-KG, how they are fetched and parsed, and their CURIE prefix conventions.

---

## Overview

Ontologies are integrated as node hierarchies. Each ontology adapter yields:
- Ontology terms as `ontology_term` (or a specific subtype) nodes
- Hierarchical relationships as `<ontology>_subclass_of` edges

All ontology adapters extend the base pattern in `biocypher_metta/adapters/ontologies_adapter.py` or a dedicated subclass that uses `rdflib` / `owlready2` to parse OWL/OBO files.

---

## Integrated ontologies

### Human ontologies

| Ontology | Full name | Adapter | Node type | Edge types | CURIE prefix | Fetch URL |
|---|---|---|---|---|---|---|
| **GO** | Gene Ontology | `gene_ontology_adapter.GeneOntologyAdapter` | `biological_process`, `molecular_function`, `cellular_component` | `biological_process_subclass`, `molecular_function_subclass`, `cellular_component_subclass` | `GO:` | `http://purl.obolibrary.org/obo/go.owl` |
| **DO** | Disease Ontology | `disease_ontology_adapter.DiseaseOntologyAdapter` | `disease` | `do_subclass_of` | `DOID:` | BioPortal API (requires `BIOPORTAL_API_KEY`) |
| **ChEBI** | Chemical Entities of Biological Interest | `chebi_ontology_adapter.ChEBIOntologyAdapter` | `small_molecule` | `chebi_subclass_of`, `chemical_substance_part_of_chemical_substance` | `CHEBI:` | `http://purl.obolibrary.org/obo/chebi.owl` |
| **CL** | Cell Ontology | `cell_ontology_adapter.CellOntologyAdapter` | `cell_type` | `cl_subclass_of`, `cl_capable_of`, `cl_part_of` | `CL:` | `http://purl.obolibrary.org/obo/cl.owl` |
| **CLO** | Cell Line Ontology | `cell_line_ontology_adapter.CellLineOntologyAdapter` | `cell_line` | `clo_subclass_of`, `cell_line_is_a_cell_type` | `CLO:` | Via CL |
| **UBERON** | Uber-anatomy Ontology | `uberon_adapter.UberonAdapter` | `anatomy` | `uberon_subclass_of` | `UBERON:` | `http://purl.obolibrary.org/obo/uberon.owl` |
| **HsapDv** | Human Developmental Stages | `hsapdv_ontology_adapter.HsapDvOntologyAdapter` | `developmental_stage` | hierarchy edges | `HsapDv:` | `http://purl.obolibrary.org/obo/hsapdv.owl` |
| **OMIM** | Online Mendelian Inheritance in Man | `omim_ontology_adapter.OmimOntologyAdapter` | `disease` | disease hierarchy | `OMIM:` | `http://purl.obolibrary.org/obo/mondo/sources/omim.owl` |
| **BTO** | BRENDA Tissue Ontology | `brenda_tissue_ontology_adapter.BrendaTissueOntologyAdapter` | `tissue` | `bto_subclass_of` | `BTO:` | `http://purl.obolibrary.org/obo/bto.owl` |
| **EFO** | Experimental Factor Ontology | `experimental_factor_ontology_adapter.ExperimentalFactorOntologyAdapter` | `experimental_factor` | `efo_subclass_of` | `EFO:` | EBI |
| **MI/PSI-MI** | Molecular Interactions Ontology | `molecular_interactions_ontology_adapter.MolecularInteractionsOntologyAdapter` | `molecular_interaction` | `mi_subclass_of` | `MI:` | `https://purl.obolibrary.org/obo/mi.owl` |
| **SO** | Sequence Ontology | `sequence_ontology_adapter.SequenceOntologyAdapter` | `sequence_type` | `so_subclass_of` | `SO:` | `http://purl.obolibrary.org/obo/so.owl` |

### Drosophila-specific ontologies

| Ontology | Full name | Adapter | Node type | CURIE prefix |
|---|---|---|---|---|
| **FBcv** | FlyBase Controlled Vocabulary | `dmel/FBcontrolled_vocabulary_ontology_adapter.FBcontrolledVocabularyOntologyAdapter` | `ontology_term` | `FBcv:` |
| **FBdv** | FlyBase Developmental Stages | `dmel/FBdevelopment_ontology_adapter.FBdevelopmentOntologyAdapter` | `developmental_stage` | `FBdv:` |
| **FBbt** | FlyBase Gross Anatomy | `dmel/FBgross_anatomy_ontology_adapter.FBgrossAnatomyOntologyAdapter` | `anatomy` | `FBbt:` |

### Generic OBO parser

`ontologies_adapter.OntologiesAdapter` — a generic OBO/OWL parser that can load any OBO Foundry ontology file. Used for ontologies that don't have a dedicated adapter.

---

## Ontology loading mechanism

Ontology adapters typically:

1. Fetch the OWL file via HTTPS from OBO Library (`http://purl.obolibrary.org/obo/`)
2. Parse with `rdflib` (for RDF/XML/OWL) or `obonet` (for OBO format)
3. Walk the hierarchy and yield term nodes + `is_a` / `part_of` edges

**Caching:** Downloaded OWL files are typically cached in the CI `ontology_dataset_cache/` directory (see `.github/workflows/test-adapters.yml`). In production, files may be re-fetched on each run unless cached locally.

**Offline requirement:** `biocypher_config.yaml` sets `offline: true`, which prevents BioCypher from making remote ontology validation calls. The ontology adapters themselves still fetch their source files from the internet.

---

## GAF annotations

GO annotations from the Gene Ontology Annotation (GAF) format are handled by `gaf_adapter.GAFAdapter`. Unlike ontology hierarchy adapters, `GAFAdapter` reads a `.gaf.gz` file (downloaded from GO Consortium or FlyBase) and yields gene→GO term association edges.

---

## CI behavior for ontology adapters

Ontology adapters are the slowest adapters (100–500 MB downloads, complex OWL parsing). They are listed in `SMOKE_SKIP_MODULE_PATTERNS` in `test/test.py` and skipped in PR smoke mode. They run only on full mode (pushes to `main`).

---

## Querying ontology hierarchy in Neo4j

```cypher
// All ancestors of a GO term
MATCH path = (start:biological_process {id: "GO:0008150"})-[:is_a*]->(ancestor:biological_process)
RETURN ancestor.term_name, length(path) as depth
ORDER BY depth

// Cell types capable of a biological process
MATCH (c:cell_type)-[:capable_of]->(p:biological_process)
WHERE p.term_name = "phagocytosis"
RETURN c.term_name

// Disease hierarchy from DOID
MATCH (d:disease {id: "DOID:9351"})-[:is_a*1..3]->(parent:disease)
RETURN parent.term_name
```

> **Note:** These queries use label and relationship names as defined by `output_label` values in the schema; they may vary between writer formats (Neo4j CSV vs. direct writer) and schema versions. Contributions welcome — see [CONTRIBUTING.md](../../CONTRIBUTING.md).
