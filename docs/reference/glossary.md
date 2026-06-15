# Glossary

Definitions for technical and biological terms used throughout the BioCypher-KG documentation.

---

## Technical terms

**Adapter** — A Python class in `biocypher_metta/adapters/` that reads a specific data source and yields typed nodes and edges. All adapters implement `get_nodes()` and/or `get_edges()` following the `Adapter` ABC contract.

**Adapter config** — A YAML file (`*_adapters_config.yaml`) that declares which adapters to run, their file paths, and their output directories. One entry per adapter invocation.

**ARCHIVE_BASE** — The filesystem path where kg-service stores versioned archives of Neo4j/MORK datasets. Configured via `ARCHIVE_BASE` in `kg-service/.env`. Defaults to a server-specific path — must be overridden for new deployments.

**BaseWriter** — Abstract base class for all output writers (`biocypher_metta/__init__.py`). Defines `write_nodes()`, `write_edges()`, and `finalize()`.

**BioCypher** — The semantic framework that validates node/edge labels against the Biolink model. Used for schema merging and ontology mapping. See [graph-construction.md](../knowledge-graph/graph-construction.md).

**Biolink model** — A standardized semantic model for biomedical knowledge graphs. Defines the class hierarchy (Gene, Disease, Protein, etc.) that BioCypher-KG maps onto. Stored locally at `config/biolink-model.yaml` and `config/biolink-model.owl.ttl`.

**Checkpoint** — A JSON file (`kg_checkpoint.json`) written to the output directory after each adapter completes. Enables interrupted pipeline runs to resume from the last successful adapter.

**CURIE** — Compact URI — a prefixed identifier like `ENSEMBL:ENSG00000000003` or `UniProtKB:P12345`. CURIE prefixes can be stripped using `--no-curie` (the default behavior).

**Dataset** — In this codebase, a "dataset" refers to a single data source integration (e.g., GENCODE, STRING, HPO). Each dataset produces one or more output files.

**`graph_info.json`** — A summary file written after the pipeline run, containing node/edge counts, schema, and dataset metadata. Served by the kg-service API.

**input_dir** — The base directory for resolving relative file paths in adapter configs. Set via `input_dir:` in the YAML or `--input-dir` CLI flag.

**KGX** — Knowledge Graph Exchange format — a JSON standard for representing biological knowledge graphs, maintained by the Biolink project.

**MeTTa** — Meta Type Talk — the programming/logic language used by the OpenCog Hyperon AI framework. BioCypher-KG can export to MeTTa format for symbolic reasoning.

**MORK** — A Rust-based server for OpenCog Hyperon's MeTTa language. Loads `.metta` files and serves queries. Exposes a REST/HTTP API on port 8027.

**output_label** — The relationship type written to output files, which may differ from `input_label`. Set in the schema YAML; used as Neo4j relationship types.

**Primer schema** — `config/primer_schema_config.yaml` — the shared base schema defining 36 node types and 108 edge types. All species-specific schemas extend this.

**Processor** — A Python class in `biocypher_metta/processors/` that builds ID mapping dictionaries with a local pickle/SQLite cache. Examples: `HGNCProcessor`, `DBSNPProcessor`.

**Pre-flight check** — Validation that all file paths declared in an adapter config exist on disk, run before any adapter executes. Bypass with `--skip-preflight`.

**Provenance** — Metadata attached to each dataset: version, source URL, checksum, license. Stored in `download_manifest.json` and `graph_info.json`.

**Schema config** — A YAML file (`*_schema_config.yaml`) defining node and edge types, their Biolink mappings, and property declarations.

**Smoke mode** — A faster test mode that skips heavy ontology adapters and limits the number of adapters tested. Used for PR CI runs.

**Species code** — Two-letter or three-letter abbreviation for a species: `hsa` (human), `dmel` (Drosophila), `mmo` (mouse), `cel` (C. elegans), `rno` (rat).

**UV** — A fast Python package manager from Astral (https://github.com/astral-sh/uv) used instead of pip. Required for this project.

**WAL** — Write-Ahead Log — a file (`mork_persist/wal.metta`) that records MORK write operations for crash recovery.

**Writer** — A format-specific serializer that converts node/edge generators into output files (Neo4j CSV, MeTTa, Prolog, Parquet, KGX, or NetworkX).

---

## Biological terms

**eQTL** — Expression Quantitative Trait Locus — a genomic locus where genetic variation is associated with changes in gene expression.

**GWAS** — Genome-Wide Association Study — a study linking genetic variants to phenotypic traits across many individuals.

**HPO** — Human Phenotype Ontology — a standardized vocabulary for describing human phenotypes, especially in the context of genetic diseases.

**LD** — Linkage Disequilibrium — non-random association of alleles at two or more loci in a population.

**ncRNA** — Non-coding RNA — RNA molecules that are not translated into protein.

**PPI** — Protein-Protein Interaction — a physical or functional interaction between two protein molecules.

**rsID** — Reference SNP cluster identifier (e.g., `rs1234567`) — a unique identifier for a SNP in the dbSNP database.

**SNP** — Single Nucleotide Polymorphism — a variation at a single DNA position.

**TAD** — Topologically Associating Domain — a region of the genome that folds into a 3D structure, within which DNA-DNA interactions are enriched.

**TF** — Transcription Factor — a protein that regulates gene expression by binding to specific DNA sequences.

**TFBS** — Transcription Factor Binding Site — a specific DNA sequence recognized by a transcription factor.

---

## Data source abbreviations

| Abbreviation | Full name |
|---|---|
| ABC | Activity-By-Contact model (enhancer prediction) |
| AGR / Alliance | Alliance of Genome Resources |
| BTO | BRENDA Tissue Ontology |
| CADD | Combined Annotation Dependent Depletion |
| CATLAS | Single-Cell Chromatin Accessibility Atlas |
| ChEBI | Chemical Entities of Biological Interest |
| CL | Cell Ontology |
| CLO | Cell Line Ontology |
| DGV | Database of Genomic Variants |
| DO | Disease Ontology |
| EFO | Experimental Factor Ontology |
| ENCODE | Encyclopedia of DNA Elements |
| EPD | Eukaryotic Promoter Database |
| FAVOR | Functional Annotation of Variants Online Resource |
| GENCODE | Genome annotation project (EMBL-EBI) |
| GO | Gene Ontology |
| GTEx | Genotype-Tissue Expression project |
| HGNC | HUGO Gene Nomenclature Committee |
| HOCOMOCO | HOComprehensive MOdelling of COoperative regulation |
| HPO | Human Phenotype Ontology |
| KGX | Knowledge Graph Exchange |
| MI | Molecular Interactions Ontology (PSI-MI) |
| OMIM | Online Mendelian Inheritance in Man |
| PSI-MI | Proteomics Standards Initiative — Molecular Interactions |
| SO | Sequence Ontology |
| UBERON | Uber-anatomy ontology |
