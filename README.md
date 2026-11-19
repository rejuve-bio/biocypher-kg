# BioCypher KG

Builds biological knowledge graphs with [BioCypher](https://github.com/biocypher/biocypher). Adapters
read public datasets (GENCODE, UniProt, Reactome, GO, FlyBase, ENCODE and 40+ more), map them to a
shared schema, and write the graph as MeTTa, Neo4j CSV, Prolog, Parquet, KGX or NetworkX. The output
can be loaded into Neo4j or a MORK server and queried there. kg-service, a FastAPI app in this repo,
serves graph metadata and version history and runs the Web Console, where you can build customized
graphs by picking the species, sources and size
([Deployment Guide § Web Console](docs/operations/deployment.md#web-console)).

## Supported species

| Species | Code | Status |
|---|---|---|
| *Homo sapiens* (human) | `hsa` | Production-ready |
| *Drosophila melanogaster* (fruit fly) | `dmel` | Production-ready |
| *Mus musculus* (mouse) | `mmu` | In development — includes baseline types\* |
| *Caenorhabditis elegans* | `cel` | In development — includes baseline types\* |
| *Rattus norvegicus* (rat) | `rno` | In development — includes baseline types\* |

\* **Baseline types:** gene, transcript types (transcript, exon, non_coding_rna), protein, pathway,
reaction, small_molecule, GO aspects (biological_process, molecular_function, cellular_component), and
the edges between them that the source data provide. Mouse, worm and rat also have promoters (EPD), gene
expression (Bgee), protein interactions and coexpression (STRING, Reactome, COXPRESdb), TF–target links
(TFLink), and orthology and disease associations (Alliance of Genome Resources, Disease Ontology); mouse
and worm also have anatomy ontologies, and mouse has developmental stages and cell types. Variants,
enhancers and most other regulatory sources are configured only for human and fly. Each species'
exact coverage is in `config/<species>/<species>_adapters_config.yaml`.

## Ways to build a graph

| | Use it when | Start with |
|---|---|---|
| **`make` targets** | You work from a checkout, locally or on a server | [Quick start](#quick-start) |
| **Web Console** | You want a browser UI for download → build → load, with no repo or env file | [Web Console](#web-console) |
| **Interactive CLI** | You want a menu for the default human or fly sample builds | `uv run python biocypher_cli/cli.py` ([biocypher_cli/README.md](biocypher_cli/README.md)) |

All three run the same pipeline, `create_knowledge_graph.py`.

## Prerequisites

- Python 3.10–3.12
- [uv](https://github.com/astral-sh/uv) (`make setup` installs it if missing)
- `libpq-dev` (provides `pg_config`, needed to build `psycopg2`)
- Docker with Compose v2, only for Neo4j, MORK and the Web Console

## Quick start

```bash
git clone https://github.com/rejuve-bio/biocypher-kg.git
cd biocypher-kg
make setup
```

**Sample build** (human, bundled sample data, no downloads needed):

```bash
make run-sample                          # MeTTa output in ./output
make run-sample WRITER_TYPE=neo4j        # or prolog, parquet, KGX, networkx
```

**Interactive build:** `make run` asks for each setting (output directory, configs, writer, dbSNP
cache, adapters, workers) and shows its default.

**Full build** from downloaded data (see [Downloading data](#downloading-data)):

```bash
make run-direct OUTPUT_DIR=./output \
               ADAPTERS_CONFIG=./config/hsa/hsa_adapters_config.yaml \
               SCHEMA_CONFIG=./config/hsa/hsa_schema_config.yaml \
               WRITER_TYPE=metta \
               INPUT_DIR=/path/to/input/hsa \
               INCLUDE_ADAPTERS="gencode_gene uniprot*"    # optional: exact keys or prefix*
```

Or, without make, by species and dataset as declared in
[`config/species_config.yaml`](config/species_config.yaml):

```bash
uv run python create_knowledge_graph.py --species dmel --dataset full --output-dir ./output/dmel
uv run python create_knowledge_graph.py --species all  --dataset sample --output-dir ./output
```

Options worth knowing (all in the [Configuration Reference](docs/operations/configuration.md)):

- **Input data location:** the adapters config's `input_dir:`, overridden by `--input-dir` / `INPUT_DIR=`.
- **Pre-flight check:** every input path is checked before any adapter runs.
  `make check-paths ADAPTERS_CONFIG=...` runs only the check; `--skip-preflight` skips it.
- **Parallelism:** adapters run on `min(8, CPU count)` workers; set `--max-workers` / `MAX_WORKERS=`.
- **Smaller graphs:** `--limit N` or `--adapter-limits FILE` caps what each adapter writes.
- **Resuming:** a checkpoint lets an interrupted build continue with `--resume`; `--restart` starts over.
- **dbSNP (human full builds only):** needs a prebuilt dbSNP cache, passed with `--dbsnp-cache-root`
  and `--dbsnp-variant`. Sample builds use the bundled one. See
  [Deployment Guide § dbSNP cache](docs/operations/deployment.md#dbsnp-cache-human-only).

## Downloading data

Data source URLs and versions are declared per species in
`config/<species>/<species>_data_source_config.yaml`.

```bash
make download                                                    # interactive
make download-direct OUTPUT_DIR=./input/hsa                      # all human sources
make download-direct OUTPUT_DIR=./input/hsa SOURCE=uniprot       # one source
make download-direct OUTPUT_DIR=./input/dmel \
                     CONFIG_FILE=./config/dmel/dmel_data_source_config.yaml
```

Each download writes `download_manifest.json` and `versions.json` (version, sha256, HTTP metadata)
into the output directory. Sources published on an older assembly (e.g. dbSUPER, hg19) are lifted to
the KG's assembly by the downloader with UCSC `liftOver`. Options, liftover setup and the automatic
sample regeneration are described in
[Ingestion Pipeline § Stage 1](docs/knowledge-graph/ingestion-pipeline.md#stage-1-download).

## Output formats

| `--writer-type` | Output | Typical consumer |
|---|---|---|
| `metta` (default) | `*.metta` files plus `type_defs.metta` | MORK, Hyperon, DAS |
| `neo4j` | CSV files plus `nodes_*.cypher` / `edges_*.cypher` load scripts | Neo4j |
| `prolog` | `.pl` facts | Prolog engines |
| `parquet` | Apache Parquet tables | Analytics tools |
| `KGX` | `*_nodes.csv` / `*_edges.csv` with Biolink `category` / `predicate` columns, plus `.cypher` load scripts | Biolink tooling, Neo4j |
| `networkx` | Pickled NetworkX graph | Python analysis |

Each build also writes `graph_info.json` (counts and dataset versions). Details:
[docs/knowledge-graph/writers.md](docs/knowledge-graph/writers.md).

## Loading into Neo4j and MORK

```bash
cp docker/neo4j.env.example docker/neo4j.env    # set NEO4J_OUTPUT_DIR, NEO4J_AUTH, ports
make neo4j-up                                   # start the container
make neo4j-load-direct                          # load everything (first run)
make neo4j-load                                 # later runs: reload only changed files
```

Running MORK and kg-service, several species, memory settings and ports are covered in the
**[Deployment Guide](docs/operations/deployment.md)**.

## Web Console

A browser UI for the whole flow, one species at a time: pick sources and download what's missing,
build (full or a smaller sample), load into that species' own Neo4j and MORK, and explore the schema.

```bash
./docker/up.sh                        # prints the address, e.g. http://localhost:20087/console/
./docker/up.sh --host <server-ip>     # on a server: listens on the network and asks for a password
```

Everything else (databases, data and build folders, Neo4j memory) is set in the console's **Settings**
page. Running without a checkout, settings, security and loading are described in
[Deployment Guide § Web Console](docs/operations/deployment.md#web-console).

## Dataset versioning & provenance

Each source's version is declared in its data source config (`url_regex`, `static`, or the default
`http_head`), recorded at download in `download_manifest.json`, and carried into the build's
`graph_info.json`. To check for changed sources (this runs weekly in CI):

```bash
uv run python -m biocypher_dataset_downloader.versioning.cli --species all --versions-root <dir>
```

Full specification: [docs/knowledge-graph/dataset-versioning.md](docs/knowledge-graph/dataset-versioning.md).

## Make commands

```bash
make help              # Show all commands and their variables
make setup             # Install uv and dependencies
make run               # Interactive build (same as make run-interactive)
make run-sample        # Build from bundled human sample data
make run-direct        # Build with explicit parameters
make check-paths       # Validate an adapters config's input paths (no adapters run)
make download          # Download data sources (interactive)
make download-direct   # Download data sources with explicit parameters
make test              # Run the tests
make clean             # Remove temporary files and output
make distclean         # Also remove the virtual environment
make neo4j-up          # Start Neo4j (also neo4j-reset, -down, -logs, -status, -load, -load-direct)
```

## Documentation

| Topic | Document |
|---|---|
| Index of all docs, by audience | [docs/index.md](docs/index.md) |
| Every config file, CLI flag and make variable | [docs/operations/configuration.md](docs/operations/configuration.md) |
| Neo4j, MORK, kg-service and Web Console | [docs/operations/deployment.md](docs/operations/deployment.md) |
| Download → adapt → write, liftover, provenance | [docs/knowledge-graph/ingestion-pipeline.md](docs/knowledge-graph/ingestion-pipeline.md) |
| Node and edge types | [docs/knowledge-graph/data-model.md](docs/knowledge-graph/data-model.md) |
| Data sources and their adapters | [docs/knowledge-graph/adapter-catalog.md](docs/knowledge-graph/adapter-catalog.md) |
| Repository layout | [docs/development/project-structure.md](docs/development/project-structure.md) |
| Common failures | [docs/operations/troubleshooting.md](docs/operations/troubleshooting.md) |

## Contributing

To add an adapter, update the schema or run the tests locally, see [CONTRIBUTING.md](CONTRIBUTING.md)
and [docs/development/local-development.md](docs/development/local-development.md).

## License

The code is MIT-licensed; see [LICENSE](LICENSE). The data sources keep their own licenses.
