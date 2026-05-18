# BioCypher KG

A project for creating [BioCypher-driven](https://github.com/biocypher/biocypher) knowledge graphs with multiple output formats.
## Prerequisites

- Python 3.9+  
- [UV](https://github.com/astral-sh/uv) package manager  

## Quick Start (Option 1)

### 1. Clone and Setup
```bash
git clone https://github.com/rejuve-bio/biocypher-kg.git
cd biocypher-kg
make setup
```

### 2. Run the Application

#### Option 1: Interactive Mode (Recommended for new users)
```bash
make run
```
This will guide you through all parameters step by step with sensible defaults.

#### Option 2: Quick Sample Run
```bash
make run-sample WRITER_TYPE=<metta,neo4j,prolog> [INCLUDE_TAXON_ID=no]
```

#### Option 3: Direct Run with Parameters
```bash
make run-direct OUTPUT_DIR=./output \
               ADAPTERS_CONFIG=./config/hsa/hsa_adapters_config.yaml \
               SCHEMA_CONFIG=./config/hsa/hsa_schema_config.yaml \
               WRITER_TYPE=metta \
               WRITE_PROPERTIES=no \
               ADD_PROVENANCE=no \
               INCLUDE_TAXON_ID=no

# Override the base input directory (e.g. on a different machine):
make run-direct OUTPUT_DIR=./output \
               ADAPTERS_CONFIG=./config/hsa/hsa_adapters_config.yaml \
               SCHEMA_CONFIG=./config/hsa/hsa_schema_config.yaml \
               INPUT_DIR=/custom/path/to/hsa
```
### Interactive Mode Example
When you run `make run`, you'll see:
```
🚀 Starting interactive knowledge graph creation...

📁 Enter output directory [./output]: 
⚙️  Enter adapters config path [./config/hsa/hsa_adapters_config_sample.yaml]: 
📊 Enter schema config path [./config/hsa/hsa_schema_config.yaml]: 
🗂️  Enter dbSNP cache root [./aux_files/hsa/sample_dbsnp]: 
🧬 Enter dbSNP variant (common/full/leave blank for sample) []: 
📝 Enter writer type (metta/prolog/neo4j/parquet/networkx/KGX) [metta]: 
🔌 Enter adapters to include [all]: 
📋 Write properties? (yes/no) [yes]: 
🔗 Add provenance? (yes/no) [no]: 
🧬 Include taxon_id in output? (yes/no) [yes]: 
🚦 Skip pre-flight path validation? (yes/no) [no]: 
```

### Available Make Commands
```bash
make help            # Show all commands
make setup           # Install UV and dependencies
make run             # Interactive mode (recommended)
make run-interactive # Same as make run
make run-direct      # Direct mode with parameters
make run-sample      # Run with sample data
make check-paths     # Validate file paths in an adapters config (no adapters run)
make download        # Download data sources (interactive)
make download-direct # Download data sources with explicit parameters
make test            # Run tests
make clean           # Clean temporary files
make distclean       # Full clean
```

### Pre-flight File Path Validation

Before any adapter runs, the pipeline checks that every file and directory path declared in the adapters config actually exists on disk. If any are missing, it prints a grouped report and exits immediately — no partial output, no wasted compute.

Example output when paths are missing:
```
ERROR: Pre-flight check failed — 2 adapter(s) have missing file paths:

  [gencode_gene]
    filepath: /mnt/hdd_1/biocypher-kg/input/hsa/gencode/gencode.v49.annotation.gtf.gz

  [bgee_gene_expressed_in_anatomical_entity]
    filepath: /mnt/hdd_1/biocypher-kg/input/hsa/bgee/Homo_sapiens_expr_simple_all_conditions.tsv.gz

Fix the paths above or run with --skip-preflight to bypass this check.
```

#### Check paths without running the pipeline

To validate an adapters config in isolation — without a schema, output directory, or any adapters executing:

```bash
# Via make (recommended)
make check-paths ADAPTERS_CONFIG=./config/hsa/hsa_adapters_config.yaml

# Override the base input directory
make check-paths ADAPTERS_CONFIG=./config/hsa/hsa_adapters_config.yaml \
                 INPUT_DIR=/custom/path/to/hsa

# Check only specific adapters
make check-paths ADAPTERS_CONFIG=./config/hsa/hsa_adapters_config.yaml \
                 INCLUDE_ADAPTERS="gencode_gene uniprotkb_sprot bgee_gene_expressed_in_anatomical_entity"

# Directly via the CLI
uv run python create_knowledge_graph.py \
    --adapters-config ./config/hsa/hsa_adapters_config.yaml \
    --check-only

# With input-dir override
uv run python create_knowledge_graph.py \
    --adapters-config ./config/hsa/hsa_adapters_config.yaml \
    --input-dir /custom/path/to/hsa \
    --check-only
```

Exits with code `0` if all paths exist, `1` if any are missing.

#### Skip the automatic check

If you know some files will be provided at runtime and want to bypass the check:

```bash
# Make targets
make run-direct ... SKIP_PREFLIGHT=yes
make run-sample     SKIP_PREFLIGHT=yes

# Directly via the CLI
uv run python create_knowledge_graph.py ... --skip-preflight
```

#### Configuring the input data directory

Full adapter configs (e.g. `config/hsa/hsa_adapters_config.yaml`) declare an `input_dir:` field at the top that serves as the base directory for all input data paths:

```yaml
input_dir: /mnt/hdd_1/biocypher-kg/input/hsa

gencode_gene:
  adapter:
    args:
      filepath: gencode/gencode.v49.annotation.gtf.gz   # resolved as input_dir/gencode/...
```

To use a different data location without editing the YAML, pass `--input-dir` (CLI) or `INPUT_DIR=` (make):

```bash
# Makefile
make run-direct OUTPUT_DIR=./output \
               ADAPTERS_CONFIG=./config/hsa/hsa_adapters_config.yaml \
               SCHEMA_CONFIG=./config/hsa/hsa_schema_config.yaml \
               INPUT_DIR=/my/data/hsa

# CLI
uv run python create_knowledge_graph.py \
    --output-dir ./output \
    --adapters-config ./config/hsa/hsa_adapters_config.yaml \
    --schema-config ./config/hsa/hsa_schema_config.yaml \
    --input-dir /my/data/hsa
```

Relative paths starting with `./` or `../` (e.g. `./aux_files/`, `./samples/`) are always treated as repository-relative and are never affected by `input_dir`.


## BioCypher Knowledge Graph CLI Tool (Option 2)

A user-friendly command line interface for generating knowledge graphs using BioCypher, with support for both Human and Drosophila melanogaster (Fly) data.

### Features

- 🧬 Human and 🪰 Fly organism support  
- ⚡ Default configurations for quick start  
- 🛠️ Custom configuration options  
- 📊 Interactive menu system with rich visual interface  
- 🔍 Multiple output formats (Neo4j, MeTTa, Prolog)  
- 📈 Progress tracking and logging  


### Setup

```bash
# 1. Clone the repository:
git clone https://github.com/rejuve-bio/biocypher-kg.git
cd biocypher-kg

# 2. Install dependencies using UV
uv sync

# 3. Create required directories and run the CLI
mkdir -p output_human output_fly
uv run python biocypher_cli/cli.py

# 📂 Project Structure:
# biocypher-kg/
# ├── biocypher_cli/            # CLI source code
# │   └── cli.py
# ├── config/                   # Configuration files or (Custom Config files)
# │   ├── adapters_config.yaml/adapters_config_sample.yaml
# │   ├── dmel_adapters_config.yaml/dmel_adapters_config_sample.yaml
# │   └── biocypher_config.yaml
# ├── aux_files/                # Auxiliary data files (or Custom config files)
# │   ├── gene_mapping.pkl/abc_tissues_to_ontology_map.pkl
# │   └── sample_dbsnp_rsids.pkl
# ├── output_human/             # Default human output
# ├── output_fly/               # Default fly output
# └── pyproject.toml            # Dependencies
```

## 🛠 Usage

### Structure
The project template is structured as follows:
```
.
.
│ # Project setup
│
├── LICENSE
├── README.md
├── pyproject.toml
│
│ # Docker setup
│
├── Dockerfile
├── docker
│   ├── biocypher_entrypoint_patch.sh
│   ├── create_table.sh
│   ├── import.sh
│   ├── neo4j.env                  # Neo4j config (image, ports, paths, memory)
│   └── docker-compose.neo4j.yml   # Parameterized Neo4j compose file
├── docker-compose.yml
├── docker-variables.env
│
│ # Project pipeline
|── biocypher_metta
│   ├── adapters
│   ├── metta_writer.py
│   ├── prolog_writer.py
│   └── neo4j_csv_writer.py
│
├── create_knowledge_graph.py
│ 
│ # Scripts
├── scripts
│   ├── neo4j_loader.py            # Direct loader — no versioning (make neo4j-load-direct)
│   ├── metta_space_import.py
│   └── ...
├── kg-service
│   ├── neo4j_loader.py            # Versioned/incremental loader (make neo4j-load)
│   └── version_manager.py
│
├── config
│   ├── adapters_config_sample.yaml
│   ├── biocypher_config.yaml
│   ├── biocypher_docker_config.yaml
│   ├── download.yaml
│   └── schema_config.yaml
│
│ # Downloading data
├── biocypher_dataset_downloader/
    ├── __init__.py
    ├── dmel_download_data.py
    ├── hsa_download_data.py
    ├── download_manager.py
    ├── protocols/
        ├── __init__.py
        ├── base.py
        └── http.py
```

The main components of the BioCypher pipeline are the
`create_knowledge_graph.py`, the configuration in the `config` directory, and
the adapter module in the `biocypher_metta` directory. The input adapters are used for preprocessing biomedical
databases and converting them into BioCypher nodes and edges. 

### Writers
The project supports multiple output formats for the knowledge graph:

1. **MeTTa Writer (`metta_writer.py`)**: Generates knowledge graph data in the MeTTa format.
2. **Prolog Writer (`prolog_writer.py`)**: Generates knowledge graph data in the Prolog format.
3. **Neo4j CSV Writer (`neo4j_csv_writer.py`)**: Generates CSV files containing nodes and edges of the knowledge graph, along with Cypher queries to load the data into a Neo4j database.

### Neo4j Deployment & Loading

Configure everything in `docker/neo4j.env` (image, ports, auth, data paths, memory), then use the Makefile targets:

```bash
# Start Neo4j Docker container
make neo4j-up

# Load ALL data directly — no version checking (use for first run or fresh container)
make neo4j-load-direct

# Load incrementally — only reloads datasets whose files have changed
make neo4j-load

# Other lifecycle commands
make neo4j-status
make neo4j-logs
make neo4j-down

# Override the env file
make neo4j-up NEO4J_ENV_FILE=docker/my-custom.env
```

To run the loaders directly:

```bash
# Direct load (no versioning) — using an env file
python scripts/neo4j_loader.py --env-file docker/neo4j.env

# Versioned/incremental load — using an env file
python kg-service/neo4j_loader.py --env-file docker/neo4j.env

# Versioned load — explicit args
python kg-service/neo4j_loader.py \
  --output-dir <path_to_neo4j_output> \
  --archive-dir <path_to_archive_dir> \
  --uri bolt://localhost:7887 \
  --username neo4j \
  --password <password>
```

#### Loader options
| Flag | Env var | Description |
|---|---|---|
| `--env-file` | — | Load all settings from a `neo4j.env` file |
| `--output-dir` | `NEO4J_OUTPUT_DIR` | Directory with generated CSV + Cypher files |
| `--archive-dir` | `NEO4J_ARCHIVE_DIR` | Archive directory for version management |
| `--uri` | `NEO4J_URI` | Neo4j bolt URI |
| `--username` | `NEO4J_USERNAME` | Neo4j username (default: `neo4j`) |
| `--password` | `NEO4J_PASSWORD` | Neo4j password |
| `--import-batch-size` | `NEO4J_IMPORT_BATCH_SIZE` | APOC batch size (default: `50000`) |
| `--import-dir` | — | Absolute path prefix for `file:///` URLs (non-Docker use only) |

**Notes:**
- The loader detects changed files via hash comparison and only reloads what changed.
- Edges use `CREATE` (not `MERGE`) — the loader surgically deletes changed edges before reloading, so the existence check is unnecessary and very slow on large files.

## ⬇ Downloading data
The `biocypher_dataset_downloader` directory contains code for downloading data from various sources.
Data source URLs and metadata are configured in the species-specific config files under `config/` (e.g. `config/hsa/hsa_data_source_config.yaml`).

### Interactive (recommended)

```bash
make download
```

You will be prompted for the output directory, config file path, and an optional source name (leave blank to download everything).

### Direct mode

```bash
# Download all sources for human
make download-direct OUTPUT_DIR=./input

# Download a single source
make download-direct OUTPUT_DIR=./input SOURCE=uniprot

# Download all sources for Drosophila
make download-direct OUTPUT_DIR=./input CONFIG_FILE=./config/dmel/dmel_data_source_config.yaml
```

### Without Make

```bash
# Download all sources
python -m biocypher_dataset_downloader.download_data --output-dir <output_directory>

# Download a specific source
python -m biocypher_dataset_downloader.download_data --output-dir <output_directory> --source <source_name>
```

## 🧬 dbSNP Cache

The pipeline requires a pre-built dbSNP cache for human (`hsa`) runs. Sample runs use the bundled cache at `aux_files/hsa/sample_dbsnp` automatically.

### Bizon server (pre-built cache available)

If you have access to the Bizon server, the cache is already available — no build step needed:
```
dbSNP cache root: /mnt/hdd_1/biocypher-kg/input/hsa/dbsnp/rsids_map/
dbSNP variant:    common
```

Pass these when prompted by `make run`, or set them directly:
```bash
uv run python create_knowledge_graph.py \
  --dbsnp-cache-root /mnt/hdd_1/biocypher-kg/input/hsa/dbsnp/rsids_map/ \
  --dbsnp-variant common \
  ...
```

### Building the cache from scratch

Use `scripts/update_dbsnp.py` to generate the cache locally:
```bash
# Common variants only (~1–2 GB, recommended)
python scripts/update_dbsnp.py --cache-dir <root>/common --common-only

# All variants (~35–50 GB)
python scripts/update_dbsnp.py --cache-dir <root>/full
```

Then either pass `--dbsnp-cache-root <root>` on the command line, or set `dbsnp_cache_root` in `config/species_config.yaml`:
```yaml
hsa:
  full:                          # dataset type (sample vs full run)
    dbsnp_cache_root: /path/to/dbsnp/cache
    dbsnp_variant: common        # SNP variant subset: "common" (~1-2 GB) or "full" (~35-50 GB)
```
