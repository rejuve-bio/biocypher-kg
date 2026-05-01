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
make run-sample WRITER_TYPE=<metta,neo4j,prolog>
```

#### Option 3: Direct Run with Parameters
```bash
make run-direct OUTPUT_DIR=./output \
               ADAPTERS_CONFIG=./config.yaml \
               DBSNP_RSIDS=./rsids.txt \
               DBSNP_POS=./pos.txt \
               WRITER_TYPE=metta \
               WRITE_PROPERTIES=no \
               ADD_PROVENANCE=no
```
### Interactive Mode Example
When you run `make run`, you'll see:
```
🚀 Starting interactive knowledge graph creation...

📁 Enter output directory [./output]: 
⚙️  Enter adapters config path [./config/adapters_config_sample.yaml]: 
🧬 Enter dbSNP RSIDs path [./aux_files/sample_dbsnp_rsids.pkl]: 
📍 Enter dbSNP positions path [./aux_files/sample_dbsnp_pos.pkl]: 
📝 Enter writer type (metta/prolog/neo4j) [metta]: 
📋 Write properties? (yes/no) [no]: 
🔗 Add provenance? (yes/no) [no]: 
```

### Available Make Commands
```bash
make help           # Show all commands
make setup          # Install UV and dependencies
make run            # Interactive mode (recommended)
make run-interactive # Same as make run
make run-direct     # Direct mode with parameters
make run-sample     # Run with sample data
make test           # Run tests
make clean          # Clean temporary files
make distclean      # Full clean
```


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
│   ├── metta_space_import.py
│   └── ...
├── kg-service
│   ├── neo4j_loader.py            # Load CSV/Cypher output into Neo4j
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

# Load data (reads connection settings from docker/neo4j.env)
make neo4j-load

# Other lifecycle commands
make neo4j-status
make neo4j-logs
make neo4j-down

# Override the env file
make neo4j-up NEO4J_ENV_FILE=docker/my-custom.env
```

To run the loader directly:

```bash
# Using an env file (recommended)
python kg-service/neo4j_loader.py --env-file docker/neo4j.env

# Or pass each argument explicitly
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
The `downloader` directory contains code for downloading data from various sources.
The `download.yaml` file contains the configuration for the data sources.

To download the data, run the `download_data.py` script with the following command:
```{bash}
python downloader/download_data.py --output_dir <output_directory>
```

To download data from a specific source, run the script with the following command:
```{bash}
python downloader/download_data.py --output_dir <output_directory> --source <source_name>
```
