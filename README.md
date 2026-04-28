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
               ADAPTERS_CONFIG=./config.yaml \
               DBSNP_RSIDS=./rsids.txt \
               DBSNP_POS=./pos.txt \
               WRITER_TYPE=metta \
               WRITE_PROPERTIES=no \
               ADD_PROVENANCE=no \
               INCLUDE_TAXON_ID=no
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
🧬 Include taxon_id in output? (yes/no) [yes]: 
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
│   └── import.sh
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
│   ├── neo4j_loader.py
│   └── ...
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

### Neo4j Loader
To load the generated knowledge graph into a Neo4j database, use the `neo4j_loader.py` script:

```bash
python scripts/neo4j_loader.py --output-dir <path_to_output_directory>
```

#### Neo4j Loader Options
- `--output-dir`: **Required**. Path to the directory containing the generated Cypher query files.
- `--uri`: Optional. Neo4j database URI (default: `bolt://localhost:7687`)
- `--username`: Optional. Neo4j username (default: `neo4j`)

When you run the script, you'll be prompted to enter your Neo4j database password securely.

**Notes:**
- Ensure your Neo4j database is running before executing the loader.
- The script will automatically find and process all Cypher query files (node and edge files) in the specified output directory.
- It supports processing multiple directories containing Cypher files.
- The loader creates constraints and loads data in a single session.
- Logging is provided to help you track the loading process.

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
