# BioCypher KG

A project for creating [BioCypher-driven](https://github.com/biocypher/biocypher) knowledge graphs with multiple output formats.
##option 1
# BioCypher Knowledge Graph CLI Tool

A user-friendly command line interface for generating knowledge graphs using BioCypher, with support for both Human and Drosophila melanogaster (Fly) data.

## Features

- 🧬 Human and 🪰 Fly organism support  
- ⚡ Default configurations for quick start  
- 🛠️ Custom configuration options  
- 📊 Interactive menu system with rich visual interface  
- 🔍 Multiple output formats (Neo4j, MeTTa, Prolog)  
- 📈 Progress tracking and logging  

## Installation

### Prerequisites

- Python 3.9+  
- Poetry (for dependency management)  

### Setup

```bash
# 1. Clone the repository:
git clone https://github.com/rejuve-bio/biocypher-kg.git
cd biocypher-kg

# 2. Install dependencies using Poetry
poetry install

# 3. Create required directories and run the CLI
mkdir -p output_human output_fly
poetry run python biocypher_cli/cli.py

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

##Option 2
## ⚙️ Installation (local)

1. Clone this repository.
```{bash}
git clone https://github.com/rejuve-bio/biocypher-kg.git
```

2. Install the dependencies using [Poetry](https://python-poetry.org/). (Or feel
 free to use your own dependency management system. We provide a `pyproject.toml`
 to define dependencies.)
```{bash}
poetry install
```

3. You are ready to go!
```{bash}
poetry shell
python create_knowledge_graph.py \
    --output_dir <output_directory> \
    --adapters_config <path_to_adapters_config> \
    --dbsnp_rsids <path_to_dbsnp_rsids_map> \
    --dbsnp_pos <path_to_dbsnp_pos_map> \
    [--writer_type {metta,prolog,neo4j}] \
    [--write_properties {true,false}] \
    [--add_provenance {true,false}]
```

### Knowledge Graph Creation
The `create_knowledge_graph.py` script supports multiple configuration options:

**Arguments:**
- `--output_dir`: Directory to save generated knowledge graph files (required)
- `--adapters_config`: Path to YAML file with adapter configurations (required)
- `--dbsnp_rsids`: Path to pickle file with dbSNP RSID mappings (required)
- `--dbsnp_pos`: Path to pickle file with dbSNP position mappings (required)
- `--writer_type`: Choose output format (optional)
  - `metta`: MeTTa format (default)
  - `prolog`: Prolog format
  - `neo4j`: Neo4j CSV format
- `--write_properties`: Include node and edge properties (optional, default: true)
- `--add_provenance`: Add provenance information (optional, default: true)

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
├── downloader/
    ├── __init__.py
    ├── download_data.py
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
