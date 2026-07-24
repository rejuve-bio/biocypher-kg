# BioCypher Dataset Downloader & Reactome Exporter

This directory contains tools for downloading biomedical data sources and exporting full Reactome reaction dataset files for all species.

## Reactome All-Species Reaction Exporter

The full `reactome_reaction_exporter_All_species.txt` dataset contains ~732,000+ reaction entries across all species (~72 MB). Because of its size, it is excluded from git version control via `.gitignore`.

### Sample Dataset
A 1,000-row sample dataset is provided in the repository for development, unit testing, and sample runs:
```
samples/reactome/reactome_reaction_exporter_All_species_sample.txt
```

### Generating the Full All-Species File

To build the full `reactome_reaction_exporter_All_species.txt` file on demand, query the Reactome Neo4j graph database using the `export-reactome` CLI command.

#### Step 1: Start the Reactome Neo4j GraphDB Container
```bash
docker run -d --name reactome-neo4j -p 8687:7687 public.ecr.aws/reactome/graphdb:latest
```

#### Step 2: Run the Exporter
```bash
python biocypher_dataset_downloader/download_data.py export-reactome \
    --output-dir ./data \
    --neo4j-uri "bolt://localhost:8687" \
    --neo4j-user "neo4j" \
    --neo4j-password "neo4j"
```

The exported file will be saved to `./data/reactome_reaction_exporter_All_species.txt`.
