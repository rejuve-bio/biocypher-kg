## BioAtomSpace with MORK

### This project uses MORK to load BioAtomSpace data into a MORK database, store it efficiently, and perform benchmarks to evaluate performance. It also enables using BioAtomSpace as a graph database.

#### Features

 - Load BioAtomSpace data into MORK.

 - Store and manage data efficiently.

 - Benchmark MORK performance with BioAtomSpace datasets.

 - Use BioAtomSpace as a graph database.

### Getting Started
#### Prerequisites

 - Docker & Docker Compose installed

 - Python 3

### how to run

### Clone the repository
```bash
git clone https://github.com/Abdu1964/biocypher-mork.git
cd biocypher-mork

# Create necessary folders
mkdir reports
mkdir benchmarks

# Build Docker containers
docker compose build

# Start containers in detached mode
docker compose up -d

# Fix permissions for the reports and data folders
sudo chown -R $USER:$USER reports data benchmarks
sudo chmod -R u+rw reports data benchmarks

#load data to mork
python3 load_metta_data.py
#do benchmarks
python3 benchmark.py
```
### option 2
#### use a prebuilt image

- mkdir reports
- mkdir benchmarks

```bash 
docker run -d \
  -p 8027:8027 \
  --name mork-biocypher \
  -v $(pwd)/data:/app/data \
  -v $(pwd)/reports:/app/reports \
  -v $(pwd)/benchmarks:/app/benchmarks \
  abdum1964/mork-biocypher:latest


# Fix permissions for the reports and data folders
sudo chown -R $USER:$USER reports data
sudo chmod -R u+rw reports data

# load data to mork
python3 load_metta_data.py
# do benchmarks
python3 benchmark.py
```
