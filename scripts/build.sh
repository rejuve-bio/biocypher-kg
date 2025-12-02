#!/bin/bash
set -e  # Exit immediately on error

echo "=== BioCypher Build Stage Starting ==="

# --- Setup Directories ---
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR/.."

OUTPUT_SUBFOLDER="${OUTPUT_SUBFOLDER:-output_human}"
OUTPUT_DIR="${OUTPUT_DIR:-/usr/app/data/$OUTPUT_SUBFOLDER}"

# If running locally (no /usr/app/data mount), fallback
if [ ! -d "/usr/app/data" ]; then
  echo " /usr/app/data not found. Using local directory instead."
  OUTPUT_DIR="./$OUTPUT_SUBFOLDER"
fi

# Create all output directories safely
mkdir -p "$OUTPUT_DIR/neo4j" "$OUTPUT_DIR/metta" "$OUTPUT_DIR/prolog"
mkdir -p biocypher-log
chmod -R 777 biocypher-log "$OUTPUT_DIR"

echo "Output directory: $OUTPUT_DIR"
echo "Subdirectories created:"
ls -l "$OUTPUT_DIR"

# --- Install Dependencies ---
echo "Installing dependencies..."
pip install poetry
poetry install

echo "Poetry setup complete."

# --- Generate Neo4j Format ---
echo "Generating Neo4j format..."
poetry run python create_knowledge_graph.py \
  --output-dir "$OUTPUT_DIR/neo4j" \
  --adapters-config config/adapters_config_sample.yaml \
  --dbsnp-rsids aux_files/sample_dbsnp_rsids.pkl \
  --dbsnp-pos aux_files/sample_dbsnp_pos.pkl \
  --writer-type neo4j \
  --no-add-provenance

# --- Generate MeTTa Format ---
echo "Generating MeTTa format..."
poetry run python create_knowledge_graph.py \
  --output-dir "$OUTPUT_DIR/metta" \
  --adapters-config config/adapters_config_sample.yaml \
  --dbsnp-rsids aux_files/sample_dbsnp_rsids.pkl \
  --dbsnp-pos aux_files/sample_dbsnp_pos.pkl \
  --writer-type metta \
  --no-add-provenance

# --- Generate Prolog Format ---
echo "Generating Prolog format..."
poetry run python create_knowledge_graph.py \
  --output-dir "$OUTPUT_DIR/prolog" \
  --adapters-config config/adapters_config_sample.yaml \
  --dbsnp-rsids aux_files/sample_dbsnp_rsids.pkl \
  --dbsnp-pos aux_files/sample_dbsnp_pos.pkl \
  --writer-type prolog \
  --no-add-provenance

# --- Final Debug Info ---
echo "Listing output files:"
ls -lR "$OUTPUT_DIR"

echo " BioCypher build completed successfully!"
