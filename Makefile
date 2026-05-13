.PHONY: help setup check-uv run run-interactive run-sample run-direct check-paths \
        download download-direct \
        test clean distclean \
        neo4j-up neo4j-down neo4j-logs neo4j-status neo4j-load neo4j-load-direct

# Path to the Neo4j env file; override with: make neo4j-up NEO4J_ENV_FILE=my.env
NEO4J_ENV_FILE ?= docker/neo4j.env

# Default target
help:
	@echo "Available commands:"
	@echo "  make setup          - Install dependencies (and UV if needed)"
	@echo "  make run            - Run with interactive prompts for parameters"
	@echo "  make run-sample     - Run with sample configuration and data"
	@echo "  make run-direct     - Run with explicit parameters (non-interactive)"
	@echo "  make check-paths    - Validate file paths in an adapters config (no adapters run)"
	@echo "  make download       - Download data sources (interactive)"
	@echo "  make download-direct - Download data sources with explicit parameters"
	@echo "  make test           - Run tests"
	@echo "  make clean          - Clean temporary files"
	@echo "  make distclean      - Full clean including virtual environment"
	@echo ""
	@echo "Neo4j deployment (configure via $(NEO4J_ENV_FILE)):"
	@echo "  make neo4j-up           - Start Neo4j Docker container"
	@echo "  make neo4j-down         - Stop and remove Neo4j Docker container"
	@echo "  make neo4j-logs         - Stream Neo4j container logs"
	@echo "  make neo4j-status       - Show Neo4j container status"
	@echo "  make neo4j-load         - Load data with version tracking (incremental)"
	@echo "  make neo4j-load-direct  - Load ALL data directly, skipping version check"
	@echo ""
	@echo "Usage examples:"
	@echo "  make run            - Interactive mode (recommended)"
	@echo "  make run-interactive - Same as 'make run'"
	@echo "  make run-sample                    - Run with sample data (default: metta writer)"
	@echo "  make run-sample WRITER_TYPE=prolog - Run with sample data using prolog writer"
	@echo "  make check-paths ADAPTERS_CONFIG=./config/hsa/hsa_adapters_config.yaml"
	@echo "  make run-direct ... SKIP_PREFLIGHT=yes   - Skip pre-flight file path validation"
	@echo "  make run-sample     SKIP_PREFLIGHT=yes   - Same, for sample runs"
	@echo "  make neo4j-up NEO4J_ENV_FILE=docker/my-custom.env"
	@echo "  make run-sample INCLUDE_TAXON_ID=no   - Run without taxon_id in output (single-species KG)"
	@echo "  make download                          - Interactive download (recommended)"
	@echo "  make download-direct OUTPUT_DIR=./input SOURCE=uniprot  - Download a single source"
	@echo "  make download-direct OUTPUT_DIR=./input CONFIG_FILE=./config/dmel/dmel_data_source_config.yaml"

# Check if UV is installed, install via pip if not
check-uv:
	@echo "🔍 Checking if UV is installed..."
	@which uv > /dev/null 2>&1 || test -f "$$HOME/.local/bin/uv" || { \
		echo "📥 UV not found. Installing UV..."; \
		curl -LsSf https://astral.sh/uv/install.sh | sh; \
		echo "✅ UV installed successfully!"; \
	}
	@echo "✅ UV is installed!"

# Install dependencies (with UV check)
setup: check-uv
	@echo "📦 Installing dependencies..."
	@export PATH="$$HOME/.local/bin:$$PATH"; uv sync
	@echo "✅ Dependencies installed successfully!"

# Interactive run with prompts
run: run-interactive


# read -p "🧬 Enter dbSNP RSIDs path [./aux_files/hsa/sample_dbsnp_rsids.pkl]: " DBSNP_RSIDS; \
# DBSNP_RSIDS=$${DBSNP_RSIDS:-./aux_files/hsa/sample_dbsnp_rsids.pkl}; \
# echo "Using dbSNP RSIDs: $$DBSNP_RSIDS"; \
# echo ""; \
# read -p "📍 Enter dbSNP positions path [./aux_files/hsa/sample_dbsnp_pos.pkl]: " DBSNP_POS; \
# DBSNP_POS=$${DBSNP_POS:-./aux_files/hsa/sample_dbsnp_pos.pkl}; \
# echo "Using dbSNP positions: $$DBSNP_POS"; \

# Interactive run with step-by-step prompts
run-interactive: check-uv
	@echo "\n🚀 Starting interactive knowledge graph creation..."
	@echo "🚀 Press ENTER to chose the default value [in square brackets] for each option...\n" 
	@read -p "📁 Enter output directory [./output]: " OUTPUT_DIR; \
	OUTPUT_DIR=$${OUTPUT_DIR:-./output}; \
	echo "Using output directory: $$OUTPUT_DIR"; \
	echo ""; \
	read -p "⚙️  Enter adapters config path [./config/hsa/hsa_adapters_config_sample.yaml]: " ADAPTERS_CONFIG; \
	ADAPTERS_CONFIG=$${ADAPTERS_CONFIG:-./config/hsa/hsa_adapters_config_sample.yaml}; \
	echo "Using adapters config: $$ADAPTERS_CONFIG"; \
	echo ""; \
	read -p "📊 Enter schema config path [./config/hsa/hsa_schema_config.yaml]: " SCHEMA_CONFIG; \
	SCHEMA_CONFIG=$${SCHEMA_CONFIG:-./config/hsa/hsa_schema_config.yaml}; \
	echo "Using schema config: $$SCHEMA_CONFIG"; \
	echo ""; \
	read -p "🗂️  Enter dbSNP cache root [./aux_files/hsa/sample_dbsnp]: " DBSNP_CACHE_ROOT; \
	DBSNP_CACHE_ROOT=$${DBSNP_CACHE_ROOT:-./aux_files/hsa/sample_dbsnp}; \
	echo "Using dbSNP cache root: $$DBSNP_CACHE_ROOT"; \
	echo ""; \
	read -p "🧬 Enter dbSNP variant (common/full/leave blank for sample) []: " DBSNP_VARIANT; \
	if [ -n "$$DBSNP_VARIANT" ]; then \
		DBSNP_VARIANT_FLAG="--dbsnp-variant $$DBSNP_VARIANT"; \
		echo "Using dbSNP variant: $$DBSNP_VARIANT"; \
	else \
		DBSNP_VARIANT_FLAG=""; \
		echo "dbSNP variant: not set (sample mode)"; \
	fi; \
	echo ""; \
	read -p "📝 Enter writer type (metta/prolog/neo4j/parquet/networkx/KGX) [metta]: " WRITER_TYPE; \
	WRITER_TYPE=$${WRITER_TYPE:-metta}; \
	echo "Using writer type: $$WRITER_TYPE"; \
	echo ""; \
	read -p "🔌 Enter adapters to include [all]: " INCLUDE_ADAPTERS; \
	INCLUDE_ADAPTERS=$${INCLUDE_ADAPTERS:-all}; \
	if [ "$$INCLUDE_ADAPTERS" = "all" ]; then \
		INCLUDE_ADAPTERS_FLAG=""; \
	else \
		INCLUDE_ADAPTERS_FLAG=""; \
		for adapter in $$INCLUDE_ADAPTERS; do \
			INCLUDE_ADAPTERS_FLAG="$$INCLUDE_ADAPTERS_FLAG --include-adapters $$adapter"; \
		done; \
	fi; \
	echo "Including adapters: $$INCLUDE_ADAPTERS"; \
	echo ""; \
	read -p "📋 Write properties? (yes/no) [yes]: " WRITE_PROPERTIES; \
	WRITE_PROPERTIES=$${WRITE_PROPERTIES:-yes}; \
	if [ "$$WRITE_PROPERTIES" = "no" ]; then \
		WRITE_PROPERTIES_FLAG="--no-write-properties"; \
		echo "Properties will NOT be written"; \
	else \
		WRITE_PROPERTIES_FLAG=""; \
		echo "Properties will be written"; \
	fi; \
	echo ""; \
	read -p "🔗 Add provenance? (yes/no) [no]: " ADD_PROVENANCE; \
	ADD_PROVENANCE=$${ADD_PROVENANCE:-no}; \
	if [ "$$ADD_PROVENANCE" = "yes" ]; then \
		ADD_PROVENANCE_FLAG=""; \
		echo "Provenance will be added"; \
	else \
		ADD_PROVENANCE_FLAG="--no-add-provenance"; \
		echo "Provenance will NOT be added"; \
	fi; \
	echo ""; \
	read -p "🧬 Include taxon_id in output? (yes/no) [yes]: " INCLUDE_TAXON_ID; \
	INCLUDE_TAXON_ID=$${INCLUDE_TAXON_ID:-yes}; \
	if [ "$$INCLUDE_TAXON_ID" = "no" ]; then \
		INCLUDE_TAXON_ID_FLAG="--no-taxon-id"; \
		echo "taxon_id will NOT be written to output"; \
	else \
		INCLUDE_TAXON_ID_FLAG=""; \
		echo "taxon_id will be written to output"; \
	fi; \
	echo ""; \
	read -p "🚦 Skip pre-flight path validation? (yes/no) [no]: " SKIP_PREFLIGHT; \
	SKIP_PREFLIGHT=$${SKIP_PREFLIGHT:-no}; \
	if [ "$$SKIP_PREFLIGHT" = "yes" ]; then \
		SKIP_PREFLIGHT_FLAG="--skip-preflight"; \
		echo "Pre-flight validation will be skipped"; \
	else \
		SKIP_PREFLIGHT_FLAG=""; \
		echo "Pre-flight validation enabled"; \
	fi; \
	echo ""; \
	echo "🎯 Starting knowledge graph creation..."; \
	export PATH="$$HOME/.local/bin:$$PATH"; \
	uv run python create_knowledge_graph.py \
		--output-dir "$$OUTPUT_DIR" \
		--adapters-config "$$ADAPTERS_CONFIG" \
		--schema-config "$$SCHEMA_CONFIG" \
		--dbsnp-cache-root "$$DBSNP_CACHE_ROOT" \
		$$DBSNP_VARIANT_FLAG \
		--writer-type "$$WRITER_TYPE" \
		$$INCLUDE_ADAPTERS_FLAG \
		$$WRITE_PROPERTIES_FLAG \
		$$ADD_PROVENANCE_FLAG \
		$$INCLUDE_TAXON_ID_FLAG \
		$$SKIP_PREFLIGHT_FLAG && \
	echo "✅ Knowledge graph creation completed! Check $$OUTPUT_DIR for results."

run-direct: check-uv
	@if [ -z "$(OUTPUT_DIR)" ] || [ -z "$(ADAPTERS_CONFIG)" ] || [ -z "$(SCHEMA_CONFIG)" ]; then \
		echo "❌ Error: Missing required parameters"; \
		echo "Usage: make run-direct OUTPUT_DIR=... ADAPTERS_CONFIG=... SCHEMA_CONFIG=... [DBSNP_CACHE_ROOT=...] [DBSNP_VARIANT=common|full] [WRITER_TYPE=...] [WRITE_PROPERTIES=...] [ADD_PROVENANCE=...]"; \
		echo ""; \
		echo "Or use 'make run' for interactive mode"; \
		exit 1; \
	fi
	@if [ "$(WRITE_PROPERTIES)" = "true" ] || [ "$(WRITE_PROPERTIES)" = "yes" ]; then \
		WRITE_PROPERTIES_FLAG=""; \
	else \
		WRITE_PROPERTIES_FLAG="--no-write-properties"; \
	fi; \
	if [ "$(ADD_PROVENANCE)" = "true" ] || [ "$(ADD_PROVENANCE)" = "yes" ]; then \
		ADD_PROVENANCE_FLAG=""; \
	else \
		ADD_PROVENANCE_FLAG="--no-add-provenance"; \
	fi; \
	if [ "$(INCLUDE_TAXON_ID)" = "false" ] || [ "$(INCLUDE_TAXON_ID)" = "no" ]; then \
		INCLUDE_TAXON_ID_FLAG="--no-taxon-id"; \
	else \
		INCLUDE_TAXON_ID_FLAG=""; \
	fi; \
	export PATH="$$HOME/.local/bin:$$PATH"; \
	uv run python create_knowledge_graph.py \
		--output-dir $(OUTPUT_DIR) \
		--adapters-config $(ADAPTERS_CONFIG) \
		--schema-config $(SCHEMA_CONFIG) \
		$(if $(DBSNP_CACHE_ROOT),--dbsnp-cache-root $(DBSNP_CACHE_ROOT),) \
		$(if $(DBSNP_VARIANT),--dbsnp-variant $(DBSNP_VARIANT),) \
		$(if $(WRITER_TYPE),--writer-type $(WRITER_TYPE),--writer-type metta) \
		$$WRITE_PROPERTIES_FLAG \
		$$ADD_PROVENANCE_FLAG \
		$$INCLUDE_TAXON_ID_FLAG \
		$(if $(filter yes true,$(SKIP_PREFLIGHT)),--skip-preflight,)

# Run with sample configuration and data (with optional writer type)
run-sample: check-uv
	@echo "🚀 Running knowledge graph creation with sample data..."
	@echo "Writer type: $(if $(WRITER_TYPE),$(WRITER_TYPE),metta (default))"
	@if [ "$(WRITE_PROPERTIES)" = "true" ] || [ "$(WRITE_PROPERTIES)" = "yes" ]; then \
		WRITE_PROPERTIES_FLAG=""; \
		echo "Properties: enabled"; \
	else \
		WRITE_PROPERTIES_FLAG="--no-write-properties"; \
		echo "Properties: disabled"; \
	fi; \
	if [ "$(ADD_PROVENANCE)" = "true" ] || [ "$(ADD_PROVENANCE)" = "yes" ]; then \
		ADD_PROVENANCE_FLAG=""; \
		echo "Provenance: enabled"; \
	else \
		ADD_PROVENANCE_FLAG="--no-add-provenance"; \
		echo "Provenance: disabled"; \
	fi; \
	if [ "$(INCLUDE_TAXON_ID)" = "false" ] || [ "$(INCLUDE_TAXON_ID)" = "no" ]; then \
		INCLUDE_TAXON_ID_FLAG="--no-taxon-id"; \
		echo "taxon_id: disabled"; \
	else \
		INCLUDE_TAXON_ID_FLAG=""; \
		echo "taxon_id: enabled"; \
	fi; \
	export PATH="$$HOME/.local/bin:$$PATH"; \
	uv run python create_knowledge_graph.py \
		--output-dir ./output \
		--adapters-config ./config/hsa/hsa_adapters_config_sample.yaml \
		--dbsnp-cache-root ./aux_files/hsa/sample_dbsnp \
		--schema-config ./config/hsa/hsa_schema_config.yaml \
		--writer-type $(if $(WRITER_TYPE),$(WRITER_TYPE),metta) \
		$$WRITE_PROPERTIES_FLAG \
		$$ADD_PROVENANCE_FLAG \
		$$INCLUDE_TAXON_ID_FLAG \
		$(if $(filter yes true,$(SKIP_PREFLIGHT)),--skip-preflight,)
	@echo "✅ Sample run completed! Check the ./output directory for results."
# Validate file paths in an adapters config without running any adapters
check-paths: check-uv
	@if [ -z "$(ADAPTERS_CONFIG)" ]; then \
		echo "❌ Error: ADAPTERS_CONFIG is required"; \
		echo "Usage: make check-paths ADAPTERS_CONFIG=./config/hsa/hsa_adapters_config.yaml"; \
		echo "       make check-paths ADAPTERS_CONFIG=... INCLUDE_ADAPTERS='gencode_gene uniprotkb_sprot'"; \
		exit 1; \
	fi
	@export PATH="$$HOME/.local/bin:$$PATH"; \
	INCLUDE_ADAPTERS_FLAG=""; \
	if [ -n "$(INCLUDE_ADAPTERS)" ]; then \
		for adapter in $(INCLUDE_ADAPTERS); do \
			INCLUDE_ADAPTERS_FLAG="$$INCLUDE_ADAPTERS_FLAG --include-adapters $$adapter"; \
		done; \
	fi; \
	uv run python create_knowledge_graph.py \
		--adapters-config $(ADAPTERS_CONFIG) \
		$$INCLUDE_ADAPTERS_FLAG \
		--check-only

# ─── Neo4j deployment targets ────────────────────────────────────────────────

neo4j-up: ## Start Neo4j Docker container (reads docker/neo4j.env)
	docker compose --env-file $(NEO4J_ENV_FILE) -f docker/docker-compose.neo4j.yml up -d
	@echo "✅ Neo4j starting — check status with: make neo4j-status"

neo4j-down: ## Stop and remove Neo4j Docker container
	docker compose --env-file $(NEO4J_ENV_FILE) -f docker/docker-compose.neo4j.yml down
	@echo "✅ Neo4j stopped"

neo4j-logs: ## Stream Neo4j container logs
	docker compose --env-file $(NEO4J_ENV_FILE) -f docker/docker-compose.neo4j.yml logs -f neo4j

neo4j-status: ## Show Neo4j container status
	docker compose --env-file $(NEO4J_ENV_FILE) -f docker/docker-compose.neo4j.yml ps

neo4j-load: check-uv ## Load data with version tracking (incremental — only reloads changed datasets)
	@export PATH="$$HOME/.local/bin:$$PATH"; \
	uv run python kg-service/neo4j_loader.py \
		--env-file $(NEO4J_ENV_FILE)

neo4j-load-direct: check-uv ## Load ALL data directly, skipping version check
	@set -a; . $(NEO4J_ENV_FILE); set +a; \
	export PATH="$$HOME/.local/bin:$$PATH"; \
	uv run python scripts/neo4j_loader.py \
		--env-file $(NEO4J_ENV_FILE)

# ─── Data download ───────────────────────────────────────────────────────────

# Interactive download
download: check-uv
	@echo "\n📥 Starting interactive data download..."
	@echo "Press ENTER to use the default value [in square brackets]...\n"
	@read -p "📁 Enter output directory [./input]: " OUTPUT_DIR; \
	OUTPUT_DIR=$${OUTPUT_DIR:-./input}; \
	echo "Using output directory: $$OUTPUT_DIR"; \
	echo ""; \
	read -p "⚙️  Enter config file path [./config/hsa/hsa_data_source_config.yaml]: " CONFIG_FILE; \
	CONFIG_FILE=$${CONFIG_FILE:-./config/hsa/hsa_data_source_config.yaml}; \
	echo "Using config file: $$CONFIG_FILE"; \
	echo ""; \
	read -p "🔬 Enter source name to download (leave blank to download all): " SOURCE; \
	if [ -n "$$SOURCE" ]; then \
		SOURCE_FLAG="--source $$SOURCE"; \
		echo "Downloading source: $$SOURCE"; \
	else \
		SOURCE_FLAG=""; \
		echo "Downloading all sources"; \
	fi; \
	echo ""; \
	echo "📥 Starting download..."; \
	export PATH="$$HOME/.local/bin:$$PATH"; \
	uv run python biocypher_dataset_downloader/download_data.py \
		--output-dir "$$OUTPUT_DIR" \
		--config-file "$$CONFIG_FILE" \
		$$SOURCE_FLAG && \
	echo "✅ Download completed! Check $$OUTPUT_DIR for results."

# Direct download with explicit parameters
download-direct: check-uv
	@if [ -z "$(OUTPUT_DIR)" ]; then \
		echo "❌ Error: OUTPUT_DIR is required"; \
		echo "Usage: make download-direct OUTPUT_DIR=./input [CONFIG_FILE=./config/hsa/hsa_data_source_config.yaml] [SOURCE=<source_name>]"; \
		exit 1; \
	fi
	@export PATH="$$HOME/.local/bin:$$PATH"; \
	uv run python biocypher_dataset_downloader/download_data.py \
		--output-dir $(OUTPUT_DIR) \
		--config-file $(if $(CONFIG_FILE),$(CONFIG_FILE),./config/hsa/hsa_data_source_config.yaml) \
		$(if $(SOURCE),--source $(SOURCE),)

# ─── Tests ───────────────────────────────────────────────────────────────────

# Run tests
test: check-uv
	@export PATH="$$HOME/.local/bin:$$PATH"; uv run pytest -v

# Clean temporary files and output
clean:
	rm -rf __pycache__
	rm -rf .pytest_cache
	rm -rf *.egg-info
	rm -rf ./output  # Also clean the output directory from sample runs
	find . -name "*.pyc" -delete
	find . -name "__pycache__" -type d -exec rm -rf {} + 2>/dev/null || true
	@echo "✅ Cleaned temporary files!"

# Full clean including virtual environment
distclean: clean
	rm -rf .venv
	rm -rf .uvcache
	@echo "✅ Full clean completed!"
