.PHONY: help setup check-uv run run-interactive run-sample run-direct test clean distclean

# Default target
help:
	@echo "Available commands:"
	@echo "  make setup          - Install dependencies (and UV if needed)"
	@echo "  make run            - Run with interactive prompts for parameters"
	@echo "  make run-sample     - Run with sample configuration and data"
	@echo "  make run-direct     - Run with explicit parameters (non-interactive)"
	@echo "  make test           - Run tests"
	@echo "  make clean          - Clean temporary files"
	@echo "  make distclean      - Full clean including virtual environment"
	@echo ""
	@echo "Usage examples:"
	@echo "  make run            - Interactive mode (recommended)"
	@echo "  make run-interactive - Same as 'make run'"
	@echo "  make run-sample                         - Run with sample data (default: metta writer)"
	@echo "  make run-sample WRITER_TYPE=prolog       - Run with sample data using prolog writer"

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
	read -p "📝 Enter writer type (metta/prolog/neo4j) [metta]: " WRITER_TYPE; \
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
		$$ADD_PROVENANCE_FLAG && \
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
	export PATH="$$HOME/.local/bin:$$PATH"; \
	uv run python create_knowledge_graph.py \
		--output-dir $(OUTPUT_DIR) \
		--adapters-config $(ADAPTERS_CONFIG) \
		--schema-config $(SCHEMA_CONFIG) \
		$(if $(DBSNP_CACHE_ROOT),--dbsnp-cache-root $(DBSNP_CACHE_ROOT),) \
		$(if $(DBSNP_VARIANT),--dbsnp-variant $(DBSNP_VARIANT),) \
		$(if $(WRITER_TYPE),--writer-type $(WRITER_TYPE),--writer-type metta) \
		$$WRITE_PROPERTIES_FLAG \
		$$ADD_PROVENANCE_FLAG

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
	export PATH="$$HOME/.local/bin:$$PATH"; \
	uv run python create_knowledge_graph.py \
		--output-dir ./output \
		--adapters-config ./config/hsa/hsa_adapters_config_sample.yaml \
		--dbsnp-cache-root ./aux_files/hsa/sample_dbsnp \
		--schema-config ./config/hsa/hsa_schema_config.yaml \
		--writer-type $(if $(WRITER_TYPE),$(WRITER_TYPE),metta) \
		$$WRITE_PROPERTIES_FLAG \
		$$ADD_PROVENANCE_FLAG
	@echo "✅ Sample run completed! Check the ./output directory for results."
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
