.PHONY: help setup check-uv run run-interactive run-sample test clean distclean

# Default target
help:
	@echo "Available commands:"
	@echo "  make setup          - Install dependencies (and UV if needed)"
	@echo "  make run            - Run with interactive prompts for parameters"
	@echo "  make run-sample     - Run with sample configuration and data"
	@echo "  make test           - Run tests"
	@echo "  make clean          - Clean temporary files"
	@echo "  make distclean      - Full clean including virtual environment"
	@echo ""
	@echo "Usage examples:"
	@echo "  make run            - Interactive mode (recommended)"
	@echo "  make run-interactive - Same as 'make run'"
	@echo "  make run-sample                    - Run with sample data (default: metta writer)"
	@echo "  make run-sample WRITER_TYPE=prolog - Run with sample data using prolog writer"

# Check if UV is installed, install via pip if not
check-uv:
	@echo "🔍 Checking if UV is installed..."
	@which uv > /dev/null 2>&1 || { \
		echo "📥 UV not found. Installing UV via pip..."; \
		pip install uv; \
		echo "✅ UV installed successfully!"; \
	}
	@echo "✅ UV is installed!"

# Install dependencies (with UV check)
setup: check-uv
	@echo "📦 Installing dependencies..."
	uv sync
	@echo "✅ Dependencies installed successfully!"

# Interactive run with prompts
run: run-interactive

# Interactive run with step-by-step prompts
run-interactive: check-uv
	@echo "🚀 Starting interactive knowledge graph creation..."
	@echo ""
	@read -p "📁 Enter output directory [./output]: " OUTPUT_DIR; \
	OUTPUT_DIR=$${OUTPUT_DIR:-./output}; \
	echo "Using output directory: $$OUTPUT_DIR"; \
	echo ""; \
	read -p "⚙️  Enter adapters config path [./config/adapters_config_sample.yaml]: " ADAPTERS_CONFIG; \
	ADAPTERS_CONFIG=$${ADAPTERS_CONFIG:-./config/adapters_config_sample.yaml}; \
	echo "Using adapters config: $$ADAPTERS_CONFIG"; \
	echo ""; \
	read -p "🧬 Enter dbSNP RSIDs path [./aux_files/sample_dbsnp_rsids.pkl]: " DBSNP_RSIDS; \
	DBSNP_RSIDS=$${DBSNP_RSIDS:-./aux_files/sample_dbsnp_rsids.pkl}; \
	echo "Using dbSNP RSIDs: $$DBSNP_RSIDS"; \
	echo ""; \
	read -p "📍 Enter dbSNP positions path [./aux_files/sample_dbsnp_pos.pkl]: " DBSNP_POS; \
	DBSNP_POS=$${DBSNP_POS:-./aux_files/sample_dbsnp_pos.pkl}; \
	echo "Using dbSNP positions: $$DBSNP_POS"; \
	echo ""; \
	read -p "📝 Enter writer type (metta/prolog/neo4j) [metta]: " WRITER_TYPE; \
	WRITER_TYPE=$${WRITER_TYPE:-metta}; \
	echo "Using writer type: $$WRITER_TYPE"; \
	echo ""; \
	read -p "📋 Write properties? (yes/no) [no]: " WRITE_PROPERTIES; \
	WRITE_PROPERTIES=$${WRITE_PROPERTIES:-no}; \
	if [ "$$WRITE_PROPERTIES" = "yes" ]; then \
		WRITE_PROPERTIES_FLAG=""; \
		echo "Properties will be written"; \
	else \
		WRITE_PROPERTIES_FLAG="--no-write-properties"; \
		echo "Properties will NOT be written"; \
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
	uv run python create_knowledge_graph.py \
		--output-dir "$$OUTPUT_DIR" \
		--adapters-config "$$ADAPTERS_CONFIG" \
		--dbsnp-rsids "$$DBSNP_RSIDS" \
		--dbsnp-pos "$$DBSNP_POS" \
		--writer-type "$$WRITER_TYPE" \
		$$WRITE_PROPERTIES_FLAG \
		$$ADD_PROVENANCE_FLAG; \
	echo "✅ Knowledge graph creation completed! Check $$OUTPUT_DIR for results."

# Direct run with parameters (non-interactive)
run-direct: check-uv
	@if [ -z "$(OUTPUT_DIR)" ] || [ -z "$(ADAPTERS_CONFIG)" ] || [ -z "$(DBSNP_RSIDS)" ] || [ -z "$(DBSNP_POS)" ]; then \
		echo "❌ Error: Missing required parameters"; \
		echo "Usage: make run-direct OUTPUT_DIR=... ADAPTERS_CONFIG=... DBSNP_RSIDS=... DBSNP_POS=... [WRITER_TYPE=...] [WRITE_PROPERTIES=...] [ADD_PROVENANCE=...]"; \
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
	uv run python create_knowledge_graph.py \
		--output-dir $(OUTPUT_DIR) \
		--adapters-config $(ADAPTERS_CONFIG) \
		--dbsnp-rsids $(DBSNP_RSIDS) \
		--dbsnp-pos $(DBSNP_POS) \
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
	uv run python create_knowledge_graph.py \
		--output-dir ./output \
		--adapters-config ./config/adapters_config_sample.yaml \
		--dbsnp-rsids ./aux_files/sample_dbsnp_rsids.pkl \
		--dbsnp-pos ./aux_files/sample_dbsnp_pos.pkl \
		--writer-type $(if $(WRITER_TYPE),$(WRITER_TYPE),metta) \
		$$WRITE_PROPERTIES_FLAG \
		$$ADD_PROVENANCE_FLAG
	@echo "✅ Sample run completed! Check the ./output directory for results."

# Run tests
test: check-uv
	uv run pytest -v

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