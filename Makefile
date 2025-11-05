.PHONY: help setup check-uv run run-sample test clean distclean

# Default target
help:
	@echo "Available commands:"
	@echo "  make setup          - Install dependencies (and UV if needed)"
	@echo "  make run            - Run knowledge graph creation (set required env variables)"
	@echo "  make run-sample     - Run with sample configuration and data"
	@echo "  make test           - Run tests"
	@echo "  make clean          - Clean temporary files"
	@echo "  make distclean      - Full clean including virtual environment"
	@echo ""
	@echo "Usage examples:"
	@echo "  make run OUTPUT_DIR=./output ADAPTERS_CONFIG=./config.yaml DBSNP_RSIDS=./rsids.txt DBSNP_POS=./pos.txt"
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

# Run the knowledge graph creation with custom parameters
run: check-uv
	@if [ -z "$(OUTPUT_DIR)" ] || [ -z "$(ADAPTERS_CONFIG)" ] || [ -z "$(DBSNP_RSIDS)" ] || [ -z "$(DBSNP_POS)" ]; then \
		echo "❌ Error: Missing required parameters"; \
		echo "Usage: make run OUTPUT_DIR=... ADAPTERS_CONFIG=... DBSNP_RSIDS=... DBSNP_POS=... [WRITER_TYPE=...] [WRITE_PROPERTIES=...] [ADD_PROVENANCE=...]"; \
		echo ""; \
		echo "Alternatively, use 'make run-sample' to run with sample data"; \
		exit 1; \
	fi
	uv run python create_knowledge_graph.py \
		--output-dir $(OUTPUT_DIR) \
		--adapters-config $(ADAPTERS_CONFIG) \
		--dbsnp-rsids $(DBSNP_RSIDS) \
		--dbsnp-pos $(DBSNP_POS) \
		$(if $(WRITER_TYPE),--writer-type $(WRITER_TYPE)) \
		$(if $(WRITE_PROPERTIES),--write-properties $(WRITE_PROPERTIES)) \
		$(if $(ADD_PROVENANCE),--add-provenance $(ADD_PROVENANCE))

# Run with sample configuration and data (with optional writer type)
run-sample: check-uv
	@echo "🚀 Running knowledge graph creation with sample data..."
	@echo "Writer type: $(if $(WRITER_TYPE),$(WRITER_TYPE),metta (default))"
	uv run python create_knowledge_graph.py \
		--output-dir ./output \
		--adapters-config ./config/adapters_config_sample.yaml \
		--dbsnp-rsids ./aux_files/sample_dbsnp_rsids.pkl \
		--dbsnp-pos ./aux_files/sample_dbsnp_pos.pkl \
		--writer-type $(if $(WRITER_TYPE),$(WRITER_TYPE),metta)
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