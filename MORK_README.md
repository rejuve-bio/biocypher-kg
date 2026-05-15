# MORK BioAtomSpace Integration

This directory provides a high-performance reasoning layer for BioCypher data, optimized for unified spacename querying using MORK.

## Core Advatanges

1.  **Direct Memory Mapping (mmap)**: Data is indexed in ArenaCompactTree (.act). 
    - Queries map the file directly from disk into memory, loading only the specific fragments needed. 
    - **Zero-Footprint**: RAM is used only on-demand, this allows for massive graphs to be searched on standard hardware.
2.  **One Spacename Architecture**: All adapter outputs (Reactome, Uniprot, Gencode, etc.) are compiled into a **single, unified binary** (`annotation.act`).
    - You can query across the entire knowledge graph in a single pattern without switching files.
3.  **Dependency-Order Build**: The build process automatically handles the required loading order (Types -> Nodes -> Edges) to ensure the engine always knows the graph structure.

---

## Step-by-Step Guide

### 1. Unified Strategy (ACT)
*Best for production & sharing. Single file, memory-mapped.*
- **Build**: `bash scripts/build_act.sh output_human` (Produces `annotation.act`)
- **Query**: `MORK_DATA_DIR=./output_human python3 scripts/mork_repl.py`

### 2. Querying the Knowledge Graph (REPL)
The `mork_repl.py` script provides an interactive interface to query your data. It supports all three formats and is designed for **benchmarking**.

```bash
# ACT Strategy (Persistent/memory-mapped)
python scripts/mork_repl.py --format act

# PATHS Strategy (Benchmarking .paths Load)
python scripts/mork_repl.py --format paths

# meTTa Strategy (Benchmarking .metta Load)
python scripts/mork_repl.py --format metta
```

> [!NOTE]
> For `paths` and `metta` formats, the REPL performs a **fresh native load** for every query. This allows you to accurately benchmark the loading time vs. the query execution time for each format.

---

## Technical Details

- **`scripts/build_act.sh`**: Cascades all `.metta` fragments into a dependency-ordered stream before compilation into a single `.act` file.
- **`scripts/build_paths.sh`**: Iteratively converts each `.metta` file into a binary `.paths` format using the MORK CLI. Incremental build ensures only new/changed files are converted.
- **`scripts/mork_repl.py`**: A unified interactive loop for all three formats. For `paths` and `metta` modes, it performs a **pure native load** (text parsing or binary deserialization) for every query to enable accurate benchmarking of MORK's internal loading performance.
