# MORK BioAtomSpace Integration

This directory provides a high-performance reasoning layer for BioCypher data, optimized for unified spacename querying using MORK.

## Core Advatanges (Unified Strategy)

1.  **Direct Memory Mapping (mmap)**: Data is indexed in ArenaCompactTree (.act). 
    - Queries map the file directly from disk into memory, loading only the specific fragments needed. 
    - **Zero-Footprint**: RAM is used only on-demand, this allows for massive graphs to be searched on standard hardware.
2.  **One Spacename Architecture**: All adapter outputs (Reactome, Uniprot, Gencode, etc.) are compiled into a **single, unified binary** (`annotation.act`).
    - You can query across the entire knowledge graph in a single pattern without switching files.
3.  **Dependency-Order Build**: The build process automatically handles the required loading order (Types -> Nodes -> Edges) to ensure the engine always knows the graph structure.

---

## Step-by-Step Guide

### 1. Generate Unified Graph
After running BioCypher on your adapters, merge and compile the results:
```bash
bash scripts/build_act.sh output_human
```
*This produces `output_human/annotation.act`.*

### 2. Query the Knowledge Graph
Start the interactive REPL. It defaults to the `annotation` space.
```bash
MORK_DATA_DIR=./output_human python3 scripts/mork_repl.py
```

### 3. Example Query
In the REPL, you can now search the entire dataset:
- **Pattern**: `(Pathway $p)`
- **Return**: `$p`

---

## Technical Details

- **`scripts/build_act.sh`**: Cascades all `.metta` fragments into a dependency-ordered stream before compilation.
- **`scripts/mork_repl.py`**: A clean interactive loop that executes queries via ephemeral Docker containers, leveraging `/dev/shm` for temporary query logic and `mmap` for the data itself.
