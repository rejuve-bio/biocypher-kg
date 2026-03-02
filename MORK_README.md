# MORK CLI & BioAtomSpace Integration

This directory contains the integration of **MORK**, a high-performance reasoning engine designed for large-scale biological knowledge graphs.

Our implementation uses a **CLI-based "Convert-then-Query" workflow, optimized for speed, zero-footprint on disk, and compatibility.

---

## Core Architecture & Advantages

Unlike traditional database servers, our MORK integration runs as a series of ephemeral, high-speed CLI operations:

1.  **Binary Conversion**: MeTTa files are compiled into **Arena Compact Trees (.act)**. This is a binary memory-snapshot of the graph that skips text parsing during queries.
    -   **Advantage**: Near-instant loading regardless of file size (GBs vs KBs). It turns a "minutes-long" parse into a "milliseconds-long" map.
2.  **RAM-Disk Execution**: All queries are executed using `/dev/shm` (Linux Shared Memory). Data is mapped directly from RAM for maximum throughput.
    -   **Advantage**: High-performance throughput. RAM access is faster than standard SSDs.
3.  **Zero-Footprint Querying**: Uses ephemeral containers (`--rm`) and memory-mapping.
    -   **Advantage**: MORK's `mmap` requirement is natively supported by the Linux kernel on the `/dev/shm` filesystem, ensuring maximum stability and speed without leaving any temporary files on your physical machine.
4.  **Memory Efficiency (mmap)**: Instead of loading the entire dataset into RAM, MORK uses memory mapping to "view" the file.
    -   **Advantage**: Allows you to query datasets that are much larger than your physical RAM. The kernel only loads the specific "pages" of the file that your query actually touches.

---

## Quick Step-by-Step Guide

Follow these steps to start querying your BioCypher data with MORK.

### Step 1: Prepare your Data
First, ensure your BioCypher exports are in a folder (default is `./output`). 
If you have human data, you might use `./output_human`.

### Step 2: Choose your Path (ACT vs MeTTa)

#### Option A: The "High Performance" Path (Recommended for large files)
Use this to convert your text files into fast binary files. **You only need to do this once** unless your data changes.
1.  Open your terminal in the project root.
2.  Run the batch converter:
    ```bash
    MORK_DATA_DIR=./output_human ./scripts/convert_toact.sh
    ```
    *(Note: This creates `.act` files next to your `.metta` files.)*

#### Option B: The "Quick Test" Path
If you just want to test a small file immediately without converting it, you can skip Step 2 and go straight to Step 3.

### Step 3: Run the Query Tool (REPL)
Start the interactive query tool by pointing it to your data folder:
```bash
MORK_DATA_DIR=./output_human python3 scripts/mork_repl.py
```

### Step 4: Ask a Question (Example)
Once the tool starts, it will ask you for 3 things. Here is an example of what to type:

1.  **File to load**: `reactome/nodes.act` (or `nodes.metta` if you skipped conversion)
2.  **Pattern**: `(Pathway $p)` (This looks for all atoms that are Pathways)
3.  **Return**: `$p` (This tells MORK to show you the name of the pathway)

---

## Summary of Tools

- **`scripts/convert_toact.sh`**: The "Compiler". Use this to turn slow text into fast binary.
- **`scripts/mork_repl.py`**: The "Searcher". Your interactive window into the data.
- **`/dev/shm`**: Where the magic happens. Everything runs in your computer's RAM for maximum speed and zero disk clutter.

---
