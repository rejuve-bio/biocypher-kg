# 1. Setup Environment

We configured the MORK service to use a persistent volume, ensuring data access across container restarts.

*   **Docker Configuration**: Modified `docker-compose.yml` to mount the local `output/` directory (containing BioCypher exports) to `/app/data` inside the MORK container.
*   **Networking**: Exposed host port `8027` mapped to container port `8027` to allow external HTTP API access.

## 2. Loading Strategy (The "Convert & Load" Workflow)

To solve the performance bottleneck of parsing `.metta` files on every restart, we implemented a two-step loading process:

**(A) Conversion (One-time)**
We created a script (`scripts/convert_topaths.py`) that compiles text-based `.metta` files into optimized binary `.paths` indices.
*   **Why**: Bypasses the expensive text parser on subsequent loads.
*   **How**: It checks file timestamps and only re-compiles files that have changed since the last run (incremental build).

**(B) Persistence (Every restart)**
We created a loader script (`scripts/load_paths.py`) that instructs MORK to memory-map these binary files.
*   **How**: It iterates through the `.paths` files and calls the MORK API to load them into the `annotation` namespace.
*   **Result**: The entire graph becomes available for querying almost instantly (microseconds per file) because it is mapped directly from disk to memory.

## 3. Execution of Queries

Once loaded, we query the graph using the MORK HTTP API, wrapped in a Python client.

*   **Mechanism**: The `mork.download(pattern, template)` method sends a request to the `/export/` endpoint.
*   **Scope**: Queries are executed against the `annotation` namespace where all binary paths are merged.

**Example Query Pattern:**
To find a specific transcript entity, we use the following Python code:

```python
# 1. Define the Pattern to match
pattern = "(transcript ENST00000353224)"

# 2. Define the Template (what to return)
template = "$x" # Return the matching atom itself

# 3. Execute the Query
result = scope.download(pattern, template)
```

## 4. How to Execute (The Workflow)

To run the full end-to-end process, execute these commands in order:

### A. Convert MeTTa to Binary
Pre-process the BioCypher exports into optimized indices.
```bash
python3 scripts/convert_topaths.py
```

### B. Load into MORK
Map the binary files into the server's memory.
```bash
python3 scripts/load_paths.py
```

### C. Run a Query
Test the connection and verify queries using the internal REPL tool. This allows you to test patterns interactively without writing new scripts for each test.
```bash
python3 scripts/mork_repl.py
```

