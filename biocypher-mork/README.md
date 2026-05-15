## BioAtomSpace with MORK

### This project uses MORK to load BioAtomSpace data into a MORK graph server and store it with crash-safe WAL persistence.

#### Features
- Load BioCypher-generated `.metta` files into MORK.
- Crash-safe persistence via snapshots + Write-Ahead Log (WAL).

---

### Setup & Configuration

#### 1. Prerequisites
- Docker & Docker Compose installed
- Python 3

#### 2. Initial Setup
```bash
git clone https://github.com/Abdu1964/biocypher-mork.git
cd biocypher-mork
mkdir -p reports mork_persist
```

#### 3. Configure Data Directory
Edit `.env` in the `biocypher-mork/` folder to point `MORK_DATA_DIR` to your BioCypher output folder. By default, it looks for `../output_human`.

```bash
# Example .env setting
MORK_DATA_DIR=../output_human
```

---

### Usage Workflow

#### 1. Start the MORK Server
Run from the `biocypher-mork/` folder:
```bash
docker compose up -d --build
docker compose ps  
```

#### 2. Load Data
Run from the **project root** (`biocypher-kg/`), one level above `biocypher-mork/`:
```bash
python3 scripts/mork_loader.py
```
**When prompted:**
- **Dataset Path**: Enter the path to your `.metta` directory (e.g., `/path/to/output_human`).
- **Clear existing data**: Always answer **`y`** on fresh loads to wipe the in-memory graph and truncate the WAL, preventing data duplication.

#### 3. Reloading Data
To reload after regenerating your data, simply re-run the loader with **Clear = y**. This automatically:
1. Clears the in-memory graph on the server.
2. Truncates `wal.metta` and `snapshot.paths` to 0 bytes.
3. Loads the fresh data.

---

### Persistence Explained

| File | Purpose |
|---|---|
| `mork_persist/snapshot.paths` | Full compressed snapshot of the graph (created every ~5 min or on shutdown). |
| `mork_persist/wal.metta` | Write-Ahead Log — records every write since the last snapshot. |

On startup, the server automatically restores state by replaying the snapshot and then the WAL.

---

### Monitoring & Querying

#### Check Server Status
```bash
# Container health
docker compose ps

# Live logs
docker compose logs -f mork-biocypher

curl http://localhost:8027/status/-
```

#### Querying Data
To see your loaded data, use the `/explore` endpoint or the Python client.

```bash
curl "http://localhost:8027/explore/%28default%20%24x%29//"
```
