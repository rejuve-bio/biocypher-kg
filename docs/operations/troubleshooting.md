# Troubleshooting Guide

This document covers common failure modes and their resolution. For processor-specific errors, see [`biocypher_metta/processors/README.md`](https://github.com/rejuve-bio/biocypher-kg/blob/main/biocypher_metta/processors/README.md). For service setup, and for code bugs that currently affect deployment, see the [Deployment Guide](deployment.md) and its [Known issues](deployment.md#known-issues).

---

## Pipeline failures

### Pre-flight check fails: missing file paths

**Symptom:**
```
ERROR: Pre-flight check failed — 2 adapter(s) have missing file paths:

  [gencode_gene]
    filepath: /mnt/hdd_1/biocypher-kg/input/hsa/gencode/gencode.v49.annotation.gtf.gz
```

**Cause:** The adapter config declares paths that don't exist on disk.

**Resolution:**
1. Download the missing files with `make download` or `make download-direct`
2. If the files are in a different location, pass `--input-dir /your/data/path`
3. To bypass the check while debugging: `--skip-preflight` (not recommended for production)

---

### Checkpoint resume fails silently

**Symptom:** Pipeline starts from scratch despite a checkpoint file existing.

**Cause:** `pipeline_id` mismatch — the output directory or adapters config path changed between runs. `CheckpointManager.load()` logs a warning and discards the checkpoint.

**Resolution:** Check the BioCypher log for `"Checkpoint pipeline_id mismatch"`. Either:
- Run with the same `--output-dir` and `--adapters-config` as the original run
- Or delete `<output_dir>/kg_checkpoint.json` manually and start fresh with `--restart`

---

### Checkpoint file corrupted

**Symptom:** `WARNING: Could not parse checkpoint file`. Pipeline starts from scratch.

**Cause:** The checkpoint write was interrupted mid-atomic-write (rare — the write uses `.tmp` → rename).

**Resolution:** Delete `<output_dir>/kg_checkpoint.json` and rerun from scratch.

---

### Species `rno` raises `FileNotFoundError`

**Symptom:**
```
FileNotFoundError: config/rno/rno_adapters_config.yaml
```

**Cause:** `rno` is declared in `config/species_config.yaml` but `config/rno/rno_adapters_config.yaml` and `config/rno/rno_schema_config.yaml` are missing.

**Resolution:** Use `hsa`, `dmel`, `mmu`, or `cel` until the `rno` configs are implemented. See the species support table in [system-overview.md](../architecture/system-overview.md).

---

### Disease Ontology adapter fails to download

**Symptom:** The Disease Ontology adapter fails while loading `doid.owl`.

**Cause:** `DiseaseOntologyAdapter` downloads from OBOLibrary, which needs no key but is occasionally unreachable. It retries via BioPortal only when `BIOPORTAL_API_KEY` is set.

**Resolution:**
1. Register for a free API key at [bioportal.bioontology.org/account](https://bioportal.bioontology.org/account)
2. Add `BIOPORTAL_API_KEY=<your_key>` to `.env` so the BioPortal fallback is used
3. Or exclude the disease ontology adapter from the run: `--include-adapters <adapters_without_disease_ontology>`

---

### Adapter import error

**Symptom:**
```
ModuleNotFoundError: No module named 'biocypher_metta.adapters.some_adapter'
```

**Cause:** The `module` field in the adapters config points to a non-existent module.

**Resolution:** Check the `adapter.module` value in the YAML config and verify the file exists at that path.

---

### Writer type not recognized

**Symptom:**
```
ValueError: Unknown writer type: foo
```

**Cause:** Invalid `--writer-type` argument.

**Resolution:** Use one of: `metta`, `prolog`, `neo4j`, `parquet`, `networkx`, `KGX` (case-insensitive).

---

## Neo4j failures

### kg-service cannot connect to Neo4j

**Symptom:**
```
GET /health → {"status": "unhealthy", "neo4j": "disconnected"}
```

**Cause:** Neo4j is not running or the `NEO4J_URI` / credentials in `kg-service/.env` are wrong.

**Resolution:**
1. Verify Neo4j is running: `make neo4j-status`
2. Check port: default Bolt port is `7887` (not Neo4j's default `7687`) — verify `NEO4J_URI=bolt://localhost:7887`
3. Verify credentials match `NEO4J_AUTH` in `docker/neo4j.env`. kg-service reads `NEO4J_USER` (not `NEO4J_USERNAME`) and needs `NEO4J_DATABASE=neo4j`; see [Configure `kg-service/.env`](deployment.md#2-configure-kg-serviceenv)

---

### Partial Neo4j load — data missing after reload

**Symptom:** After a `make neo4j-load`, some nodes/edges are absent.

**Cause:** `Neo4jLoader` uses surgical deletion — it deletes only changed datasets before reloading. If a delete succeeds but the subsequent reload fails (e.g., Neo4j OOM), the dataset is absent.

**Resolution:**
1. Run `make neo4j-load-direct` for a full reload (skips version check, reloads everything)
2. Increase Neo4j heap: set `NEO4J_HEAP_MAX=8G` or higher in `docker/neo4j.env`

---

### `APOC not found` during LOAD CSV

**Symptom:**
```
Neo.ClientError.Procedure.ProcedureNotFound: There is no procedure with the name `apoc.periodic.iterate`
```

**Cause:** APOC plugin not loaded in the Neo4j container.

**Resolution:** APOC is enabled by default in `docker/docker-compose.neo4j.yml`. If you're using a custom Neo4j container, add the APOC plugin. Verify with: `docker exec biocypher_neo4j-neo4j-1 ls /plugins/`.

---

## MORK failures

### kg-service cannot reach MORK

**Symptom:** API endpoints for MORK return 500 errors or connection refused.

**Cause:** Port mismatch — kg-service defaults to `MORK_URL=http://localhost:8432` but the MORK container exposes port `8027`.

**Resolution:** Set `MORK_URL=http://localhost:8027` in `kg-service/.env`.

> Even with the right port, kg-service currently reports MORK as offline: its MORK client imports a `mork` Python package that isn't installed. See [kg-service issues](deployment.md#kg-service-issues).

---

### MORK container exits at startup

**Symptom:** The `mork-biocypher` container restarts repeatedly; its logs show `ERROR: /app/persist not found` or `ERROR: /app/persist is not writable.`

**Cause:** The container runs as UID 1000 and must write to the persist directory (`biocypher-mork/mork_persist/`, or the repo-root `mork_persist/` with the root compose file). Docker creates a missing bind-mount directory as root.

**Resolution:** `sudo chown -R 1000:1000 <persist dir>` and restart. See [Run the server](deployment.md#1-run-the-server).

---

### MORK container fails health check

**Symptom:** `docker ps` shows `(unhealthy)` for the MORK container.

**Cause:** On start, the container imports the last snapshot and replays the WAL before the server answers `/status/-`. A large snapshot can take longer than the health check's 60-second start period.

**Resolution:** Wait for the restore to finish. Monitor with `docker logs mork-biocypher -f`. If it stays unhealthy after 5 minutes, check WAL integrity.

---

### MORK WAL corruption

**Symptom:** MORK fails to start, logs show WAL parse errors.

**Cause:** `wal.metta` in the persist directory (`biocypher-mork/mork_persist/` by default) was written partially due to an interrupted write.

**Resolution:**
1. Stop the container
2. Inspect `wal.metta` for truncated lines at the end
3. Remove the last incomplete line, or empty the file to fall back to `snapshot.paths`
4. Restart the container

---

## kg-service startup failure

### `ImportError: No module named 'backend.api.routes.meta'`

**Symptom:** kg-service crashes immediately on startup.

**Cause:** An older `kg-service/backend/api/main.py` imported `meta` and `entities` from `backend.api.routes` before those files existed.

**Resolution:** Update to a version that includes `kg-service/backend/api/routes/meta.py` and `entities.py`. `main.py` now imports them optionally. See the Deployment Guide's [Known issues](deployment.md#known-issues) for other kg-service problems.

---

### `GET /api/databases/mork/status` returns 404

**Symptom:** The MORK status endpoint isn't found.

**Cause:** The route is defined in `backend/api/routes/databases.py`, but `main.py` never registers that router.

**Resolution:** None yet; this is a known issue.

---

## Test failures

### Ontology adapter tests hang in CI

**Symptom:** `pytest` hangs or times out when running ontology adapters.

**Cause:** Ontology adapters fetch large OWL files from the internet (some are 100+ MB).

**Resolution:** Run in smoke mode (PRs do this automatically):
```bash
uv run pytest test/test.py --adapter-test-mode smoke
```

Heavy ontology adapters are listed in `SMOKE_SKIP_MODULE_PATTERNS` in `test/test.py`.

---

## Diagnostic commands

```bash
# Check which adapters were completed in the last checkpoint
cat <output_dir>/kg_checkpoint.json | python -m json.tool | grep -A5 "completed_adapters"

# Validate adapter config paths without running anything
make check-paths ADAPTERS_CONFIG=./config/hsa/hsa_adapters_config_sample.yaml

# Check Neo4j connectivity
curl http://localhost:7674/  # browser UI
# Or from Python:
uv run python -c "from neo4j import GraphDatabase; d=GraphDatabase.driver('bolt://localhost:7887', auth=('neo4j','password')); d.verify_connectivity(); print('OK')"

# Check MORK health
curl http://localhost:8027/status/-

# View recent BioCypher logs
ls -lt biocypher-log/*.log | head -5
cat biocypher-log/biocypher-$(date +%Y%m%d)*.log | tail -100

# Check kg-service health
curl http://localhost:8000/health
```
