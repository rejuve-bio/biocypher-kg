# Monitoring

This document describes what monitoring is available for the BioCypher-KG system.

---

## Dataset staleness detection (automated)

The most important recurring monitoring task is checking whether the integrated data sources have been updated upstream.

**Mechanism:** `.github/workflows/check-dataset-versions.yml` runs every **Monday at 06:00 UTC** and on manual `workflow_dispatch`. It:

1. Runs `uv run python -m biocypher_dataset_downloader.versioning.cli --species <species> --versions-root <dir>`
2. Compares current HTTP headers / URL content against an optional baseline in `versioning/baselines/<species>/versions.json` (if no baseline has been committed yet, the workflow is a no-op pass — seed it by copying a download run's `versions.json` to that path)
3. Reports: `CHANGED` (ETag/Last-Modified differs), `drift` (version string changed), or `unknown` (static/pinned sources)
4. Creates a GitHub issue if staleness is detected
5. Saves `version_report.json` as a workflow artifact

**Manual trigger:**
- Go to GitHub Actions → "Check Dataset Versions" → Run workflow
- Select species: `all`, `hsa`, or `dmel`

**Check staleness locally:**
```bash
uv run python -m biocypher_dataset_downloader.versioning.cli \
    --species hsa \
    --versions-root versioning/baselines
# Exits 0 if no changes, non-zero if any source changed
```

---

## Code quality (SonarQube)

**Mechanism:** `.github/workflows/ci-quality-analysis.yml` triggers on every push to `main` / `staging` and on all PRs.

**Project key:** `Biocypher_KG`

**What it scans:**
- Code smells, bugs, security hotspots
- Duplicate code
- Test coverage

**Exclusions:** `node_modules`, `venv`, `__pycache__`, `build`, `dist`, `target`, `.git`, `coverage` directories

**Quality gate:** Checked with a 5-minute timeout after analysis. The PR is blocked if the gate fails.

**Access:** > **Unknown** — SonarQube dashboard URL not documented in the repository. Requires maintainer clarification.

---

## kg-service cache refresh

**Mechanism:** `main.py` schedules a background job via `APScheduler`:
```python
scheduler.add_job(refresh_graph_info, 'interval', hours=72, id='graph_info_refresh')
```

**What it does:** Calls `graph_info_cache.refresh()` which regenerates `graph_info.json` from Neo4j.

**Monitoring it:**
```bash
# Check cache age
curl http://localhost:8000/api/graph-info/status
# {"age_minutes": 120, "next_refresh_in_minutes": 3120, ...}

# Force a refresh
curl "http://localhost:8000/api/graph-info?force_refresh=true"
```

**If the refresh fails:** The scheduler logs to stderr:
```
❌ Auto-refresh failed: <error>
```
There is no alerting configured for scheduler failures — monitor container logs: `docker logs <kg-service-container>`.

---

## Neo4j health

```bash
# Browser UI
open http://localhost:7674

# API health check
curl http://localhost:7674/db/data/  # Neo4j 5.x: use /db/neo4j/cluster/available

# Via kg-service
curl http://localhost:8000/health
# {"status": "healthy", "neo4j": "connected"}
```

**Memory monitoring:** Watch Neo4j heap via container stats:
```bash
docker stats neo4j_bio_atomspace
```

---

## MORK health

```bash
# Health endpoint
curl http://localhost:8027/status/-

# Container health
docker ps --filter name=mork-biocypher --format "{{.Status}}"
# Should show: Up X hours (healthy)
```

---

## CI workflow status

Five GitHub Actions workflows run on schedule or on push:

| Workflow | Trigger | What to watch |
|---|---|---|
| `test-adapters.yml` | Push / PR | Matrix of adapter + writer tests |
| `test-schema.yml` | Push / PR | Schema validation (smoke on PR, full on main) |
| `check-dataset-versions.yml` | Weekly Monday 06:00 UTC | Creates issue if data sources are stale |
| `ci-quality-analysis.yml` | Push to main/staging / PR | SonarQube quality gate |
| `dependabot-auto-merge.yml` | Dependabot PRs | Automatic dependency merges |

---

## Log files

The pipeline writes per-run logs to two locations:

1. **BioCypher framework logs** — `biocypher-log/biocypher-<timestamp>.log`
2. **Pipeline run log** — `<output_dir>/kg_pipeline.log` (added by `_add_file_logger()` in `create_knowledge_graph.py`)

```bash
# Most recent pipeline log
ls -lt biocypher-log/ | head -3

# Pipeline-specific log (in output dir)
tail -f ./output/kg_pipeline.log
```

---

> **Note:** The following details are not yet documented: SonarQube dashboard URL and access, alerting configuration for background scheduler failures, metrics export setup (Prometheus or equivalent), and production hardware requirements. Contributions welcome — see [CONTRIBUTING.md](../../CONTRIBUTING.md).
