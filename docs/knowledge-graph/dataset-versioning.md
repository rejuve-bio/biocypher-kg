# Dataset Versioning & Provenance

BioCypher-KG builds knowledge graphs from ~40 input datasets per species (GENCODE, dbSNP,
Reactome, UniProt, STRING, GTEx, …). This document explains how the version and provenance of
each dataset is captured, where it is stored, and how to declare it for a new source.

## Why this exists

Previously, dataset versions were hardcoded in individual adapters (`self.version = 'v49'`) and in
config file paths. They drifted out of sync — for example two GENCODE adapters reading the *same*
`gencode.v49` file reported `v49` and `v44` — and there was no single answer to "which version of
each source produced this knowledge graph". That makes a build hard to reproduce or cite.

Now there is **one source of truth**: the data-source config. A version flows along a single path:

```
config/<species>/<species>_data_source_config.yaml   (you declare how to find the version)
        │   downloader resolves + records
        ▼
<output_dir>/download_manifest.json   +   <output_dir>/versions.json   (per species)
        │   build reads it via --manifest
        ▼
<output_dir>/graph_info.json  → datasets[] (version, url, checksums, citation, license)
        │   Neo4j loader reads graph_info
        ▼
(:KGVersion).source_provenance_json   (upstream version + checksums per KG version)
```

The design is declarative and lightweight: because our source URLs usually already embed the
version (`gencode.v49`, `protein.links.v12.0`, `..._fb_2026_01.tsv`), a small regex over the URL
captures it — no per-source scraper code is needed.

## Declaring a source version

Each entry in a `*_data_source_config.yaml` may carry an optional `version:` block plus optional
provenance fields. Everything is backward-compatible — a source with no `version:` block defaults
to the `http_head` strategy.

### Strategies

| Strategy | When to use | Fields |
|---|---|---|
| `url_regex` | The version is embedded in the download URL/filename (most sources) | `pattern` (regex with **one capture group**), `vtype` (optional), `url_index` (optional — which URL to match when several are declared) |
| `static` | The source has a fixed version that isn't in the URL | `value` (the version string), `vtype` (optional) |
| `http_head` | **Default.** No parseable version (e.g. a `current/` symlink); track changes instead | none — uses the file's `ETag` / `Last-Modified` / `Content-Length` as a change signature |

`vtype` is a free-form classification (`semver`, `date`, `sequential`, `static`, `other`, …) and is
recorded for reference only.

### Extra provenance fields (all optional)

| Field | Meaning |
|---|---|
| `source_url` | Human-facing landing page for the source (used in `graph_info.json` and the paper) |
| `citation` | Citation string for the dataset |
| `license` | License/terms string |
| `checksum: false` | Skip sha256 for this source's files (use for multi-GB files like dbSNP VCFs) |

### Examples (taken from the live configs)

`url_regex` — version parsed from the URL:

```yaml
gencode:
  name: GENCODE v49
  version: {strategy: url_regex, pattern: 'gencode\.(v\d+)\.', vtype: sequential}
  source_url: https://www.gencodegenes.org/
  url: https://ftp.ebi.ac.uk/.../gencode.v49.annotation.gtf.gz
```

`static` — fixed version:

```yaml
hocomoco:
  name: HOCOMOCOv11
  version: {strategy: static, value: v11}
```

`url_regex` replacing per-adapter logic — the dmel FlyBase block reproduces what
`FlybasePrecomputedTable.extract_date_string()` did, declaratively, for every FlyBase file:

```yaml
flybase:
  name: Flybase 2026_01
  version: {strategy: url_regex, pattern: 'fb_(\d{4}_\d{2})', vtype: date}
```

Skipping checksums on a huge source:

```yaml
dbsnp:
  version: {strategy: url_regex, pattern: '_(b\d+)_', vtype: sequential}
  checksum: false   # multi-GB VCFs — HEAD metadata + version still recorded
```

A source with **no** `version:` block (e.g. a `current/` symlink) falls back to `http_head`
automatically — its version stays unknown but a change signature is recorded.

> **Multi-species note.** The same logical source can carry a different version per species
> (`alliance` is `8.3.0`, `bgee` uses a `current/` symlink, `ensembl`/`epd`/`flybase` differ). There
> is no global registry — each `<species>_data_source_config.yaml` declares its own blocks, and the
> manifest is written per species.

## The download manifest

Running the downloader writes two files into the download `--output-dir` (one pair **per species**):

- **`download_manifest.json`** — the authoritative per-run record. Shape:

  ```json
  {
    "manifest_version": 1,
    "config_file": "config/hsa/hsa_data_source_config.yaml",
    "generated_at": "2026-06-12T10:00:00",
    "sources": {
      "gencode": {
        "name": "GENCODE v49",
        "version": "v49",
        "date": null,
        "signature": null,
        "vtype": "sequential",
        "strategy": "url_regex",
        "resolve_error": null,
        "source_url": "https://www.gencodegenes.org/",
        "citation": null,
        "license": null,
        "declared_url": "https://ftp.ebi.ac.uk/.../gencode.v49.annotation.gtf.gz",
        "remote_metadata": { "<url>": {"last_modified": "...", "etag": "...", "content_length": "...", "checked_at": "..."} },
        "files": [
          {"rel_path": "gencode/gencode.v49.annotation.gtf", "sha256": "…", "size_bytes": 1481234567, "downloaded_at": "..."}
        ]
      }
    }
  }
  ```

- **`versions.json`** — an append-only history per source. A new row is appended only when the
  resolved `version`/`signature` changes, so repeated runs don't bloat it:

  ```json
  { "gencode": [ {"retrieved": "2026-06-12T10:00:00", "version": "v49", "signature": null, "date": null, "vtype": "sequential"} ] }
  ```

### Checksums

By default every downloaded file is sha256-hashed. Hashes are **carried forward** for files that
didn't change between runs (matched by relative path + size), so large datasets aren't re-hashed
each time. To skip hashing entirely:

- `--no-checksum` on the download command (whole run), or
- `checksum: false` on an individual source (e.g. dbSNP).

Files relocated by `move_to` (e.g. dmel files moved to `aux_files/dmel/`) are still recorded.

## Provenance at build time

`create_knowledge_graph.py` reads the manifest so the generated graph carries authoritative
versions instead of the hardcoded adapter attributes:

```bash
uv run python create_knowledge_graph.py \
  --adapters-config config/hsa/hsa_adapters_config.yaml \
  --schema-config   config/hsa/hsa_schema_config.yaml \
  --output-dir      output_hsa \
  --manifest        <download_dir>/download_manifest.json
```

If `--manifest` is omitted, the build looks for `download_manifest.json` beside the resolved
`input_dir`. Provenance is matched to adapters **by `source_id`** — derived from the adapter
entry's `outdir` first segment (`gencode/gene` → `gencode`), or an explicit `source_id:` field on
the adapter entry. When a source isn't in the manifest, the build falls back to the adapter's own
attributes, so nothing breaks.

The resolved provenance lands in `graph_info.json` under each `datasets[]` entry:

```json
{
  "name": "GENCODE",
  "version": "v49",
  "url": "https://www.gencodegenes.org/",
  "date": null,
  "citation": null,
  "license": null,
  "checksums": {"gencode/gencode.v49.annotation.gtf": "…"},
  "nodes": ["gene", "transcript"],
  "edges": ["transcribes_to"],
  "imported_on": "2026-06-12"
}
```

> Sample runs (`*_adapters_config_sample.yaml`) use bundled `./samples/...` data and have **no**
> manifest — they run normally and fall back to adapter attributes. Provenance is a full-run feature.

## Checking for stale or changed sources

`check-versions` resolves each source's current version and compares it to the last recorded one:

```bash
# All species, table output
uv run python -m biocypher_dataset_downloader.versioning.cli \
  --config-dir config --versions-root <dir-with-per-species-versions>

# One species, machine-readable
uv run python -m biocypher_dataset_downloader.versioning.cli \
  --species hsa --versions-root <…> --json

# One source
uv run python -m biocypher_dataset_downloader.versioning.cli --species hsa --source gencode --versions-root <…>
```

Options: `--species` (`hsa`/`dmel`/`cel`/`mmu`/`rno` or `all`, default `all`), `--source`,
`--config-dir` (default `config`), `--versions-root` (`<root>/<species>/versions.json`), `--json`.

Status meanings:

| Status | Meaning |
|---|---|
| `up-to-date` | Resolved token matches the recorded one |
| `CHANGED` | An `http_head` source's signature differs — a `current/` file was refreshed upstream |
| `drift` | A resolved version differs from the recorded one — the config's pinned URL changed since the last download |
| `unknown` | Pinned source with no recorded baseline; latest-release availability can't be determined here |
| `error` | Resolution failed (e.g. regex matched nothing) |

The command exits non-zero if any source is `CHANGED` or `drift`.

> **Known limit.** A regex over a *pinned* URL can never reveal that a newer upstream release
> exists — the URL still names the old version. Pinned sources are therefore reported `unknown`
> rather than a misleading `up-to-date`. True latest-release discovery (a landing-page scraper or a
> [bioversions](https://github.com/biopragmatics/bioversions) delegate) is a planned future add-on.

### Scheduled CI

`.github/workflows/check-dataset-versions.yml` runs `check-versions` weekly (and on manual
dispatch), uploads the JSON report as an artifact, and opens an issue when sources are
`CHANGED`/`drift`. It compares against committed baselines in `versioning/baselines/<species>/versions.json`.
Until those baselines are seeded (copy a real download run's `versions.json` there), the job is a
quiet no-op pass.

## Neo4j lineage

When the versioned Neo4j loader finalizes a build, each `KGVersion` node records the upstream
provenance it was built from, read from `graph_info.json`:

```
(:KGVersion {version, ..., source_provenance_json})
```

`source_provenance_json` maps each dataset to `{upstream_version, source_url, checksums}`. Example
query — find every KG version built from GENCODE v49:

```cypher
MATCH (v:KGVersion)
WHERE apoc.convert.fromJsonMap(v.source_provenance_json).GENCODE.upstream_version = "v49"
RETURN v.version, v.created_at
```

## Reproducing a published knowledge graph

For a given KG build you have a complete, citable chain:

1. `graph_info.json` `datasets[]` — the exact version, URL, and sha256 of every source used.
2. `download_manifest.json` — the per-file checksums and HTTP metadata captured at download time.
3. `(:KGVersion).source_provenance_json` — the same provenance attached to the loaded graph version.

To reproduce: re-download with the same config (the manifest's `version`/sha256 confirm you got the
same inputs), then rebuild with `--manifest`. Matching checksums guarantee identical inputs.

## See also

- [biocypher_dataset_downloader/versioning/README.md](https://github.com/rejuve-bio/biocypher-kg/blob/main/biocypher_dataset_downloader/versioning/README.md) —
  the resolver framework and how to add a new strategy.
- [ingestion-pipeline.md](ingestion-pipeline.md) — the full download → adapt → write flow.
- [configuration.md](../operations/configuration.md) — version strategy fields in `*_data_source_config.yaml`.
