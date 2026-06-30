# Dataset Version Resolution

This package resolves the **version** of each input dataset declaratively, so versions live in one
place (the data-source config) instead of being hardcoded across adapters. It powers the download
provenance manifest and the `check-versions` staleness checker.

For the end-to-end picture (manifest, build wiring, Neo4j lineage, CI), see
[doc/dataset-versioning.md](../../doc/dataset-versioning.md). This README is the framework reference.

## Overview

A source entry in a `*_data_source_config.yaml` may carry an optional `version:` block. The
registry turns that block into a `VersionGetter`, and the getter resolves the source to a
`VersionInfo`. Because our URLs usually embed the version, a regex over the URL covers most sources
with no per-source code.

## Core contract

- **`VersionInfo`** ([base.py](base.py)) — the result dataclass: `source_id`, `name`, `version`,
  `date`, `url`, `signature` (opaque change token when there's no human version), `vtype`,
  `strategy`, `resolved_at`, `error`.
- **`VersionGetter`** ([base.py](base.py)) — abstract base. Subclasses implement `_resolve()`
  returning a partial dict; the public `get()` wraps it. **Getters never raise** — any failure is
  captured on `VersionInfo.error`, so one broken source can't abort a download or a staleness run.
- **`resolve_source(source_id, source_config, session=None)`** — the one call sites use: builds the
  configured getter and returns its `VersionInfo`.

## Strategies ([strategies.py](strategies.py))

| Class | `strategy` name | Resolves from | Required config |
|---|---|---|---|
| `UrlRegexGetter` | `url_regex` | A regex (one capture group) over the declared URL(s) | `pattern` |
| `StaticGetter` | `static` | A declared constant | `value` |
| `HttpHeadGetter` | `http_head` | HTTP HEAD `ETag`/`Last-Modified`/`Content-Length` → `signature` | none (the default) |

The registry ([registry.py](registry.py)) selects the class from `version.strategy`; if the block
is absent it defaults to `http_head`, and an unknown strategy name falls back to `http_head` with a
warning (again, never raises).

## Declaring a version on a source

```yaml
gencode:
  name: GENCODE v49
  version: {strategy: url_regex, pattern: 'gencode\.(v\d+)\.', vtype: sequential}
  url: https://ftp.ebi.ac.uk/.../gencode.v49.annotation.gtf.gz
```

`UrlRegexGetter` matches the pattern against each declared URL until one captures (use `url_index`
to pin which URL when several are declared). `StaticGetter` takes `value:`. `HttpHeadGetter` needs
nothing — it's the fallback for `current/`-style URLs with no parseable version.

See [doc/dataset-versioning.md](../../doc/dataset-versioning.md#declaring-a-source-version) for the
full schema (including the `source_url` / `citation` / `license` / `checksum` provenance fields).

## Using it from Python

```python
from biocypher_dataset_downloader.versioning import resolve_source

info = resolve_source("gencode", source_config)   # source_config is the YAML dict for that source
print(info.version)   # "v49"
print(info.error)     # None, or the failure message
```

## Adding a new strategy

1. Subclass `VersionGetter` in [strategies.py](strategies.py) and implement `_resolve()` returning a
   partial dict of `{version, date, signature, vtype}`. Read your config from `self.spec` (the
   `version:` block) and the declared URLs from `self.urls`.
2. Set a unique `strategy` class attribute.
3. Register it in the `STRATEGIES` dict at the bottom of `strategies.py`.

```python
class MyGetter(VersionGetter):
    strategy = "my_strategy"

    def _resolve(self) -> dict:
        # self.spec -> the version: block; self.urls -> declared URLs; self.session -> requests session
        return {"version": "...", "vtype": self.spec.get("vtype", "other")}

STRATEGIES["my_strategy"] = MyGetter
```

Add a unit test in [test/test_versioning.py](../../test/test_versioning.py).

## Planned

A `LatestUrlGetter` (landing-page scraper) and/or an optional
[bioversions](https://github.com/biopragmatics/bioversions) delegate would add true "is there a
newer upstream release?" detection for pinned sources — the one thing a URL regex can't do. The
framework shape above is designed to absorb it as just another strategy.
