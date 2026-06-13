"""``check-versions`` — report dataset staleness against recorded versions.

Scope (see doc/plans/dataset-versioning-provenance.md, Stage 5): a resolver over a
*pinned* URL cannot reveal a newer upstream release, so this checker detects only what
is detectable from the config itself:

- ``CHANGED``    — an ``http_head`` source's signature (ETag/Last-Modified/size) differs
                   from what was last recorded (a ``current/`` file was refreshed).
- ``drift``      — a resolved version differs from the recorded one (the config's pinned
                   URL changed since the last download).
- ``up-to-date`` — resolved token matches the recorded one.
- ``unknown``    — pinned source with no recorded baseline; latest-release availability
                   cannot be determined here (future: a LatestUrlGetter / bioversions delegate).
- ``error``      — resolution failed.

Exit code is non-zero if any source is ``CHANGED`` or ``drift`` so CI can act.
"""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Optional

import requests
import typer
import yaml
from requests.adapters import HTTPAdapter
from typing_extensions import Annotated
from urllib3.util.retry import Retry

from biocypher_dataset_downloader.versioning.base import resolve_source

logger = logging.getLogger(__name__)

SPECIES = ["hsa", "dmel", "cel", "mmu", "rno"]
STALE_STATUSES = {"CHANGED", "drift"}

app = typer.Typer()


def _make_session() -> requests.Session:
    session = requests.Session()
    retry = Retry(total=3, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504])
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    return session


def _recorded_token(versions_history: dict, source_id: str) -> dict:
    """Return the last recorded {version, signature} for a source, or {} if none."""
    rows = (versions_history or {}).get(source_id) or []
    return rows[-1] if rows else {}


def evaluate_source(source_id: str, source_config: dict, recorded: dict, session=None) -> dict:
    """Resolve a source now and classify it against its recorded baseline."""
    info = resolve_source(source_id, source_config, session=session)
    row = {
        "source": source_id,
        "strategy": info.strategy,
        "recorded": recorded.get("version") or recorded.get("signature"),
        "current": info.version or info.signature,
        "date": info.date,
        "status": "unknown",
    }

    if info.error:
        row["status"] = "error"
        row["error"] = info.error
        return row

    if not recorded:
        # No baseline recorded yet. Signature sources are simply unverified; pinned
        # sources can't have latest-availability inferred either.
        row["status"] = "unknown"
        return row

    if info.strategy == "http_head":
        row["status"] = "up-to-date" if info.signature == recorded.get("signature") else "CHANGED"
    else:  # url_regex / static — version is pinned by the config URL
        row["status"] = "up-to-date" if info.version == recorded.get("version") else "drift"

    return row


def check_versions_for_config(config_path: Path, versions_path: Optional[Path], session=None) -> list[dict]:
    """Evaluate every source in one data_source_config against its recorded versions.json."""
    with open(config_path, "r") as f:
        sources = yaml.safe_load(f) or {}

    history = {}
    if versions_path and Path(versions_path).exists():
        try:
            with open(versions_path, "r") as f:
                history = json.load(f)
        except json.JSONDecodeError as e:
            logger.warning(f"Recorded versions file {versions_path} is invalid JSON ({e})")

    rows = []
    for source_id, source_config in sources.items():
        if source_id == "name" or not isinstance(source_config, dict):
            continue
        rows.append(evaluate_source(source_id, source_config, _recorded_token(history, source_id), session=session))
    return rows


def _print_table(species: str, rows: list[dict]):
    typer.echo(f"\n=== {species} ===")
    typer.echo(f"{'source':<22} {'strategy':<10} {'recorded':<18} {'current':<18} {'status'}")
    typer.echo("-" * 84)
    for r in sorted(rows, key=lambda x: x["source"]):
        typer.echo(
            f"{r['source']:<22} {r['strategy']:<10} "
            f"{str(r['recorded'] or '-'):<18} {str(r['current'] or '-'):<18} {r['status']}"
        )


@app.command()
def check_versions(
    species: str = typer.Option("all", help="Species to check (hsa/dmel/cel/mmu/rno) or 'all'."),
    source: Optional[str] = typer.Option(None, help="Restrict to a single source id."),
    config_dir: Path = typer.Option(Path("config"), help="Directory holding per-species config folders."),
    versions_root: Optional[Path] = typer.Option(
        None,
        help="Base dir holding per-species recorded versions: <versions-root>/<species>/versions.json.",
    ),
    json_output: Annotated[bool, typer.Option("--json", help="Emit machine-readable JSON instead of a table.")] = False,
):
    """Compare each source's resolvable version against the last recorded one."""
    species_list = SPECIES if species == "all" else [species]
    session = _make_session()

    all_rows: dict[str, list[dict]] = {}
    any_stale = False

    for sp in species_list:
        config_path = config_dir / sp / f"{sp}_data_source_config.yaml"
        if not config_path.exists():
            logger.warning(f"No data source config for species '{sp}' at {config_path}; skipping")
            continue
        versions_path = (versions_root / sp / "versions.json") if versions_root else None

        rows = check_versions_for_config(config_path, versions_path, session=session)
        if source:
            rows = [r for r in rows if r["source"] == source]
        all_rows[sp] = rows
        any_stale = any_stale or any(r["status"] in STALE_STATUSES for r in rows)

    if json_output:
        typer.echo(json.dumps(all_rows, indent=2))
    else:
        for sp, rows in all_rows.items():
            _print_table(sp, rows)
        n_stale = sum(1 for rows in all_rows.values() for r in rows if r["status"] in STALE_STATUSES)
        typer.echo(f"\n{n_stale} source(s) CHANGED/drifted.")

    raise typer.Exit(1 if any_stale else 0)


if __name__ == "__main__":
    app()
