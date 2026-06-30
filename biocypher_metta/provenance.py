"""Resolve dataset provenance for adapters from the download manifest.

At build time the KG pipeline reads ``download_manifest.json`` (written by the
downloader) so each adapter's dataset version / URL / checksum comes from a single
authoritative record instead of drifting hardcoded ``self.version`` attributes.

Provenance is keyed by **source_id**, derived from an adapter entry's ``outdir``
first path segment (e.g. ``gencode/gene`` -> ``gencode``) or an explicit
``source_id:`` field on the adapter entry. Keying by source_id (rather than by file
path) is robust to sample paths, renamed files, and intermediate processor outputs.
"""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)


def source_id_for_adapter(adapter_entry: dict) -> Optional[str]:
    """Derive the manifest source_id for an adapter entry.

    Prefers an explicit ``source_id`` field; otherwise uses the first path segment
    of ``outdir`` (``gencode/gene`` -> ``gencode``). Returns None if neither is set.
    """
    explicit = adapter_entry.get("source_id")
    if explicit:
        return str(explicit)
    outdir = adapter_entry.get("outdir")
    if isinstance(outdir, str) and outdir.strip():
        return outdir.strip().strip("/").split("/")[0]
    return None


class ProvenanceLookup:
    """Read a download manifest and resolve per-source provenance by source_id."""

    def __init__(self, manifest_path: Path):
        self.manifest_path = Path(manifest_path)
        self.sources: dict = {}
        try:
            with open(self.manifest_path, "r") as f:
                self.sources = (json.load(f) or {}).get("sources", {})
            logger.info(f"Loaded provenance manifest ({len(self.sources)} sources): {self.manifest_path}")
        except FileNotFoundError:
            logger.warning(f"Provenance manifest not found at {self.manifest_path}; provenance disabled.")
        except json.JSONDecodeError as e:
            logger.warning(f"Provenance manifest at {self.manifest_path} is invalid JSON ({e}); provenance disabled.")

    def __bool__(self) -> bool:
        return bool(self.sources)

    def for_source(self, source_id: Optional[str]) -> Optional[dict]:
        """Return provenance for a source_id, or None if not in the manifest.

        Shape: ``{version, source_url, date, citation, license, checksums}`` where
        ``checksums`` maps each file's rel_path to its sha256.
        """
        if not source_id:
            return None
        entry = self.sources.get(source_id)
        if not entry:
            return None
        return {
            "version": entry.get("version"),
            "source_url": entry.get("source_url"),
            "date": entry.get("date"),
            "citation": entry.get("citation"),
            "license": entry.get("license"),
            "checksums": {f["rel_path"]: f.get("sha256") for f in entry.get("files", []) if f.get("rel_path")},
        }

    def attach(self, adapters_dict: dict) -> int:
        """Attach a ``provenance`` dict to each adapter entry it can resolve.

        Returns the number of adapters matched. Adapters with no manifest match keep
        ``provenance == None`` and fall back to their own attributes at build time.
        """
        matched = 0
        for entry in adapters_dict.values():
            if not isinstance(entry, dict):
                continue
            prov = self.for_source(source_id_for_adapter(entry))
            entry["provenance"] = prov
            if prov:
                matched += 1
        return matched
