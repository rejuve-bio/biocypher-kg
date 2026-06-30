"""Core abstractions for dataset version resolution.

A :class:`VersionGetter` turns one source entry from a data-source config into a
:class:`VersionInfo`. Getters never raise: any failure is captured on
``VersionInfo.error`` so a single broken source can't abort a whole download or
staleness run.
"""

from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass, field, asdict
from datetime import datetime
from typing import Any, Iterator, Optional

logger = logging.getLogger(__name__)


def _now_iso() -> str:
    return datetime.now().isoformat()


@dataclass
class VersionInfo:
    """Resolved version metadata for a single data source.

    ``version`` is a human-meaningful release string when one can be determined
    (e.g. ``v49``, ``2026_01``). When the source is pinned by URL with no parseable
    version, or only a ``current/`` symlink is available, ``version`` may be ``None``
    and ``signature`` carries an opaque change-detection token (ETag / Last-Modified /
    size) instead.
    """

    source_id: str
    name: Optional[str] = None
    version: Optional[str] = None
    date: Optional[str] = None
    url: Any = None  # str | list | dict, as declared in the config
    signature: Optional[str] = None  # opaque HEAD-derived change token
    vtype: str = "other"  # semver | date | sequential | static | daily | other
    strategy: str = ""
    resolved_at: str = field(default_factory=_now_iso)
    error: Optional[str] = None

    def to_dict(self) -> dict:
        return asdict(self)


def _strip_url_comment(url: str) -> str:
    """Drop an inline ``# comment`` from a config URL value (mirrors DownloadManager)."""
    return url.split("#", 1)[0].strip() if isinstance(url, str) else url


def iter_source_urls(source_config: dict) -> Iterator[str]:
    """Yield every download URL declared in a source config, comments stripped.

    Handles the same shapes DownloadManager accepts: ``url`` as str / list / dict
    (either ``{filename: url}`` or ``{sub_key: [urls]}``), plus the specialised
    ``bed_url`` / ``ep_base_url`` and ``zip_extract[].url`` keys.
    """

    def _walk(value: Any) -> Iterator[str]:
        if value is None:
            return
        if isinstance(value, str):
            cleaned = _strip_url_comment(value)
            if cleaned.startswith("http") or cleaned.startswith("gs://") or cleaned.startswith("ftp"):
                yield cleaned
        elif isinstance(value, list):
            for item in value:
                yield from _walk(item)
        elif isinstance(value, dict):
            for item in value.values():
                yield from _walk(item)

    yield from _walk(source_config.get("url"))
    for key in ("bed_url", "ep_base_url"):
        if source_config.get(key):
            yield _strip_url_comment(source_config[key])
    for entry in source_config.get("zip_extract", []) or []:
        if isinstance(entry, dict) and entry.get("url"):
            yield _strip_url_comment(entry["url"])
    for dir_url in (source_config.get("directories", {}) or {}).values():
        if isinstance(dir_url, str):
            yield _strip_url_comment(dir_url)


class VersionGetter(ABC):
    """Resolve the version of one data source. Subclasses implement :meth:`_resolve`."""

    strategy: str = "base"

    def __init__(self, source_id: str, source_config: dict, session: Any = None):
        self.source_id = source_id
        self.source_config = source_config or {}
        self.session = session
        self.spec = self.source_config.get("version") or {}

    @property
    def urls(self) -> list[str]:
        return list(iter_source_urls(self.source_config))

    @abstractmethod
    def _resolve(self) -> dict:
        """Return a partial dict of {version, date, signature, vtype}. May raise."""

    def get(self) -> VersionInfo:
        """Resolve to a :class:`VersionInfo`, capturing any error instead of raising."""
        base = VersionInfo(
            source_id=self.source_id,
            name=self.source_config.get("name"),
            # Keep the declared `url` structure when present; otherwise fall back to the
            # resolved URLs so sources that use bed_url / zip_extract / directories still
            # record a meaningful declared_url in the manifest.
            url=self.source_config.get("url") or (self.urls or None),
            strategy=self.strategy,
            vtype=self.spec.get("vtype", "other"),
        )
        try:
            resolved = self._resolve() or {}
        except Exception as exc:  # getters must never abort the caller
            logger.warning("Version resolution failed for '%s' (%s): %s", self.source_id, self.strategy, exc)
            base.error = str(exc)
            return base

        base.version = resolved.get("version", base.version)
        base.date = resolved.get("date", base.date)
        base.signature = resolved.get("signature", base.signature)
        base.vtype = resolved.get("vtype", base.vtype)
        return base


def resolve_source(source_id: str, source_config: dict, session: Any = None) -> VersionInfo:
    """Build the configured getter for a source and resolve it to a :class:`VersionInfo`."""
    # Imported here to avoid a circular import (registry imports from this module).
    from biocypher_dataset_downloader.versioning.registry import build_getter

    return build_getter(source_id, source_config, session=session).get()
