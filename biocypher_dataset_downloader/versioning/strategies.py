"""Concrete version-resolution strategies.

- :class:`StaticGetter`   — a declared constant version (sources with no upstream versioning).
- :class:`UrlRegexGetter` — capture the version from the configured URL/filename via regex.
                            Covers the common case where the version is embedded in the URL
                            (``gencode.v49``, ``protein.links.v12.0``, ``..._fb_2026_01.tsv``).
- :class:`HttpHeadGetter` — derive an opaque change signature from HTTP HEAD metadata
                            (ETag / Last-Modified / Content-Length). The safe default and the
                            only strategy that can detect a refresh of a ``current/`` symlink.
"""

from __future__ import annotations

import logging
import re
from typing import Any, Optional

import requests

from biocypher_dataset_downloader.versioning.base import VersionGetter

logger = logging.getLogger(__name__)


class StaticGetter(VersionGetter):
    """Return a fixed version declared in the config: ``version: {strategy: static, value: v11}``."""

    strategy = "static"

    def _resolve(self) -> dict:
        value = self.spec.get("value")
        if value is None:
            raise ValueError("static strategy requires version.value")
        return {"version": str(value), "vtype": self.spec.get("vtype", "static")}


class UrlRegexGetter(VersionGetter):
    """Extract the version from a configured URL via a regex with one capture group.

    Config::

        version:
          strategy: url_regex
          pattern: 'gencode\\.(v\\d+)\\.'
          vtype: sequential        # optional
          url_index: 0             # optional, which URL to match when several are declared

    The pattern is matched against each declared URL (after stripping inline comments)
    until one captures; group 1 is the version.
    """

    strategy = "url_regex"

    def _resolve(self) -> dict:
        pattern = self.spec.get("pattern")
        if not pattern:
            raise ValueError("url_regex strategy requires version.pattern")
        regex = re.compile(pattern)

        urls = self.urls
        url_index = self.spec.get("url_index")
        if url_index is not None:
            urls = urls[url_index : url_index + 1]

        for url in urls:
            match = regex.search(url)
            if match:
                version = match.group(1) if match.groups() else match.group(0)
                return {"version": version, "vtype": self.spec.get("vtype", "other")}

        raise ValueError(f"pattern {pattern!r} matched none of {len(self.urls)} URL(s)")


class HttpHeadGetter(VersionGetter):
    """Derive a change signature from HTTP HEAD metadata of the primary URL.

    This is the default when no ``version:`` block is present. It yields no human
    version (``version`` stays ``None``) but records a ``signature`` token from
    ETag / Last-Modified / Content-Length, which the staleness checker compares
    across runs to detect a refreshed ``current/`` file.
    """

    strategy = "http_head"
    timeout = 15

    def _head(self, url: str) -> Optional[dict]:
        sess = self.session or requests
        try:
            resp = sess.head(url, timeout=self.timeout, allow_redirects=True)
            resp.raise_for_status()
            return {
                "etag": resp.headers.get("ETag"),
                "last_modified": resp.headers.get("Last-Modified"),
                "content_length": resp.headers.get("Content-Length"),
            }
        except requests.RequestException as exc:
            logger.warning("HEAD failed for %s: %s", url, exc)
            return None

    def _resolve(self) -> dict:
        urls = [u for u in self.urls if u.startswith("http")]
        if not urls:
            raise ValueError("http_head strategy needs at least one HTTP url")

        meta = self._head(urls[0])
        if not meta:
            raise ValueError(f"could not HEAD {urls[0]}")

        # Build an opaque, stable token; prefer the strongest available validator.
        token = meta.get("etag") or meta.get("last_modified") or meta.get("content_length")
        date = None
        if meta.get("last_modified"):
            date = meta["last_modified"]
        return {"signature": token, "date": date, "vtype": self.spec.get("vtype", "daily")}


STRATEGIES: dict[str, type[VersionGetter]] = {
    StaticGetter.strategy: StaticGetter,
    UrlRegexGetter.strategy: UrlRegexGetter,
    HttpHeadGetter.strategy: HttpHeadGetter,
}
