"""Dataset version resolution for biocypher-kg.

Lightweight, declarative version detection for input datasets. Each source in a
``{species}_data_source_config.yaml`` may carry an optional ``version:`` block selecting
a resolver strategy (``url_regex`` / ``http_head`` / ``static``); the resolver turns the
source config into a :class:`VersionInfo` recording the version, date, and a change
signature. Used by the downloader to build a provenance manifest and by the staleness CLI.
"""

from biocypher_dataset_downloader.versioning.base import (
    VersionInfo,
    VersionGetter,
    iter_source_urls,
    resolve_source,
)

__all__ = [
    "VersionInfo",
    "VersionGetter",
    "iter_source_urls",
    "resolve_source",
]
