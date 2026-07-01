"""Select the version-resolution strategy for a source from its config.

A source's ``version:`` block names a strategy; absent a block, the default is
``http_head`` (works for any HTTP source and detects ``current/`` refreshes).
"""

from __future__ import annotations

import logging
from typing import Any

from biocypher_dataset_downloader.versioning.base import VersionGetter
from biocypher_dataset_downloader.versioning.strategies import STRATEGIES, HttpHeadGetter

logger = logging.getLogger(__name__)

DEFAULT_STRATEGY = HttpHeadGetter.strategy


def build_getter(source_id: str, source_config: dict, session: Any = None) -> VersionGetter:
    """Return the configured :class:`VersionGetter` for a source.

    The ``version.strategy`` field selects the class; an unknown strategy falls back
    to the default with a warning rather than raising, so one bad config entry can't
    break a whole run.
    """
    spec = (source_config or {}).get("version") or {}
    strategy = spec.get("strategy", DEFAULT_STRATEGY)

    cls = STRATEGIES.get(strategy)
    if cls is None:
        logger.warning(
            "Unknown version strategy %r for source '%s'; falling back to %r",
            strategy, source_id, DEFAULT_STRATEGY,
        )
        cls = STRATEGIES[DEFAULT_STRATEGY]

    return cls(source_id, source_config, session=session)
