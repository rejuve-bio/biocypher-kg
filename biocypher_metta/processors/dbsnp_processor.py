"""
dbSNP Processor for rsID to Genomic Position Mappings.

LOAD-ONLY: This processor only loads pre-existing cache files.
Updates are handled by the separate update_dbsnp.py script.

Supports two backends:
  - SQLite (.db)  — preferred, low memory usage regardless of dataset size
  - Pickle (.pkl) — legacy fallback for existing cache files

Reverse lookups (position → rsID) use the idx_pos index on the single
rsid_to_pos table — no separate pos_to_rsid table needed.
"""

import sqlite3
import pickle
import gzip
import time
from pathlib import Path
from typing import Dict, Any, Optional

from biocypher._logger import logger


class DBSNPProcessor:

    def __init__(self, cache_dir: str = 'aux_files/hsa/sample_dbsnp'):
        self.name = 'dbsnp'
        self.cache_dir = Path(cache_dir)
        self.db_file = self.cache_dir / 'dbsnp_mapping.db'
        self.mapping_file = self.cache_dir / 'dbsnp_mapping.pkl'
        self.version_file = self.cache_dir / 'dbsnp_version.json'

        self._conn: Optional[sqlite3.Connection] = None
        self._backend: Optional[str] = None  # 'sqlite' or 'pickle'

        # Only used for pickle fallback
        self.mapping: Dict[str, Any] = {}

    def load_mapping(self) -> None:
        """Load mapping — prefers SQLite, falls back to pickle."""
        if self.db_file.exists():
            self._load_sqlite()
        elif self.mapping_file.exists():
            self._load_pickle()
        else:
            raise FileNotFoundError(
                f"{self.name}: No cache file found in {self.cache_dir}\n"
                f"Run 'python scripts/update_dbsnp.py' to create the cache."
            )
        self._log_version()

    def _load_sqlite(self) -> None:
        db_size_bytes = self.db_file.stat().st_size if self.db_file.exists() else 0
        logger.info(
            f"{self.name}: Opening SQLite cache at {self.db_file}"
            f" ({self._format_bytes(db_size_bytes)})"
        )

        started = time.time()
        db_uri = f"file:{self.db_file}?immutable=1"
        self._conn = sqlite3.connect(db_uri, uri=True)
        self._conn.execute("PRAGMA query_only=ON")
        self._backend = 'sqlite'
        logger.info(f"{self.name}: SQLite connection ready in {time.time() - started:.1f}s")

        info = self._read_version_info()
        entries = info.get('entries')
        if entries is not None:
            logger.info(
                f"{self.name}: Cache ready with {entries:,} rsIDs"
                f" (from version metadata) in {time.time() - started:.1f}s"
            )
            return

        logger.info(f"{self.name}: Counting rsIDs in SQLite cache. This can take a while for full datasets...")
        count_started = time.time()
        row = self._conn.execute("SELECT COUNT(*) FROM rsid_to_pos").fetchone()
        logger.info(
            f"{self.name}: Loaded SQLite database ({row[0]:,} rsIDs) from {self.db_file}"
            f" in {time.time() - count_started:.1f}s"
        )

    def _load_pickle(self) -> None:
        try:
            with gzip.open(self.mapping_file, 'rb') as f:
                self.mapping = pickle.load(f)
        except (OSError, gzip.BadGzipFile):
            logger.info(f"{self.name}: Loading uncompressed pickle file...")
            with open(self.mapping_file, 'rb') as f:
                self.mapping = pickle.load(f)

        self._backend = 'pickle'
        logger.info(f"{self.name}: Loaded pickle mapping from {self.mapping_file}")

    def _log_version(self) -> None:
        info = self._read_version_info()
        if not info:
            return
        if 'timestamp' in info:
            from datetime import datetime
            try:
                timestamp = datetime.fromisoformat(info['timestamp'])
                updated_at = timestamp.strftime('%Y-%m-%d %H:%M:%S')
            except (ValueError, TypeError):
                updated_at = info.get('timestamp', 'Unknown')
            logger.info(f"{self.name}: Cache last updated: {updated_at}")
        if 'common_only' in info:
            variant = 'common-only' if info['common_only'] else 'full'
            logger.info(f"{self.name}: Variant: {variant} ({info.get('entries', '?')} entries)")

    def _read_version_info(self) -> Dict[str, Any]:
        """Return contents of dbsnp_version.json, or {} if missing/unreadable."""
        if not self.version_file.exists():
            return {}
        import json
        try:
            with open(self.version_file, 'r') as f:
                return json.load(f)
        except Exception:
            return {}

    @staticmethod
    def _format_bytes(size_bytes: int) -> str:
        """Format a byte count for logging."""
        if size_bytes <= 0:
            return "0 B"
        units = ["B", "KB", "MB", "GB", "TB"]
        size = float(size_bytes)
        for unit in units:
            if size < 1024 or unit == units[-1]:
                return f"{size:.1f} {unit}"
            size /= 1024

    def is_common_only(self) -> Optional[bool]:
        """
        Return True if the loaded cache was built with --common-only,
        False if it's the full dataset, or None if the field is absent
        (legacy caches predating the common/full split).
        """
        return self._read_version_info().get('common_only')

    @staticmethod
    def _clean_rsid(raw_rsid: str) -> str:
        """Return the actual rsID from a possibly malformed SQLite key."""
        return raw_rsid.split('\t', 1)[0].strip()

    @staticmethod
    def _malformed_rsid_bounds(rsid: str):
        """Return an index-friendly key range for malformed rsID rows."""
        lower = f"{rsid}\t"
        return lower, f"{lower}\uffff"

    def _lookup_rsid_row(self, rsid: str):
        """Return SQLite row for an rsID, tolerating legacy malformed full-db keys."""
        row = self._conn.execute(
            "SELECT rsid, chr, pos FROM rsid_to_pos WHERE rsid = ?",
            (rsid,),
        ).fetchone()
        if row:
            return row

        lower, upper = self._malformed_rsid_bounds(rsid)
        return self._conn.execute(
            "SELECT rsid, chr, pos FROM rsid_to_pos "
            "WHERE rsid >= ? AND rsid < ? ORDER BY rsid LIMIT 1",
            (lower, upper),
        ).fetchone()

    # --- Public query API ---

    def get_position(self, rsid: str) -> Optional[Dict[str, Any]]:
        """Get genomic position for an rsID."""
        self._ensure_loaded()

        if self._backend == 'sqlite':
            row = self._lookup_rsid_row(rsid)
            if row:
                return {'chr': row[1], 'pos': row[2]}
            return None

        # pickle fallback
        if self._is_nested_format():
            return self.mapping.get('rsid_to_pos', {}).get(rsid)
        return self.mapping.get(rsid)

    def get_rsid(self, chrom: str, pos: int) -> Optional[str]:
        """Get rsID for a genomic position. Uses idx_pos index on rsid_to_pos."""
        self._ensure_loaded()

        if self._backend == 'sqlite':
            row = self._conn.execute(
                "SELECT rsid FROM rsid_to_pos WHERE chr = ? AND pos = ?",
                (chrom, pos)
            ).fetchone()
            if row:
                return self._clean_rsid(row[0])
            # Try alternative format (with/without 'chr' prefix)
            if not chrom.startswith('chr'):
                row = self._conn.execute(
                    "SELECT rsid FROM rsid_to_pos WHERE chr = ? AND pos = ?",
                    (f"chr{chrom}", pos)
                ).fetchone()
                if row:
                    return self._clean_rsid(row[0])
            return None

        # pickle fallback
        if not self._is_nested_format():
            return None
        pos_to_rsid = self.mapping.get('pos_to_rsid', {})
        pos_key = f"{chrom}:{pos}"
        rsid = pos_to_rsid.get(pos_key)
        if rsid:
            return rsid
        if not chrom.startswith('chr'):
            return pos_to_rsid.get(f"chr{chrom}:{pos}")
        return None

    def get_dict_wrappers(self):
        """Return dict-like accessors for rsid_to_pos and pos_to_rsid."""
        self._ensure_loaded()

        if self._backend == 'sqlite':
            return (
                _SQLiteRsidToPosWrapper(self._conn),
                _SQLitePosToRsidWrapper(self._conn),
            )

        # pickle fallback
        if self._is_nested_format():
            return (
                self.mapping.get('rsid_to_pos', {}),
                self.mapping.get('pos_to_rsid', {}),
            )
        logger.info(f"{self.name}: Detected legacy flat format, using as rsid_to_pos")
        return (self.mapping, {})

    # --- Internal helpers ---

    def _ensure_loaded(self) -> None:
        if self._backend is None:
            if self.db_file.exists() or self.mapping_file.exists():
                self.load_mapping()
            else:
                raise FileNotFoundError(
                    f"{self.name}: No cache file found in {self.cache_dir}\n"
                    f"Run 'python scripts/update_dbsnp.py' to create the cache."
                )

    def _is_nested_format(self) -> bool:
        return 'rsid_to_pos' in self.mapping or 'pos_to_rsid' in self.mapping

    def close(self) -> None:
        if self._conn:
            self._conn.close()
            self._conn = None
            self._backend = None


class _SQLiteRsidToPosWrapper:
    """Dict-like wrapper: rsid → {'chr': ..., 'pos': ...}"""

    def __init__(self, conn: sqlite3.Connection):
        self._conn = conn

    def get(self, rsid, default=None):
        row = self._conn.execute(
            "SELECT rsid, chr, pos FROM rsid_to_pos WHERE rsid = ?",
            (rsid,),
        ).fetchone()
        if row is None:
            lower, upper = DBSNPProcessor._malformed_rsid_bounds(rsid)
            row = self._conn.execute(
                "SELECT rsid, chr, pos FROM rsid_to_pos "
                "WHERE rsid >= ? AND rsid < ? ORDER BY rsid LIMIT 1",
                (lower, upper),
            ).fetchone()
        if row is None:
            return default
        return {'chr': row[1], 'pos': row[2]}

    def __getitem__(self, rsid):
        result = self.get(rsid)
        if result is None:
            raise KeyError(rsid)
        return result

    def __contains__(self, rsid):
        return self.get(rsid) is not None

    def __len__(self):
        return self._conn.execute("SELECT COUNT(*) FROM rsid_to_pos").fetchone()[0]


class _SQLitePosToRsidWrapper:
    """Dict-like wrapper: (chr, pos) → rsid. Accepts 'chr_pos' or 'chr:pos' key formats."""

    def __init__(self, conn: sqlite3.Connection):
        self._conn = conn

    def _parse_key(self, key: str):
        """Parse 'chr_pos' or 'chr:pos' into (chr, pos)."""
        for sep in ('_', ':'):
            if sep in key:
                parts = key.rsplit(sep, 1)
                if len(parts) == 2:
                    try:
                        return parts[0], int(parts[1])
                    except ValueError:
                        continue
        return None, None

    def get(self, key, default=None):
        chrom, pos = self._parse_key(key)
        if chrom is None:
            return default
        row = self._conn.execute(
            "SELECT rsid FROM rsid_to_pos WHERE chr = ? AND pos = ?",
            (chrom, pos)
        ).fetchone()
        if row is None:
            return default
        return DBSNPProcessor._clean_rsid(row[0])

    def __getitem__(self, key):
        result = self.get(key)
        if result is None:
            raise KeyError(key)
        return result

    def __contains__(self, key):
        return self.get(key) is not None

    def __len__(self):
        return self._conn.execute("SELECT COUNT(*) FROM rsid_to_pos").fetchone()[0]
