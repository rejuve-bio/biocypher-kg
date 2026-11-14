"""A small but real dbSNP cache, laid out the way scripts/update_dbsnp.py builds one,
for tests (the console refuses files that only look like a cache by name)."""
import json
import sqlite3
from pathlib import Path


def make_dbsnp_cache(variant_dir, variant="common", rows=None, index=True, version=True):
    d = Path(variant_dir)
    d.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(d / "dbsnp_mapping.db"))
    conn.execute("CREATE TABLE rsid_to_pos (rsid TEXT PRIMARY KEY, chr TEXT NOT NULL, "
                 "pos INTEGER NOT NULL)")
    conn.executemany("INSERT INTO rsid_to_pos VALUES (?, ?, ?)",
                     rows if rows is not None else [("rs123", "chr1", 10177),
                                                    ("rs456", "chr2", 20500)])
    if index:
        conn.execute("CREATE INDEX idx_pos ON rsid_to_pos (chr, pos)")
    conn.commit()
    conn.close()
    if version:
        (d / "dbsnp_version.json").write_text(json.dumps({
            "timestamp": "2026-11-01T02:00:00", "processor": "dbsnp", "format": "sqlite",
            "entries": 2, "common_only": variant == "common"}))
    return d / "dbsnp_mapping.db"
