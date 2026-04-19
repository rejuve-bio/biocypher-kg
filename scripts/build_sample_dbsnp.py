#!/usr/bin/env python3
"""
Build a tiny sample dbSNP SQLite DB from an existing common/full DB.

Reads a sample adapters config (e.g. config/hsa/hsa_adapters_config_sample.yaml),
finds every adapter that consumes dbSNP lookups, extracts the rsIDs and
(chr, pos) pairs referenced by that adapter's sample input file, queries the
source DB for each, and writes a small SQLite DB + version JSON to the output
directory.

The output lives at <output>/dbsnp_mapping.db and is committed to the repo, so
most users never run this script. Only people who change sample input files
need to regenerate the sample cache.

Usage:
    python3 scripts/build_sample_dbsnp.py \\
        --source /path/to/dbsnp/common \\
        --adapters-config config/hsa/hsa_adapters_config_sample.yaml \\
        --output aux_files/hsa/sample_dbsnp

Prerequisites:
    A common (or full) SQLite DB must exist at <source>/dbsnp_mapping.db.
    Build one with: scripts/update_dbsnp.py --cache-dir <source> --common-only
"""

import argparse
import csv
import gzip
import json
import logging
import sqlite3
import sys
import yaml
from datetime import datetime
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Set, Tuple


# Per-adapter extraction rules. Keyed by the adapter class name.
# Only include adapters that actually *consume* the dbSNP cache (have a
# dbsnp_rsid_map or dbsnp_pos_map kwarg). DBSNPAdapter is NOT in this list —
# it reads the VCF directly to emit rsID nodes and does not need the cache.
# Each entry describes:
#   kind:      'rsid' (forward lookup) or 'pos' (reverse lookup)
#   column:    for 'rsid': index of the rsID column in the CSV
#              for 'pos':  list of position column indices (e.g. [0, 1] for TopLD)
#   has_header: True if the CSV has a header row to skip
#   chr_from_args: for 'pos', which adapter arg holds the chromosome (e.g. "chr")
EXTRACTORS = {
    'ABCAdapter':              {'kind': 'rsid', 'column': 0, 'has_header': True},
    'CADDAdapter':             {'kind': 'rsid', 'column': 0, 'has_header': True},
    'RefSeqClosestGeneAdapter': {'kind': 'rsid', 'column': 0, 'has_header': True},
    'TopLDAdapter':            {'kind': 'pos',  'columns': [0, 1], 'has_header': True,
                                'chr_from_args': 'chr'},
}


def setup_logging() -> logging.Logger:
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
    )
    return logging.getLogger('build_sample_dbsnp')


def open_maybe_gz(path: Path):
    """Open a plain or gzipped text file."""
    if str(path).endswith('.gz'):
        return gzip.open(path, 'rt', encoding='utf-8')
    return open(path, 'rt', encoding='utf-8')


def extract_rsids_csv(path: Path, column: int, has_header: bool, logger) -> Set[str]:
    """Extract rsIDs from a column of a CSV."""
    rsids: Set[str] = set()
    with open_maybe_gz(path) as f:
        reader = csv.reader(f)
        if has_header:
            next(reader, None)
        for row in reader:
            if len(row) > column:
                rsid = row[column].strip()
                if rsid and rsid != '.':
                    rsids.add(rsid)
    logger.info(f"  {path.name}: {len(rsids)} unique rsIDs")
    return rsids


def extract_positions_csv(path: Path, columns: List[int], chrom: str,
                          has_header: bool, logger) -> Set[Tuple[str, int]]:
    """Extract (chr, pos) pairs from given columns (each is a pos). chrom is fixed per-file."""
    positions: Set[Tuple[str, int]] = set()
    with open_maybe_gz(path) as f:
        reader = csv.reader(f)
        if has_header:
            next(reader, None)
        for row in reader:
            for col in columns:
                if len(row) > col:
                    try:
                        pos = int(row[col].strip())
                    except ValueError:
                        continue
                    positions.add((chrom, pos))
    logger.info(f"  {path.name}: {len(positions)} unique positions on {chrom}")
    return positions


def collect_from_adapters_config(config_path: Path, logger) -> Tuple[Set[str], Set[Tuple[str, int]]]:
    """Parse the adapters config and extract rsIDs + positions from each adapter's input file."""
    with open(config_path) as f:
        config = yaml.safe_load(f)

    rsids: Set[str] = set()
    positions: Set[Tuple[str, int]] = set()
    matched_adapters: List[str] = []

    for name, block in config.items():
        adapter = block.get('adapter', {})
        cls_name = adapter.get('cls')
        if cls_name not in EXTRACTORS:
            continue

        args = adapter.get('args', {})
        # Only include adapters that actually accept a dbSNP map kwarg
        if 'dbsnp_rsid_map' not in args and 'dbsnp_pos_map' not in args:
            continue

        filepath = args.get('filepath')
        if not filepath:
            logger.warning(f"  {name}: missing 'filepath' arg, skipping")
            continue

        file_path = Path(filepath)
        if not file_path.is_absolute():
            # Config paths are typically relative to repo root
            file_path = Path.cwd() / file_path
        if not file_path.exists():
            logger.warning(f"  {name}: input file not found: {file_path}, skipping")
            continue

        rule = EXTRACTORS[cls_name]
        matched_adapters.append(name)

        if rule['kind'] == 'rsid':
            rsids |= extract_rsids_csv(file_path, rule['column'], rule['has_header'], logger)
        elif rule['kind'] == 'pos':
            chrom = args.get(rule['chr_from_args'])
            if not chrom:
                logger.warning(f"  {name}: no '{rule['chr_from_args']}' arg, skipping")
                continue
            positions |= extract_positions_csv(file_path, rule['columns'], chrom,
                                               rule['has_header'], logger)

    logger.info(f"Matched {len(matched_adapters)} adapters: {', '.join(matched_adapters)}")
    return rsids, positions


def query_source_db(source_db: Path, rsids: Set[str], positions: Set[Tuple[str, int]],
                    logger) -> List[Tuple[str, str, int]]:
    """Query the source SQLite DB for each rsID and position. Return list of (rsid, chr, pos) rows."""
    if not source_db.exists():
        logger.error(f"Source DB not found: {source_db}")
        logger.error("Build it first with: python3 scripts/update_dbsnp.py --cache-dir <dir> --common-only")
        sys.exit(1)

    conn = sqlite3.connect(str(source_db))
    conn.execute("PRAGMA query_only=ON")

    rows: Dict[str, Tuple[str, str, int]] = {}  # keyed by rsid to dedupe

    # Forward lookups
    rsid_hits, rsid_misses = 0, 0
    for rsid in rsids:
        row = conn.execute(
            "SELECT chr, pos FROM rsid_to_pos WHERE rsid = ?", (rsid,)
        ).fetchone()
        if row:
            rows[rsid] = (rsid, row[0], row[1])
            rsid_hits += 1
        else:
            rsid_misses += 1
    if rsids:
        logger.info(f"rsID lookups: {rsid_hits} hit / {rsid_misses} miss")

    # Reverse lookups (TopLD positions)
    pos_hits, pos_misses = 0, 0
    for chrom, pos in positions:
        row = conn.execute(
            "SELECT rsid FROM rsid_to_pos WHERE chr = ? AND pos = ?", (chrom, pos)
        ).fetchone()
        if not row and not chrom.startswith('chr'):
            # Try with 'chr' prefix
            row = conn.execute(
                "SELECT rsid FROM rsid_to_pos WHERE chr = ? AND pos = ?",
                (f"chr{chrom}", pos)
            ).fetchone()
        if row:
            rsid = row[0]
            rows[rsid] = (rsid, chrom, pos)
            pos_hits += 1
        else:
            pos_misses += 1
    if positions:
        logger.info(f"Position lookups: {pos_hits} hit / {pos_misses} miss")

    conn.close()
    return list(rows.values())


def write_sample_db(output_dir: Path, rows: List[Tuple[str, str, int]],
                    source_db: Path, logger) -> None:
    """Write the sample SQLite DB + version JSON."""
    output_dir.mkdir(parents=True, exist_ok=True)
    db_path = output_dir / 'dbsnp_mapping.db'
    version_path = output_dir / 'dbsnp_version.json'

    # Remove old DB so we start fresh
    if db_path.exists():
        db_path.unlink()

    conn = sqlite3.connect(str(db_path))
    conn.execute("PRAGMA journal_mode=DELETE")  # no WAL for committed file
    conn.execute("""
        CREATE TABLE rsid_to_pos (
            rsid TEXT PRIMARY KEY,
            chr  TEXT NOT NULL,
            pos  INTEGER NOT NULL
        )
    """)
    conn.executemany(
        "INSERT OR REPLACE INTO rsid_to_pos (rsid, chr, pos) VALUES (?, ?, ?)", rows
    )
    conn.execute("CREATE INDEX idx_pos ON rsid_to_pos (chr, pos)")
    conn.commit()
    conn.close()

    version_info = {
        'timestamp': datetime.now().isoformat(),
        'processor': 'dbsnp',
        'entries': len(rows),
        'source': str(source_db),
        'format': 'sqlite',
        'sample_from_common': True,
        'filter': 'sampled to cover sample adapter inputs',
    }
    with open(version_path, 'w') as f:
        json.dump(version_info, f, indent=2)

    size_kb = db_path.stat().st_size / 1024
    logger.info(f"Wrote {db_path} ({len(rows)} entries, {size_kb:.1f} KB)")
    logger.info(f"Wrote {version_path}")


def main() -> int:
    parser = argparse.ArgumentParser(
        description='Build a sample dbSNP SQLite DB from an existing common/full DB.'
    )
    parser.add_argument(
        '--source', required=True,
        help='Directory containing the source dbsnp_mapping.db (common or full).'
    )
    parser.add_argument(
        '--adapters-config', required=True,
        help='Sample adapters YAML (e.g. config/hsa/hsa_adapters_config_sample.yaml).'
    )
    parser.add_argument(
        '--output', required=True,
        help='Output directory for dbsnp_mapping.db + dbsnp_version.json.'
    )
    parser.add_argument(
        '--extra-rsid', nargs='*', default=[],
        help='Extra rsIDs to always include.'
    )
    parser.add_argument(
        '--extra-pos', nargs='*', default=[],
        help='Extra positions to always include, in "chr:pos" format.'
    )
    args = parser.parse_args()

    logger = setup_logging()

    source_dir = Path(args.source)
    source_db = source_dir / 'dbsnp_mapping.db'
    config_path = Path(args.adapters_config)
    output_dir = Path(args.output)

    logger.info(f"Source DB:       {source_db}")
    logger.info(f"Adapters config: {config_path}")
    logger.info(f"Output dir:      {output_dir}")

    logger.info("Parsing adapters config and extracting rsIDs + positions...")
    rsids, positions = collect_from_adapters_config(config_path, logger)

    # Add extras
    for rsid in args.extra_rsid:
        rsids.add(rsid)
    for spec in args.extra_pos:
        if ':' not in spec:
            logger.warning(f"Bad --extra-pos '{spec}', expected 'chr:pos'")
            continue
        chrom, pos_str = spec.rsplit(':', 1)
        try:
            positions.add((chrom, int(pos_str)))
        except ValueError:
            logger.warning(f"Bad --extra-pos '{spec}', pos must be int")

    logger.info(f"Total to query: {len(rsids)} rsIDs, {len(positions)} positions")
    if not rsids and not positions:
        logger.error("Nothing to extract — check your adapters config.")
        return 1

    rows = query_source_db(source_db, rsids, positions, logger)
    if not rows:
        logger.error("No rows returned from source DB. Sample DB would be empty.")
        return 1

    write_sample_db(output_dir, rows, source_db, logger)
    logger.info("Done.")
    return 0


if __name__ == '__main__':
    sys.exit(main())
