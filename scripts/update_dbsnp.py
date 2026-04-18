#!/usr/bin/env python3
"""
Standalone background service for updating dbSNP mappings.

This script is COMPLETELY STANDALONE - no dependency on biocypher_metta.
It runs independently from user queries and updates the dbSNP
SQLite database every 3 months.

Uses SQLite instead of in-memory dicts to avoid OOM kills.
Memory usage stays under ~100 MB regardless of dataset size.

Users always use the existing cached data - they never trigger updates.
Updates are atomic: new DB is built in temp location, then swapped
with old DB only after successful completion.

Usage:
    python update_dbsnp.py [--force]

Options:
    --force    Force update even if not due yet

Run as cron job (every 3 months, 1st of month at 2 AM):
    0 2 1 */3 * cd /path/to/biocypher-kg && python3 scripts/update_dbsnp.py \
        --cache-dir /your/dbsnp/common --common-only

Or create systemd timer for automatic scheduling.
"""

import sys
import logging
import argparse
import sqlite3
import gzip
import json
import requests
import shutil
from pathlib import Path
from datetime import datetime


DBSNP_URL = "https://ftp.ncbi.nih.gov/snp/latest_release/VCF/GCF_000001405.40.gz"

BATCH_SIZE = 500_000  # rows per INSERT batch — keeps memory low

COMMON_MAF_THRESHOLD = 0.01  # MAF ≥ 1% in any population → "common"


def is_common_variant(info_str: str, threshold: float = COMMON_MAF_THRESHOLD) -> bool:
    """
    Return True if variant has MAF >= threshold in any population
    listed in the INFO field's FREQ tag.

    FREQ format: FREQ=<source1>:<ref>,<alt>[,...]|<source2>:<ref>,<alt>|...
    Example:     FREQ=1000Genomes:0.95,0.05|TOPMED:0.97,0.03

    Returns False if no FREQ tag or no population meets the threshold.
    """
    freq_start = info_str.find('FREQ=')
    if freq_start == -1:
        return False

    # Extract FREQ value (up to next ';' or end of string)
    freq_end = info_str.find(';', freq_start)
    freq_value = info_str[freq_start + 5:freq_end] if freq_end != -1 else info_str[freq_start + 5:]

    # Upper bound for "varies at ≥1%": an allele freq in [threshold, 1-threshold]
    upper = 1.0 - threshold

    for pop_entry in freq_value.split('|'):
        colon = pop_entry.find(':')
        if colon == -1:
            continue
        for freq_str in pop_entry[colon + 1:].split(','):
            if not freq_str or freq_str == '.':
                continue
            try:
                f = float(freq_str)
            except ValueError:
                continue
            if threshold <= f <= upper:
                return True

    return False


def download_and_process_dbsnp(cache_dir: Path, logger: logging.Logger,
                               temp_dir: Path = None,
                               common_only: bool = False) -> bool:
    """
    Download and process dbSNP VCF file into a SQLite database.
    Streams rows directly to disk — memory usage stays under ~100 MB.

    Args:
        cache_dir: Directory to save processed files (SQLite DB + version JSON)
        logger: Logger instance
        temp_dir: Directory for the ~30 GB VCF download. Defaults to cache_dir.
        common_only: If True, keep only variants with MAF >= 1% in any
                     population (FREQ tag). Reduces ~800M → ~15-25M entries.

    Returns:
        True if successful, False otherwise
    """
    if temp_dir is None:
        temp_dir = cache_dir

    vcf_temp = None
    db_path = cache_dir / 'dbsnp_mapping.db'
    try:
        logger.info(f"Downloading from: {DBSNP_URL}")
        vcf_temp = temp_dir / "dbsnp_temp.vcf.gz"

        response = requests.get(DBSNP_URL, timeout=(30, 7200), stream=True)
        response.raise_for_status()

        downloaded = 0
        chunk_size = 1024 * 1024  # 1MB chunks
        with open(vcf_temp, 'wb') as f:
            for chunk in response.iter_content(chunk_size=chunk_size):
                if chunk:
                    f.write(chunk)
                    downloaded += len(chunk)
                    gb_downloaded = downloaded // (1024 * 1024 * 1024)
                    if gb_downloaded > 0 and downloaded % (1024 * 1024 * 1024) < chunk_size:
                        logger.info(f"Downloaded {gb_downloaded} GB...")

        logger.info(f"Download complete: {downloaded // (1024 * 1024)} MB")

        # --- Build SQLite DB on disk ---
        logger.info("Processing VCF file into SQLite (streaming, low memory)...")

        conn = sqlite3.connect(str(db_path))
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        conn.execute("PRAGMA cache_size=-64000")  # 64 MB page cache

        conn.execute("""
            CREATE TABLE IF NOT EXISTS rsid_to_pos (
                rsid TEXT PRIMARY KEY,
                chr  TEXT NOT NULL,
                pos  INTEGER NOT NULL
            )
        """)
        conn.commit()

        total_processed = 0
        kept_count = 0
        batch = []

        # VCF columns: CHROM(0) POS(1) ID(2) REF(3) ALT(4) QUAL(5) FILTER(6) INFO(7)
        # common_only=True needs INFO → split into 8 fields; else only 3.
        split_count = 7 if common_only else 2

        with gzip.open(vcf_temp, 'rt', encoding='utf-8') as vcf_in:
            for line in vcf_in:
                if line.startswith('#'):
                    continue

                total_processed += 1
                if total_processed % 5_000_000 == 0:
                    logger.info(f"Processed {total_processed:,} variants, kept {kept_count:,}...")

                fields = line.split('\t', split_count)
                if len(fields) < 3:
                    continue

                chrom = fields[0]
                pos = fields[1]
                rsid = fields[2]

                if not rsid or rsid == '.' or not pos:
                    continue

                if common_only:
                    if len(fields) < 8:
                        continue
                    if not is_common_variant(fields[7]):
                        continue

                kept_count += 1
                batch.append((rsid, chrom, int(pos)))

                if len(batch) >= BATCH_SIZE:
                    conn.executemany(
                        "INSERT OR REPLACE INTO rsid_to_pos (rsid, chr, pos) VALUES (?, ?, ?)",
                        batch
                    )
                    conn.commit()
                    batch.clear()

        # flush remaining rows
        if batch:
            conn.executemany(
                "INSERT OR REPLACE INTO rsid_to_pos (rsid, chr, pos) VALUES (?, ?, ?)",
                batch
            )
            conn.commit()

        logger.info(f"Processed {total_processed:,} total variants")
        logger.info(f"Inserted {kept_count:,} rsID mappings into SQLite")

        # Build indexes after bulk insert (faster than indexing during insert)
        logger.info("Building indexes...")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_pos ON rsid_to_pos (chr, pos)")
        conn.commit()
        conn.close()

        db_size_mb = db_path.stat().st_size / (1024 * 1024)
        logger.info(f"SQLite database size: {db_size_mb:.1f} MB")

        # Save version info
        filter_desc = (
            f'common variants only (MAF >= {COMMON_MAF_THRESHOLD} in any FREQ population)'
            if common_only
            else 'all variants with rsID and chr/pos'
        )
        version_info = {
            'timestamp': datetime.now().isoformat(),
            'processor': 'dbsnp',
            'entries': kept_count,
            'source': DBSNP_URL,
            'format': 'sqlite',
            'common_only': common_only,
            'filter': filter_desc,
        }

        version_file = cache_dir / 'dbsnp_version.json'
        with open(version_file, 'w') as f:
            json.dump(version_info, f, indent=2)
        logger.info(f"Saved version info to: {version_file}")

        return True

    except Exception as e:
        logger.error(f"Error during download/processing: {e}")
        # Close DB connection if open
        try:
            conn.close()
        except Exception:
            pass
        # Remove partial DB
        if db_path.exists():
            db_path.unlink()
        return False

    finally:
        if vcf_temp and vcf_temp.exists():
            logger.info("Cleaning up temporary VCF file...")
            vcf_temp.unlink()


def setup_logging(log_dir: Path) -> logging.Logger:
    """Set up file logging for background updates."""
    log_dir.mkdir(parents=True, exist_ok=True)

    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    log_file = log_dir / f"dbsnp_update_{timestamp}.log"

    logger = logging.getLogger('dbsnp_updater')
    logger.setLevel(logging.INFO)

    file_handler = logging.FileHandler(log_file)
    file_handler.setLevel(logging.INFO)

    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)

    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    file_handler.setFormatter(formatter)
    console_handler.setFormatter(formatter)

    logger.addHandler(file_handler)
    logger.addHandler(console_handler)

    logger.info(f"Logging to: {log_file}")

    return logger


def check_if_update_needed(cache_dir: Path, update_interval_days: int = 90) -> bool:
    """Check if update is needed based on time interval."""
    version_file = cache_dir / "dbsnp_version.json"

    if not version_file.exists():
        return True

    try:
        with open(version_file, 'r') as f:
            version_info = json.load(f)

        last_update = datetime.fromisoformat(version_info['timestamp'])
        time_since_update = datetime.now() - last_update

        return time_since_update.days >= update_interval_days

    except Exception:
        return True


def main():
    """Main entry point for background dbSNP updater."""
    parser = argparse.ArgumentParser(
        description='Background service for updating dbSNP mappings'
    )
    parser.add_argument(
        '--force',
        action='store_true',
        help='Force update even if not due yet'
    )
    parser.add_argument(
        '--cache-dir',
        type=str,
        required=True,
        help='Cache directory for dbSNP output (SQLite DB + version JSON). '
             'Convention: point at a variant subdir like <root>/common or <root>/full.'
    )
    parser.add_argument(
        '--temp-dir',
        type=str,
        default=None,
        help='Directory for the ~30 GB VCF download (default: same as --cache-dir)'
    )
    parser.add_argument(
        '--common-only',
        action='store_true',
        help='Keep only common variants (MAF >= 1%% in any FREQ population). '
             'Reduces ~800M → ~15-25M entries, final DB ~1-2 GB.'
    )

    args = parser.parse_args()

    cache_dir = Path(args.cache_dir)
    temp_dir = Path(args.temp_dir) if args.temp_dir else None

    logger = setup_logging(cache_dir)

    logger.info("=" * 80)
    logger.info("Starting dbSNP background updater")
    logger.info(f"Cache directory: {cache_dir}")
    logger.info(f"Temp directory: {temp_dir or cache_dir}")
    logger.info(f"Force update: {args.force}")
    logger.info(f"Common-only filter: {args.common_only}")
    logger.info("=" * 80)

    if not args.force:
        if not check_if_update_needed(cache_dir, update_interval_days=90):
            version_file = cache_dir / "dbsnp_version.json"
            with open(version_file, 'r') as f:
                version_info = json.load(f)
            last_update = datetime.fromisoformat(version_info['timestamp'])
            days_ago = (datetime.now() - last_update).days

            logger.info(f"Update not needed. Last updated {days_ago} days ago.")
            logger.info(f"Next update due in {90 - days_ago} days.")
            logger.info("Use --force to update anyway.")
            return 0

    logger.info("Update needed. Starting download and processing...")
    logger.info("This will take approximately 2-4 hours.")

    temp_cache_dir = cache_dir / f".updating_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    temp_cache_dir.mkdir(parents=True, exist_ok=True)

    logger.info(f"Building new database in temporary location: {temp_cache_dir}")

    try:
        logger.info("Downloading dbSNP VCF file (~30 GB)...")
        logger.info("Keeping all variants with rsID, chromosome, and position")

        success = download_and_process_dbsnp(
            cache_dir=temp_cache_dir,
            logger=logger,
            temp_dir=temp_dir,
            common_only=args.common_only
        )

        if not success:
            logger.error("Update failed!")
            logger.error(f"Temporary cache kept for debugging: {temp_cache_dir}")
            return 1

        logger.info("Processing completed successfully!")

        new_mapping = temp_cache_dir / "dbsnp_mapping.db"
        new_version = temp_cache_dir / "dbsnp_version.json"

        if not new_mapping.exists():
            logger.error("Mapping database not found after processing!")
            return 1

        mapping_size_mb = new_mapping.stat().st_size / (1024 * 1024)
        logger.info(f"New database size: {mapping_size_mb:.1f} MB")

        # Atomic swap: move new files to production location
        logger.info("Swapping new database with old files...")

        old_mapping = cache_dir / "dbsnp_mapping.db"
        old_version = cache_dir / "dbsnp_version.json"

        # Backup old files if they exist
        if old_mapping.exists():
            backup_mapping = cache_dir / f"dbsnp_mapping_backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}.db"
            logger.info(f"Backing up old mapping to: {backup_mapping}")
            shutil.copy2(old_mapping, backup_mapping)
            old_mapping.unlink()

        if old_version.exists():
            backup_version = cache_dir / f"dbsnp_version_backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
            shutil.copy2(old_version, backup_version)
            old_version.unlink()

        shutil.move(str(new_mapping), str(old_mapping))
        shutil.move(str(new_version), str(old_version))

        logger.info("File swap completed successfully!")

        # Clean up temporary directory
        if temp_cache_dir.exists():
            try:
                shutil.rmtree(temp_cache_dir)
                logger.info(f"Removed temporary directory: {temp_cache_dir}")
            except Exception as e:
                logger.error(f"Failed to remove temp directory: {e}")
                logger.error(f"Please manually remove: {temp_cache_dir}")

        # Clean up old backups (keep only last 2)
        backup_dbs = sorted(cache_dir.glob("dbsnp_mapping_backup_*.db"))
        if len(backup_dbs) > 2:
            for old_backup in backup_dbs[:-2]:
                logger.info(f"Removing old backup: {old_backup.name}")
                old_backup.unlink()
                version_backup = old_backup.parent / old_backup.name.replace('mapping', 'version').replace('.db', '.json')
                if version_backup.exists():
                    version_backup.unlink()

        logger.info("=" * 80)
        logger.info("dbSNP update completed successfully!")
        logger.info(f"New database: {old_mapping}")
        logger.info(f"Database size: {mapping_size_mb:.1f} MB")
        logger.info("=" * 80)

        return 0

    except Exception as e:
        logger.error("=" * 80)
        logger.error("FATAL ERROR during update:")
        logger.error(str(e))
        logger.error("=" * 80)

        import traceback
        logger.error(traceback.format_exc())

        logger.error(f"Temporary cache directory preserved for debugging: {temp_cache_dir}")

        return 1


if __name__ == '__main__':
    sys.exit(main())
