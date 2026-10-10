#!/usr/bin/env python3
"""
One-time fix for .cypher files generated before the multi-species import_root
fix in biocypher_metta/neo4j_csv_writer.py.

Each species subdirectory under ROOT (e.g. ROOT/dmel, ROOT/hsa, ...) contains
nodes_*.cypher / edges_*.cypher files whose `LOAD CSV FROM 'file:///...'`
paths were computed relative to the species subdir itself, missing the
species-name prefix needed when ROOT (not the species subdir) is mounted as
Neo4j's /import.

This script rewrites `file:///<relative_path>` to `file:///<species>/<relative_path>`
in place, for every .cypher file under each immediate subdirectory of ROOT.
Idempotent: skips files that already have the species prefix.

Usage:
    python fix_cypher_import_paths.py /mnt/hdd_1/biocypher-kg/output_20260706 [--dry-run]
"""
import argparse
import re
import sys
from pathlib import Path

FILE_URI_RE = re.compile(r"file:///([^'\"]+)")


def fix_file(cypher_path: Path, species: str, dry_run: bool) -> bool:
    text = cypher_path.read_text()

    def repl(match):
        rel_path = match.group(1)
        if rel_path.startswith(f"{species}/"):
            return match.group(0)
        return f"file:///{species}/{rel_path}"

    new_text, count = FILE_URI_RE.subn(repl, text)
    if count == 0:
        return False
    if new_text == text:
        return False

    if not dry_run:
        cypher_path.write_text(new_text)
    return True


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("root", type=Path, help="Parent output directory containing one subdir per species")
    parser.add_argument("--dry-run", action="store_true", help="Report what would change without writing files")
    args = parser.parse_args()

    root = args.root
    if not root.is_dir():
        sys.exit(f"error: {root} is not a directory")

    species_dirs = sorted(p for p in root.iterdir() if p.is_dir())
    if not species_dirs:
        sys.exit(f"error: no subdirectories found under {root}")

    total_changed = 0
    total_files = 0
    for sp_dir in species_dirs:
        species = sp_dir.name
        cypher_files = sorted(sp_dir.rglob("*.cypher"))
        if not cypher_files:
            continue
        changed = 0
        for cf in cypher_files:
            total_files += 1
            if fix_file(cf, species, args.dry_run):
                changed += 1
        total_changed += changed
        print(f"[{species}] {changed}/{len(cypher_files)} files updated")

    action = "would update" if args.dry_run else "updated"
    print(f"\nDone. {action} {total_changed}/{total_files} .cypher files under {root}")


if __name__ == "__main__":
    main()
