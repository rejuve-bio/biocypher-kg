"""
Version Comparison Tool for BioCypher Archives
Supports Neo4j (CSV) and MORK (MeTTa) databases
"""

import argparse
import csv
import json
import sys
from pathlib import Path
from typing import Dict, Set, List



def read_csv_ids(csv_file: Path) -> Set[str]:
    """Read IDs from CSV file (first column)."""
    ids = set()
    try:
        with open(csv_file, 'r', encoding='utf-8') as f:
            reader = csv.reader(f)
            header = next(reader, None)
            for row in reader:
                if row and len(row) > 0:
                    ids.add(row[0])
    except Exception as e:
        print(f"Error reading {csv_file}: {e}", file=sys.stderr)
    return ids


def read_csv_rows(csv_file: Path) -> Dict[str, Dict]:
    """Read full rows from CSV file indexed by ID."""
    rows = {}
    try:
        with open(csv_file, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                if row:
                    row_id = row.get(reader.fieldnames[0])
                    if row_id:
                        rows[row_id] = dict(row)
    except Exception as e:
        print(f"Error reading {csv_file}: {e}", file=sys.stderr)
    return rows


def read_metta_atoms(metta_file: Path) -> Set[str]:
    """Read atoms from MeTTa file (each line is an atom)."""
    atoms = set()
    try:
        with open(metta_file, 'r', encoding='utf-8') as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith(';'):
                    atoms.add(line)
    except Exception as e:
        print(f"Error reading {metta_file}: {e}", file=sys.stderr)
    return atoms


def compare_dataset(archive_dir: Path, dataset: str, version1: str, version2: str, db_type: str = "neo4j") -> Dict:
    """Compare a single dataset between two versions."""
    base_dir = archive_dir / db_type
    v1_dir = base_dir / dataset / version1
    v2_dir = base_dir / dataset / version2
    
    if not v1_dir.exists():
        return {"error": f"Version {version1} not found"}
    if not v2_dir.exists():
        return {"error": f"Version {version2} not found"}
    
    results = {"dataset": dataset, "db_type": db_type, "version1": version1, "version2": version2, "files": {}}
    
    if db_type == "neo4j":
        v1_csvs = list(v1_dir.rglob("*.csv"))
        v2_csvs = list(v2_dir.rglob("*.csv"))
        
        for v1_csv in v1_csvs:
            relative_path = v1_csv.relative_to(v1_dir)
            v2_csv = v2_dir / relative_path
            
            if not v2_csv.exists():
                results["files"][str(relative_path)] = {"status": "deleted"}
                continue
            
            v1_ids = read_csv_ids(v1_csv)
            v2_ids = read_csv_ids(v2_csv)
            v1_rows = read_csv_rows(v1_csv)
            v2_rows = read_csv_rows(v2_csv)
            
            added = v2_ids - v1_ids
            deleted = v1_ids - v2_ids
            common = v1_ids & v2_ids
            
            modified = []
            modified_details = []
            for id in common:
                if v1_rows.get(id) != v2_rows.get(id):
                    modified.append(id)
                    modified_details.append({"id": id, "old": v1_rows.get(id), "new": v2_rows.get(id)})
            
            added_records = [v2_rows.get(id) for id in list(added)[:20]]
            deleted_records = [v1_rows.get(id) for id in list(deleted)[:20]]
            
            results["files"][str(relative_path)] = {
                "status": "changed", "v1_count": len(v1_ids), "v2_count": len(v2_ids),
                "added": len(added), "deleted": len(deleted), "modified": len(modified),
                "added_records": added_records, "deleted_records": deleted_records,
                "modified_details": modified_details[:20]
            }
        
        for v2_csv in v2_csvs:
            relative_path = v2_csv.relative_to(v2_dir)
            v1_csv = v1_dir / relative_path
            if not v1_csv.exists():
                v2_ids = read_csv_ids(v2_csv)
                v2_rows = read_csv_rows(v2_csv)
                added_records = [v2_rows.get(id) for id in list(v2_ids)[:20]]
                results["files"][str(relative_path)] = {
                    "status": "added", 
                    "v2_count": len(v2_ids),
                    "added": len(v2_ids),  
                    "added_records": added_records
                }
    elif db_type == "mork":
        v1_mettas = list(v1_dir.rglob("*.metta"))
        v2_mettas = list(v2_dir.rglob("*.metta"))
        
        for v1_metta in v1_mettas:
            relative_path = v1_metta.relative_to(v1_dir)
            v2_metta = v2_dir / relative_path
            
            if not v2_metta.exists():
                results["files"][str(relative_path)] = {"status": "deleted"}
                continue
            
            v1_atoms = read_metta_atoms(v1_metta)
            v2_atoms = read_metta_atoms(v2_metta)
            
            added = v2_atoms - v1_atoms
            deleted = v1_atoms - v2_atoms
            
            results["files"][str(relative_path)] = {
                "status": "changed", "v1_count": len(v1_atoms), "v2_count": len(v2_atoms),
                "added": len(added), "deleted": len(deleted),
                "added_atoms": list(added)[:20], "deleted_atoms": list(deleted)[:20]
            }
        
        for v2_metta in v2_mettas:
            relative_path = v2_metta.relative_to(v2_dir)
            v1_metta = v1_dir / relative_path
            if not v1_metta.exists():
                v2_atoms = read_metta_atoms(v2_metta)
                results["files"][str(relative_path)] = {"status": "added", "v2_count": len(v2_atoms),"added": len(v2_atoms),"added_atoms": list(v2_atoms)[:20]}
    
    return results


def compare_all_datasets(archive_dir: Path, version1: str, version2: str, db_type: str = "neo4j") -> Dict:
    """Compare all datasets between two versions."""
    base_dir = archive_dir / db_type
    if not base_dir.exists():
        return {"error": f"Archive directory not found: {base_dir}"}
    
    results = {"version1": version1, "version2": version2, "db_type": db_type, "datasets": {}}
    
    for dataset_dir in sorted(base_dir.iterdir()):
        if not dataset_dir.is_dir():
            continue
        dataset_name = dataset_dir.name
        print(f"\nComparing [{dataset_name}]...", file=sys.stderr)
        results["datasets"][dataset_name] = compare_dataset(archive_dir, dataset_name, version1, version2, db_type)
    
    return results


def main():
    parser = argparse.ArgumentParser(description="Compare BioCypher archives between versions")
    parser.add_argument("--archive-dir", default="/tmp/biocypher-archives", help="Archive base directory")
    parser.add_argument("--from", dest="version1", required=True, help="First version (e.g., v1)")
    parser.add_argument("--to", dest="version2", required=True, help="Second version (e.g., v2)")
    parser.add_argument("--db-type", default="neo4j", choices=["neo4j", "mork"], help="Database type")
    parser.add_argument("--dataset", help="Compare specific dataset only")
    parser.add_argument("--json", action="store_true", help="Output as JSON")
    args = parser.parse_args()
    
    archive_dir = Path(args.archive_dir)
    
    if args.dataset:
        result = compare_dataset(archive_dir, args.dataset, args.version1, args.version2, args.db_type)
        results = {"datasets": {args.dataset: result}}
    else:
        results = compare_all_datasets(archive_dir, args.version1, args.version2, args.db_type)
    
    if args.json:
        print(json.dumps(results, indent=2))
    else:
        print(f"\n{'='*60}\nCOMPARISON: {args.version1} → {args.version2} ({args.db_type.upper()})\n{'='*60}\n")
        for dataset, data in results.get("datasets", {}).items():
            if "error" in data:
                print(f"[{dataset}]: {data['error']}")
                continue
            total_added = sum(f.get("added", 0) for f in data.get("files", {}).values())
            total_deleted = sum(f.get("deleted", 0) for f in data.get("files", {}).values())
            total_modified = sum(f.get("modified", 0) for f in data.get("files", {}).values())
            if total_added or total_deleted or total_modified:
                print(f"[{dataset}]: ✅ {total_added} added, ❌ {total_deleted} deleted" + (f", ✏️ {total_modified} modified" if total_modified else ""))

if __name__ == "__main__":
    main()