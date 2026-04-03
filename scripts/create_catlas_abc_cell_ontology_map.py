"""
Build a pickle mapping ABC_scores filename stems → Cell Ontology IDs.

Reads the hand-curated TSV at ./aux_files/hsa/catlas/catlas_abc_cell_type_aliases.tsv
which has three columns:
    abc_stem      — filename stem used in ABC_scores/ (e.g. "Airway Goblet")
    ontology_id   — CL: or UBERON: ID           (e.g. "CL:0002370")
    cre_key       — corresponding cCRE pkl key  (e.g. "Airway_Goblet_Cell")

Output pkl: dict  {abc_stem: "CL:XXXXXXX" or "UBERON:XXXXXXX"}

Usage:
    python scripts/create_catlas_abc_cell_ontology_map.py \\
        ./aux_files/hsa/catlas/catlas_abc_cell_type_aliases.tsv \\
        ./aux_files/hsa/catlas/catlas_abc_cell_ontology_map.pkl
"""

import csv
import pickle
import sys
from pathlib import Path


def build_map(tsv_path: Path) -> dict:
    mapping = {}
    skipped = []

    with open(tsv_path, "rt", encoding="utf-8") as f:
        reader = csv.DictReader(f, delimiter="\t")
        for row in reader:
            abc_stem = (row.get("abc_stem") or "").strip()
            ont_id = (row.get("ontology_id") or "").strip()

            if not abc_stem or not ont_id:
                skipped.append(row)
                continue

            if not (ont_id.startswith("CL:") or ont_id.startswith("UBERON:")):
                print(
                    f"Warning: unexpected ontology ID '{ont_id}' "
                    f"for '{abc_stem}' — skipping"
                )
                continue

            mapping[abc_stem] = ont_id

    if skipped:
        print(f"Warning: {len(skipped)} row(s) skipped (missing abc_stem or ontology_id):")
        for r in skipped:
            print(f"  {r}")

    return mapping


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print(
            "Usage: python create_catlas_abc_cell_ontology_map.py "
            "<aliases.tsv> <output.pkl>"
        )
        sys.exit(1)

    tsv_path = Path(sys.argv[1])
    out_path = Path(sys.argv[2])

    if not tsv_path.exists():
        print(f"Error: {tsv_path} not found")
        sys.exit(1)

    print(f"Reading {tsv_path} ...")
    mapping = build_map(tsv_path)

    cl_count = sum(1 for v in mapping.values() if v.startswith("CL:"))
    uberon_count = sum(1 for v in mapping.values() if v.startswith("UBERON:"))
    print(f"  {len(mapping)} keys mapped  ({cl_count} CL:, {uberon_count} UBERON:)")

    out_path.parent.mkdir(parents=True, exist_ok=True)
    with open(out_path, "wb") as f:
        pickle.dump(mapping, f)

    print(f"Saved to {out_path}")
