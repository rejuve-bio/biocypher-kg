"""
Create a pickle mapping CATLAS cell type names → Cell Ontology IDs.

Keys are normalised to match the corresponding .bed filename stems in the
CATLAS cCRE directory (spaces→_, parens removed, commas preserved).

Output pkl: dict  {bed_file_stem: "CL:XXXXXXX" or "UBERON:XXXXXXX"}

Usage:
    python scripts/create_catlas_cell_ontology_map.py \
        /path/to/CATLAS/Cell_ontology.tsv \
        aux_files/hsa/catlas_cell_ontology_map.pkl \
        [/path/to/ccre_dir]   # optional: warn about unmatched bed files
"""

import csv
import pickle
import sys
from pathlib import Path
from typing import Optional

# Filename typos in the cCRE directory that differ from the TSV spelling.
# Maps the normalised TSV key → the actual bed file stem.
FILENAME_TYPOS = {
    "Glutamatergic_Neuron_1": "Glutaminergic_Neuron_1",
    "Glutamatergic_Neuron_2": "Glutaminergic_Neuron_2",
    "Fetal_Ventricular_Cardiomyocyte": "Fetal_Ventricular_Cardioyocyte",
}


def clean_cell_type(name: str) -> str:
    """
    Normalise a cell-type name to match the .bed filename stem.

    Rules (derived from inspecting the cCRE directory):
      - Strip leading/trailing whitespace
      - Remove parentheses
      - Replace spaces with underscores
      - Commas are KEPT (e.g. 'Pancreatic_Delta,Gamma_cell')
    """
    return (
        name.strip()
        .replace("(", "")
        .replace(")", "")
        .replace(" ", "_")
        # NOTE: commas intentionally NOT replaced — filenames keep them
    )


def pick_id(cl_id_field: str) -> Optional[str]:
    """
    Return the best ontology ID from a (possibly comma-separated) field.

    Priority: first CL: term > first UBERON: term > None

    Splitting on commas here is safe because ontology IDs never contain
    commas — unlike cell-type names which may.
    """
    tokens = [t.strip() for t in cl_id_field.replace(";", ",").split(",")]

    cl_hit = next((t for t in tokens if t.startswith("CL:")), None)
    if cl_hit:
        return cl_hit

    return next((t for t in tokens if t.startswith("UBERON:")), None)


def build_map(tsv_path: Path) -> dict:
    mapping = {}
    skipped = []

    with open(tsv_path, "rt", encoding="utf-8") as f:
        reader = csv.DictReader(f, delimiter="\t")
        for row in reader:
            raw_name = row.get("Cell type") or ""
            cl_id_field = (row.get("Cell Ontology ID") or "").strip()

            cell_type = clean_cell_type(raw_name)

            if not cell_type or not cl_id_field:
                skipped.append(row)
                continue

            ont_id = pick_id(cl_id_field)

            if not ont_id:
                print(
                    f"Warning: no CL:/UBERON: ID found in '{cl_id_field}' "
                    f"for cell type '{cell_type}' — skipping"
                )
                continue

            # Store under the normalised TSV key
            mapping[cell_type] = ont_id

            # Also store under the typo'd filename stem so lookups work
            # against actual bed files without extra munging at query time
            if cell_type in FILENAME_TYPOS:
                alias = FILENAME_TYPOS[cell_type]
                mapping[alias] = ont_id
                print(f"Info: added filename alias  '{cell_type}' → '{alias}'")

    if skipped:
        print(f"Warning: {len(skipped)} row(s) skipped (missing cell type or ID):")
        for r in skipped:
            print(f"  {r}")

    return mapping


def check_bed_coverage(mapping: dict, ccre_dir: Path) -> None:
    """Warn about any .bed file whose stem is not a key in the mapping."""
    bed_stems = {p.stem for p in ccre_dir.glob("*.bed")}
    missing = sorted(bed_stems - mapping.keys())
    extra = sorted(mapping.keys() - bed_stems)

    if missing:
        print(f"\n{len(missing)} .bed file(s) have NO entry in the mapping:")
        for s in missing:
            print(f"  {s}")
    else:
        print("\nAll .bed files are covered by the mapping ✓")

    if extra:
        print(f"\n{len(extra)} mapping key(s) have no corresponding .bed file:")
        for s in extra:
            print(f"  {s}")


if __name__ == "__main__":
    if len(sys.argv) not in (3, 4):
        print(
            "Usage: python create_catlas_cell_ontology_map.py "
            "<Cell_ontology.tsv> <output.pkl> [ccre_dir]"
        )
        sys.exit(1)

    tsv_path = Path(sys.argv[1])
    out_path = Path(sys.argv[2])
    ccre_dir = Path(sys.argv[3]) if len(sys.argv) == 4 else None

    if not tsv_path.exists():
        print(f"Error: {tsv_path} not found")
        sys.exit(1)

    print(f"Reading {tsv_path} ...")
    mapping = build_map(tsv_path)

    cl_count = sum(1 for v in mapping.values() if v.startswith("CL:"))
    uberon_count = sum(1 for v in mapping.values() if v.startswith("UBERON:"))
    print(f"  {len(mapping)} keys mapped  ({cl_count} CL:, {uberon_count} UBERON:)")

    if ccre_dir:
        if ccre_dir.is_dir():
            check_bed_coverage(mapping, ccre_dir)
        else:
            print(f"Warning: ccre_dir '{ccre_dir}' not found, skipping coverage check")

    out_path.parent.mkdir(parents=True, exist_ok=True)
    with open(out_path, "wb") as f:
        pickle.dump(mapping, f)

    print(f"\nSaved to {out_path}")