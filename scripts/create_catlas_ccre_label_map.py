"""
Create a pickle mapping CATLAS cCRE coordinates → label (enhancer / promoter).

Reads cCRE_hg38.tsv[.gz] from the CATLAS master catalog:
    #Chromosome  hg38_Start  hg38_End  Class  ...
    chr1         9955        10355     Promoter Proximal  ...

Output pkl: dict  {(chr, start_1based, end_1based): "enhancer" | "promoter"}

  - Coordinates are normalised from BED (0-based start) to 1-based closed to
    match the node IDs produced by CAtlasCCREAdapter.
  - Class mapping:
      Distal               → enhancer
      Promoter             → promoter
      Promoter Proximal    → promoter

Used by:
    CAtlasCCRECellTypeAdapter   (accessible_in edges)
    CAtlasABCScoreAdapter       (activity_by_contact edges)

Download the master file (~60 MB):
    wget https://decoder-genetics.wustl.edu/catlasv1/humanenhancer/data/cCRE_hg38.tsv.gz \\
         -O samples/hsa/catlasv1/cCRE_hg38.tsv.gz

Usage:
    python scripts/create_catlas_ccre_label_map.py \\
        samples/hsa/catlasv1/cCRE_hg38.tsv.gz \\
        aux_files/hsa/catlas/catlas_ccre_label_map.pkl
"""

MASTER_TSV_URL = (
    "https://decoder-genetics.wustl.edu/catlasv1/humanenhancer/data/cCRE_hg38.tsv.gz"
)

import csv
import gzip
import pickle
import sys
from pathlib import Path

_PROMOTER_CLASSES = {"promoter", "promoter proximal"}
_ENHANCER_CLASSES = {"distal"}


def _class_to_label(cls_value: str):
    cls = cls_value.lower().strip()
    if cls in _ENHANCER_CLASSES:
        return "enhancer"
    if cls in _PROMOTER_CLASSES:
        return "promoter"
    return None


def _open(filepath: Path):
    if str(filepath).endswith(".gz"):
        return gzip.open(filepath, "rt")
    return open(filepath, "rt")


def build_map(tsv_path: Path) -> dict:
    mapping = {}
    skipped_unknown_class = 0

    with _open(tsv_path) as fh:
        reader = csv.DictReader(fh, delimiter="\t")
        for row in reader:
            chrom = row.get("#Chromosome") or row.get("Chromosome")
            cls = (row.get("Class") or "").strip()
            start_raw = row.get("hg38_Start")
            end_raw = row.get("hg38_End")

            if not chrom or not cls or start_raw is None or end_raw is None:
                continue

            label = _class_to_label(cls)
            if label is None:
                skipped_unknown_class += 1
                continue

            try:
                start = int(start_raw) + 1  # BED 0-based → 1-based closed
                end = int(end_raw)
            except ValueError:
                continue

            mapping[(chrom, start, end)] = label

    if skipped_unknown_class:
        print(f"Warning: {skipped_unknown_class} row(s) skipped (unrecognised Class value)")

    return mapping


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python create_catlas_ccre_label_map.py <cCRE_hg38.tsv[.gz]> <output.pkl>")
        sys.exit(1)

    tsv_path = Path(sys.argv[1])
    out_path = Path(sys.argv[2])

    if not tsv_path.exists():
        print(f"Error: {tsv_path} not found")
        sys.exit(1)

    print(f"Reading {tsv_path} ...")
    mapping = build_map(tsv_path)

    enhancer_count = sum(1 for v in mapping.values() if v == "enhancer")
    promoter_count = sum(1 for v in mapping.values() if v == "promoter")
    print(f"  {len(mapping):,} cCREs mapped  ({enhancer_count:,} enhancer, {promoter_count:,} promoter)")

    out_path.parent.mkdir(parents=True, exist_ok=True)
    with open(out_path, "wb") as f:
        pickle.dump(mapping, f)

    print(f"Saved to {out_path}")
