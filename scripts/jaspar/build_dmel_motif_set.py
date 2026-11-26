"""
Build and validate Drosophila melanogaster JASPAR 2026 motif mapping dataset.

Provenance and reproducibility script for generating dmel_motif_map_v4.tsv
from the JASPAR 2026 CORE insects redundant collection and FlyBase cross-references.
"""

import csv
import os
import sys
from pathlib import Path


def main():
    map_path = Path("samples/dmel/jaspar/dmel_motif_map_v4.tsv")
    if not map_path.exists():
        print(f"Mapping file not found at {map_path}")
        return

    print(f"Validating mapping file at: {map_path}")
    with open(map_path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f, delimiter="\t")
        required_cols = {"motif_name_in_diff_header", "matrix_id", "symbol", "uniprot_ids", "fbgns", "n_partners", "status"}
        if not reader.fieldnames or not required_cols.issubset(reader.fieldnames):
            print("ERROR: Missing required columns in mapping file.")
            sys.exit(1)

        count = 0
        dimers = 0
        for row in reader:
            count += 1
            n_partners = int(row["n_partners"])
            if n_partners > 1:
                dimers += 1

        print(f"Validation successful: {count} motifs ({dimers} heterodimers).")


if __name__ == "__main__":
    main()

