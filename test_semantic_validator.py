"""
Test the semantic_validator against two cases:
  Case A — a correct adapter (bgee/gene-anatomy), should PASS
  Case B — a deliberately broken adapter (wrong column for source_id), should WARN/FAIL

Run from the project root:
    python test_semantic_validator.py
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from schema_generator.semantic_validator import validate_semantic_correctness, print_semantic_report

# ─────────────────────────────────────────────────────────────────────────────
# Shared: mock inspection with realistic sample rows from a gene-expression file
# Columns: gene_id, anatomy_id, score, rank
# ─────────────────────────────────────────────────────────────────────────────
MOCK_INSPECTION = {
    "main_file": {
        "metadata": {
            "filepath": "bgee_expr.tsv.gz",
            "delimiter": "\t",
            "has_header": True,
            "compression": "gzip",
        },
        "headers": ["gene_id", "anatomy_id", "score", "rank"],
        "sample_rows": [
            ["ENSG00000139618", "UBERON:0002048", "0.87", "1"],
            ["ENSG00000141510", "UBERON:0002107", "0.91", "2"],
            ["ENSG00000012048", "UBERON:0000955", "0.73", "3"],
        ],
    }
}

# ─────────────────────────────────────────────────────────────────────────────
# Shared spec: gene → expressed_in → anatomy
# ─────────────────────────────────────────────────────────────────────────────
MOCK_SPEC = {
    "analysis": {
        "purpose": "Build gene-to-anatomy expression edges from Bgee dataset",
        "logic_interpretation": "Each row is a gene expressed in an anatomy term with a confidence score.",
    },
    "data_format": {
        "delimiter": "\t",
        "has_header": True,
        "compression": "gzip",
    },
    "relationships": [
        {
            "name": "gene_expressed_in_anatomy",
            "source": "gene",
            "source_column": 0,          # col 0 = gene_id (ENSG...)
            "target": "anatomy",
            "target_column": 1,          # col 1 = anatomy_id (UBERON:...)
            "input_label": "expressed_in",
            "properties": {"score": "float", "rank": "int"},
        }
    ],
}

# ─────────────────────────────────────────────────────────────────────────────
# Case A: CORRECT adapter — reads row[0] for gene, row[1] for anatomy
# ─────────────────────────────────────────────────────────────────────────────
CORRECT_CODE = '''
import csv
import gzip
from biocypher_metta.adapters import Adapter

class BgeeAdapter(Adapter):
    def __init__(self, filepath, label, write_properties, add_provenance):
        super().__init__(write_properties=write_properties, add_provenance=add_provenance)
        self.filepath = filepath
        self.label = label
        self.source = "Bgee"
        self.source_url = "https://bgee.org"

    def get_edges(self):
        open_fn = gzip.open if self.filepath.endswith(".gz") else open
        with open_fn(self.filepath, "rt") as f:
            reader = csv.reader(f, delimiter="\\t")
            next(reader)  # skip header
            for row in reader:
                if len(row) < 4:
                    continue
                source_id = row[0]  # gene_id  (ENSG...)
                target_id = row[1].replace(":", "_")  # anatomy_id (UBERON:...)
                if not source_id or not target_id:
                    continue
                props = {}
                if self.write_properties:
                    try:
                        props["score"] = float(row[2]) if row[2] not in (".", "NA", "") else None
                    except ValueError:
                        props["score"] = None
                    props["rank"] = int(row[3]) if row[3].isdigit() else None
                    if self.add_provenance:
                        props["source"] = self.source
                        props["source_url"] = self.source_url
                yield source_id, ("anatomy", target_id), self.label, props

    def get_nodes(self):
        pass
'''

# ─────────────────────────────────────────────────────────────────────────────
# Case B: BROKEN adapter — reads row[2] (score column) as gene ID  ← wrong!
# ─────────────────────────────────────────────────────────────────────────────
BROKEN_CODE = '''
import csv
import gzip
from biocypher_metta.adapters import Adapter

class BgeeAdapter(Adapter):
    def __init__(self, filepath, label, write_properties, add_provenance):
        super().__init__(write_properties=write_properties, add_provenance=add_provenance)
        self.filepath = filepath
        self.label = label
        self.source = "Bgee"
        self.source_url = "https://bgee.org"

    def get_edges(self):
        open_fn = gzip.open if self.filepath.endswith(".gz") else open
        with open_fn(self.filepath, "rt") as f:
            reader = csv.reader(f, delimiter="\\t")
            next(reader)  # skip header
            for row in reader:
                if len(row) < 4:
                    continue
                # BUG: using score column (2) as gene ID instead of column 0
                source_id = row[2]
                target_id = row[3]
                if not source_id or not target_id:
                    continue
                props = {}
                if self.write_properties:
                    props["score"] = row[0]   # also wrong
                yield source_id, ("anatomy", target_id), self.label, props

    def get_nodes(self):
        pass
'''


def run_case(label: str, code: str, spec: dict, inspection: dict, skip_llm: bool = True):
    print("=" * 65)
    print(f"  {label}")
    print("=" * 65)
    report = validate_semantic_correctness(code, spec, inspection, skip_llm=skip_llm)
    print_semantic_report(report)
    return report


if __name__ == "__main__":
    use_llm = "--with-llm" in sys.argv
    if not use_llm:
        print("[ Running layers 1 & 2 only — pass --with-llm to include LLM review ]\n")

    report_a = run_case(
        "CASE A — correct adapter (should PASS)",
        CORRECT_CODE, MOCK_SPEC, MOCK_INSPECTION, skip_llm=not use_llm,
    )
    report_b = run_case(
        "CASE B — broken adapter, wrong columns (should WARN/FAIL)",
        BROKEN_CODE, MOCK_SPEC, MOCK_INSPECTION, skip_llm=not use_llm,
    )

    print("\n── Final verdicts ──────────────────────────────────────────")
    print(f"  Case A: {report_a['overall_verdict'].upper()}")
    print(f"  Case B: {report_b['overall_verdict'].upper()}")

    if report_a["overall_verdict"] in ("pass", "warning") and report_b["overall_verdict"] != "pass":
        print("\n[+] Semantic validator is working correctly.")
    else:
        print("\n[!] Unexpected results — review output above.")
