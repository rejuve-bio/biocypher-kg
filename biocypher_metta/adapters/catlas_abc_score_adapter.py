import csv
import gzip
import os
import pickle
import subprocess
import sys
import urllib.request
from pathlib import Path
from biocypher_metta.adapters import Adapter
from biocypher_metta.adapters.helpers import build_regulatory_region_id, to_float

_SCRIPTS_DIR = Path(__file__).parent.parent.parent / "scripts"
_MASTER_TSV_URL = (
    "https://decoder-genetics.wustl.edu/catlasv1/humanenhancer/data/cCRE_hg38.tsv.gz"
)

# ABC (Activity-By-Contact) model cCRE-to-gene linkage predictions from CATLAS.
# https://catlas.org/
#
# Each file in ABC_scores/ corresponds to a cell type (e.g., Adipocyte.tsv.gz).
# Columns: cCRE, Promoter, ABC_score, Gene Name, Distance
#
# cCRE field: "chr10:100006303-100006703"  (0-based start, as in BED)
# Gene Name:  "CHUK:ENST00000370397.8"     (symbol:transcript_id)
#
# Pre-built pkls required:
#   catlas_ccre_label_map.pkl   — {(chr, start_1based, end): "enhancer"|"promoter"}
#                                 produced by scripts/create_catlas_ccre_label_map.py
#   hgnc_mapping.pkl (gzipped)  — {symbol_to_ensembl: {symbol: ENSG_ID, ...}, ...}
#   catlas_cell_ontology_map.pkl — {cell_type_name: "CL:XXXXXXX"}  (optional)
#
# Edge label is chosen from the cCRE class:
#   enhancer cCRE  →  enhancer_activity_by_contact
#   promoter cCRE  →  promoter_activity_by_contact
#
# Rows with no ENSG mapping are skipped.


class CAtlasABCScoreAdapter(Adapter):
    """
    Edge adapter linking CATLAS cCRE regions to genes using ABC model scores.

    Yields edges: (ccre_id, gene_id, label, props)
      - ccre_id  : CATLAS:cCRE:{chr}:{start}-{end}:{enhancer|promoter}
      - label    : enhancer_activity_by_contact  OR  promoter_activity_by_contact
      - gene_id  : ENSEMBL:{ENSG_ID}
      - props    : abc_score, distance, cell_type, biological_context (CL ID)
    """

    _LABEL_MAP = {
        "enhancer": "enhancer_activity_by_contact",
        "promoter": "promoter_activity_by_contact",
    }

    def __init__(
        self,
        dirpath,
        ccre_label_pkl,
        hgnc_mapping_pkl,
        write_properties,
        add_provenance,
        cell_ontology_pkl,
        score_threshold=None,
        ccre_master_tsv=None,
        abc_aliases_tsv=None,
    ):
        self.dirpath = dirpath
        self.ccre_label_pkl = ccre_label_pkl
        self.hgnc_mapping_pkl = hgnc_mapping_pkl
        self.cell_ontology_pkl = cell_ontology_pkl
        self.score_threshold = score_threshold
        self.source = "CATLAS"
        self.source_url = "https://catlas.org/"

        self._symbol_to_ensembl = None    # lazy-loaded
        self._cell_ontology_map = None    # lazy-loaded

        self._ensure_ccre_label_pkl(ccre_master_tsv)
        self._ensure_cell_ontology_pkl(abc_aliases_tsv)

        super(CAtlasABCScoreAdapter, self).__init__(write_properties, add_provenance)

    def _ensure_ccre_label_pkl(self, ccre_master_tsv):
        if os.path.exists(self.ccre_label_pkl):
            return
        if not ccre_master_tsv:
            raise FileNotFoundError(
                f"ccre_label_pkl not found: {self.ccre_label_pkl}\n"
                "Either pre-build it with:\n"
                f"  python scripts/create_catlas_ccre_label_map.py <cCRE_hg38.tsv.gz> {self.ccre_label_pkl}\n"
                "Or pass ccre_master_tsv= in the adapter config to auto-generate it."
            )
        if not os.path.exists(ccre_master_tsv):
            print(f"[CAtlasABCScoreAdapter] Downloading master TSV → {ccre_master_tsv} ...")
            os.makedirs(os.path.dirname(os.path.abspath(ccre_master_tsv)), exist_ok=True)
            urllib.request.urlretrieve(_MASTER_TSV_URL, ccre_master_tsv)
        print(f"[CAtlasABCScoreAdapter] Building {self.ccre_label_pkl} from {ccre_master_tsv} ...")
        subprocess.run(
            [sys.executable, str(_SCRIPTS_DIR / "create_catlas_ccre_label_map.py"),
             ccre_master_tsv, self.ccre_label_pkl],
            check=True,
        )

    def _ensure_cell_ontology_pkl(self, abc_aliases_tsv):
        if os.path.exists(self.cell_ontology_pkl):
            return
        if not abc_aliases_tsv:
            raise FileNotFoundError(
                f"cell_ontology_pkl not found: {self.cell_ontology_pkl}\n"
                "Either pre-build it with:\n"
                f"  python scripts/create_catlas_abc_cell_ontology_map.py <aliases.tsv> {self.cell_ontology_pkl}\n"
                "Or pass abc_aliases_tsv= in the adapter config to auto-generate it."
            )
        print(f"[CAtlasABCScoreAdapter] Building {self.cell_ontology_pkl} from {abc_aliases_tsv} ...")
        subprocess.run(
            [sys.executable, str(_SCRIPTS_DIR / "create_catlas_abc_cell_ontology_map.py"),
             abc_aliases_tsv, self.cell_ontology_pkl],
            check=True,
        )

    # ------------------------------------------------------------------ helpers

    def _open(self, filepath):
        if filepath.endswith(".gz"):
            return gzip.open(filepath, "rt")
        return open(filepath, "rt")

    def _get_symbol_to_ensembl(self):
        if self._symbol_to_ensembl is None:
            with gzip.open(self.hgnc_mapping_pkl, "rb") as f:
                hgnc_data = pickle.load(f)
            self._symbol_to_ensembl = hgnc_data.get("symbol_to_ensembl", {})
        return self._symbol_to_ensembl

    def _get_cell_ontology_map(self):
        if self._cell_ontology_map is None:
            if self.cell_ontology_pkl:
                with open(self.cell_ontology_pkl, "rb") as f:
                    self._cell_ontology_map = pickle.load(f)
            else:
                self._cell_ontology_map = {}
        return self._cell_ontology_map

    def _cell_type_files(self):
        # split by dot then take the first part as cell type (e.g., "Adipocyte" from "Adipocyte.tsv.gz")

        for fname in os.listdir(self.dirpath):
            cell_type = fname.split(".", 1)[0]
            # if fname.endswith(".tsv.gz"):
            #     cell_type = fname[: -len(".tsv.gz")]
            # elif fname.endswith(".tsv"):
            #     cell_type = fname[: -len(".tsv")]
            # else:
            #     continue
            yield cell_type, os.path.join(self.dirpath, fname)

    @staticmethod
    def _parse_ccre(ccre_str):
        """Parse 'chrN:start-end' (0-based start) → (chr, start_1based, end)."""
        try:
            chrom, coords = ccre_str.split(":", 1)
            start_str, end_str = coords.split("-", 1)
            # ABC cCRE coordinates match master file (0-based start in storage)
            start = int(start_str) + 1  # → 1-based closed to match node IDs
            end = int(end_str)
            return chrom, start, end
        except (ValueError, AttributeError):
            return None

    @staticmethod
    def _parse_gene_symbol(gene_name_field):
        """Extract gene symbol from 'SYMBOL:ENST...' field."""
        return gene_name_field.split(":")[0].strip()

    # ------------------------------------------------------------------ interface

    def get_nodes(self):
        pass

    def get_edges(self):
        with open(self.ccre_label_pkl, "rb") as f:
            coord_map = pickle.load(f)
        symbol_to_ensembl = self._get_symbol_to_ensembl()
        cell_ontology_map = self._get_cell_ontology_map()

        for cell_type, filepath in self._cell_type_files():
            biological_context = cell_ontology_map.get(cell_type)

            with self._open(filepath) as handle:
                reader = csv.DictReader(handle, delimiter="\t")
                for row in reader:
                    ccre_str = (row.get("cCRE") or "").strip()
                    gene_name_field = (row.get("Gene Name") or "").strip()
                    abc_score_str = (row.get("ABC_score") or "").strip()
                    distance_str = (row.get("Distance") or "").strip()

                    if not ccre_str or not gene_name_field or not abc_score_str:
                        continue

                    parsed = self._parse_ccre(ccre_str)
                    if parsed is None:
                        continue
                    chrom, start, end = parsed

                    ccre_label = coord_map.get((chrom, start, end))
                    if ccre_label is None:
                        continue

                    try:
                        abc_score = to_float(abc_score_str)
                    except ValueError:
                        continue

                    if self.score_threshold is not None and abc_score < self.score_threshold:
                        continue

                    gene_symbol = self._parse_gene_symbol(gene_name_field)
                    if not gene_symbol:
                        continue

                    ensg_id = symbol_to_ensembl.get(gene_symbol)
                    if not ensg_id:
                        continue  # skip genes with no ENSG mapping

                    try:
                        distance = int(distance_str)
                    except ValueError:
                        distance = None

                    # ccre_id = f"CATLAS:cCRE:{chrom}:{start}-{end}:{ccre_label}"
                    ccre_id = build_regulatory_region_id(chrom, start, end)
                    gene_id = f"ENSEMBL:{ensg_id}"
                    edge_label = self._LABEL_MAP[ccre_label]

                    props = {}
                    if self.write_properties:
                        props["abc_score"] = abc_score
                        props["cell_type"] = cell_type
                        props["distance"] = distance
                        props["biological_context"] = biological_context

                        if self.add_provenance:
                            props["source"] = self.source
                            props["source_url"] = self.source_url

                    yield ccre_id, gene_id, edge_label, props
