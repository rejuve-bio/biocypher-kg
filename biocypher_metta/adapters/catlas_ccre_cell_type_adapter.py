import gzip
import os
import pickle
import subprocess
import sys
import urllib.request
from pathlib import Path
from biocypher_metta.adapters import Adapter
from biocypher_metta.adapters.helpers import build_regulatory_region_id

_SCRIPTS_DIR = Path(__file__).parent.parent.parent / "scripts"
_MASTER_TSV_URL = (
    "https://decoder-genetics.wustl.edu/catlasv1/humanenhancer/data/cCRE_hg38.tsv.gz"
)

# CATLAS cCRE cell-type / tissue accessibility edges.
# https://catlas.org/
#
# Data layout:
#   cCREs/Adipocyte.bed           — BED6, cCREs accessible in each cell type
#   catlas_ccre_label_map.pkl     — {(chr, start_1based, end): "enhancer"|"promoter"}
#                                   produced by scripts/create_catlas_ccre_label_map.py
#   catlas_cell_ontology_map.pkl  — {cell_type_name: "CL:XXXXXXX" | "UBERON:XXXXXXX"}
#                                   produced by scripts/create_catlas_cell_ontology_map.py
#
# BED sample (cCREs/Adipocyte.bed):
#   chr1    9955    10355   cCRE1   .       .
#
# cCRE node ID (via build_regulatory_region_id, BED 0-based start → 1-based closed):
#   {chr}_{start}_{end}_GRCh38   e.g. chr1_9956_10355_GRCh38
#
# Two adapter config entries are used — one per ccre_type:
#   ccre_type=enhancer  yields edges with labels:
#     CL:    enhancer_accessible_in_cell_type  →  cell type node
#     UBERON:enhancer_accessible_in_tissue     →  anatomy node
#   ccre_type=promoter  yields edges with labels:
#     CL:    promoter_accessible_in_cell_type  →  cell type node
#     UBERON:promoter_accessible_in_tissue     →  anatomy node


class CAtlasCCRECellTypeAdapter(Adapter):
    """
    Yields edges linking CATLAS cCRE nodes to Cell Ontology (CL) or UBERON
    anatomy nodes, representing chromatin accessibility per cell type / tissue.

    Run twice via separate config entries:
        ccre_type="enhancer"  →  enhancer_accessible_in_cell_type / _tissue
        ccre_type="promoter"  →  promoter_accessible_in_cell_type / _tissue
    """

    # (ccre_type, ontology_prefix) → edge input_label
    _LABEL_MAP = {
        ("enhancer", "CL"):     "enhancer_accessible_in_cell_type",
        ("enhancer", "UBERON"): "enhancer_accessible_in_tissue",
        ("promoter", "CL"):     "promoter_accessible_in_cell_type",
        ("promoter", "UBERON"): "promoter_accessible_in_tissue",
    }

    def __init__(
        self,
        ccre_label_pkl,
        cres_dirpath,
        cell_ontology_pkl,
        ccre_type,
        write_properties=True,
        add_provenance=True,
        ccre_master_tsv=None,
        cell_ontology_tsv=None,
    ):
        if ccre_type not in ("enhancer", "promoter"):
            raise ValueError(f"ccre_type must be 'enhancer' or 'promoter', got '{ccre_type}'")

        self.ccre_label_pkl = ccre_label_pkl
        self.cres_dirpath = cres_dirpath
        self.cell_ontology_pkl = cell_ontology_pkl
        self.ccre_type = ccre_type
        self.source = "CATLAS"
        self.source_url = "https://catlas.org/"

        self._ensure_ccre_label_pkl(ccre_master_tsv)
        self._ensure_cell_ontology_pkl(cell_ontology_tsv)

        super(CAtlasCCRECellTypeAdapter, self).__init__(write_properties, add_provenance)

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
            print(f"[CAtlasCCRECellTypeAdapter] Downloading master TSV → {ccre_master_tsv} ...")
            os.makedirs(os.path.dirname(os.path.abspath(ccre_master_tsv)), exist_ok=True)
            urllib.request.urlretrieve(_MASTER_TSV_URL, ccre_master_tsv)
        print(f"[CAtlasCCRECellTypeAdapter] Building {self.ccre_label_pkl} from {ccre_master_tsv} ...")
        subprocess.run(
            [sys.executable, str(_SCRIPTS_DIR / "create_catlas_ccre_label_map.py"),
             ccre_master_tsv, self.ccre_label_pkl],
            check=True,
        )

    def _ensure_cell_ontology_pkl(self, cell_ontology_tsv):
        if os.path.exists(self.cell_ontology_pkl):
            return
        if not cell_ontology_tsv:
            raise FileNotFoundError(
                f"cell_ontology_pkl not found: {self.cell_ontology_pkl}\n"
                "Either pre-build it with:\n"
                f"  python scripts/create_catlas_cell_ontology_map.py <Cell_ontology.tsv> {self.cell_ontology_pkl}\n"
                "Or pass cell_ontology_tsv= in the adapter config to auto-generate it."
            )
        print(
            f"[CAtlasCCRECellTypeAdapter] Building {self.cell_ontology_pkl} "
            f"from {cell_ontology_tsv} ..."
        )
        subprocess.run(
            [sys.executable, str(_SCRIPTS_DIR / "create_catlas_cell_ontology_map.py"),
             cell_ontology_tsv, self.cell_ontology_pkl],
            check=True,
        )

    # ------------------------------------------------------------------ helpers

    def _open(self, filepath):
        if filepath.endswith(".gz"):
            return gzip.open(filepath, "rt")
        return open(filepath, "rt")

    @staticmethod
    def _ont_prefix(ont_id):
        """Return 'CL' or 'UBERON' from an ID like 'CL:0000136', else None."""
        if ont_id.startswith("CL:"):
            return "CL"
        if ont_id.startswith("UBERON:"):
            return "UBERON"
        return None

    def _bed_files(self):
        for fname in os.listdir(self.cres_dirpath):
            if fname.endswith(".bed.gz"):
                cell_type = fname[: -len(".bed.gz")]
            elif fname.endswith(".bed"):
                cell_type = fname[: -len(".bed")]
            else:
                continue
            yield cell_type, os.path.join(self.cres_dirpath, fname)

    # ------------------------------------------------------------------ interface

    def get_nodes(self):
        pass

    def get_edges(self):
        with open(self.ccre_label_pkl, "rb") as f:
            coord_map = pickle.load(f)

        with open(self.cell_ontology_pkl, "rb") as f:
            cell_ontology_map = pickle.load(f)

        for cell_type, filepath in self._bed_files():
            ont_id = cell_ontology_map.get(cell_type)
            if ont_id is None:
                continue

            ont_prefix = self._ont_prefix(ont_id)
            if ont_prefix is None:
                continue

            edge_label = self._LABEL_MAP.get((self.ccre_type, ont_prefix))
            if edge_label is None:
                continue

            with self._open(filepath) as fh:
                for line in fh:
                    line = line.strip()
                    if not line or line.startswith("#"):
                        continue

                    fields = line.split("\t")
                    if len(fields) < 3:
                        continue

                    chrom = fields[0]
                    try:
                        start = int(fields[1]) + 1  # BED 0-based → 1-based closed
                        end = int(fields[2])
                    except ValueError:
                        continue

                    ccre_label = coord_map.get((chrom, start, end))
                    if ccre_label != self.ccre_type:
                        continue

                    ccre_id = build_regulatory_region_id(chrom, start, end)

                    props = {}
                    if self.write_properties:
                        if self.add_provenance:
                            props["source"] = self.source
                            props["source_url"] = self.source_url

                    yield ccre_id, ont_id, edge_label, props
