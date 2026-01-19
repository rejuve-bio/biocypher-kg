import gzip
import pickle
from typing import Dict
from typing import Optional

from biocypher_metta.adapters import Adapter


def _open_text(path: str):
    if path.endswith(".gz"):
        return gzip.open(path, "rt")
    return open(path, "r")


def _load_pickle_map(path: str) -> Dict:
    with open(path, "rb") as fh:
        data = pickle.load(fh)
    if not isinstance(data, dict):
        raise TypeError(f"Expected dict in pickle map at {path}, got {type(data)}")
    return data


def _strip_curie_prefix(value: str, prefix: str) -> str:
    if value.startswith(prefix):
        return value[len(prefix) :]
    return value


class HPOGeneDiseaseAdapter(Adapter):
    """Adapter for HPO genes_to_disease.txt.

    Produces gene->disease associations (e.g., OMIM, ORPHA).
    """

    def __init__(
        self,
        filepath: str,
        entrez_to_ensembl_map: str,
        write_properties: bool,
        add_provenance: bool,
        label: str = "gene_disease",
    ):
        self.filepath = filepath
        self.label = label
        self.source = "Human Phenotype Ontology"
        self.source_url = "https://hpo.jax.org/"
        self.entrez_to_ensembl = _load_pickle_map(entrez_to_ensembl_map)
        super().__init__(write_properties, add_provenance)

    def _map_entrez_to_ensembl(self, raw: str) -> Optional[str]:
        value = raw.strip()
        value = _strip_curie_prefix(value, "NCBIGene:")
        for key in (value, int(value) if value.isdigit() else None):
            if key is None:
                continue
            mapped = self.entrez_to_ensembl.get(key)
            if mapped:
                return mapped
        return None

    def get_edges(self):
        with _open_text(self.filepath) as f:
            header = f.readline()
            if not header:
                return

            for line in f:
                if not line.strip() or line.startswith("#"):
                    continue
                parts = line.rstrip("\n").split("\t")
                if len(parts) < 5:
                    continue

                ncbi_gene_id, gene_symbol, association_type, disease_id, source_ref = parts[:5]
                ensembl_id = self._map_entrez_to_ensembl(ncbi_gene_id)
                if not ensembl_id:
                    continue

                source_id = f"ENSEMBL:{ensembl_id}"
                target_id = disease_id

                props = {}
                if self.write_properties:
                    props["gene_symbol"] = gene_symbol
                    props["association_type"] = association_type
                    props["source_ref"] = source_ref

                if self.add_provenance:
                    props["source"] = self.source
                    props["source_url"] = self.source_url

                yield source_id, target_id, self.label, props
