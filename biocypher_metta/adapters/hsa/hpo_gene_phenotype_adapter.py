import gzip
import pickle
from typing import Dict, Iterator, Optional, Tuple

from biocypher_metta.adapters import Adapter

# Expected header for HPO phenotype files
EXPECTED_HEADER = "ncbi_gene_id\tgene_symbol\thpo_id\thpo_name\tfrequency\tdisease_id"


def _open_text(path: str):
    """Open a text file, handling gzip compression."""
    if path.endswith(".gz"):
        return gzip.open(path, "rt")
    return open(path, "r")


def _load_pickle_map(path: str) -> Dict:
    """Load a dictionary from a pickle file."""
    with open(path, "rb") as fh:
        data = pickle.load(fh)
    if not isinstance(data, dict):
        raise TypeError(f"Expected dict in pickle map at {path}, got {type(data)}")
    return data


def _strip_curie_prefix(value: str, prefix: str) -> str:
    """Remove CURIE prefix if present."""
    if value.startswith(prefix):
        return value[len(prefix) :]
    return value


class HPOAdapter(Adapter):
    """Adapter for HPO gene-to-phenotype annotations.

    Processes genes_to_phenotype.txt files with NCBI Gene IDs.
    Expected format: ncbi_gene_id	gene_symbol	hpo_id	hpo_name	frequency	disease_id
    """

    def __init__(
        self,
        filepath: str,
        write_properties: bool,
        add_provenance: bool,
        entrez_to_ensembl_map: str,
        label: str = "gene_phenotype",
    ):
        """Initialize the HPO adapter.

        Args:
            filepath: Path to the HPO phenotype annotation file
            write_properties: Whether to include properties in output
            add_provenance: Whether to add source metadata
            entrez_to_ensembl_map: Path to Entrez-to-Ensembl mapping pickle file
            label: Edge label for the relationships
        """
        self.filepath = filepath
        self.label = label
        self.source = "Human Phenotype Ontology"
        self.source_url = "https://hpo.jax.org/"

        self.entrez_to_ensembl = _load_pickle_map(entrez_to_ensembl_map)

        super().__init__(write_properties, add_provenance)

    def _map_entrez_to_ensembl(self, raw_entrez_id: str) -> Optional[str]:
        """Map Entrez Gene ID to Ensembl ID."""
        if not self.entrez_to_ensembl:
            return None

        entrez_id = raw_entrez_id.strip()
        entrez_id = _strip_curie_prefix(entrez_id, "NCBIGene:")

        
        for key in (entrez_id, int(entrez_id) if entrez_id.isdigit() else None):
            if key is None:
                continue
            if key in self.entrez_to_ensembl:
                return self.entrez_to_ensembl[key]

        return None

    def _iter_gene_phenotype_rows(self) -> Iterator[Tuple[str, str, str, str, str, str]]:
        """Parse HPO phenotype file and yield (gene_id, symbol, hpo_id, name, frequency, disease_id)."""
        with _open_text(self.filepath) as f:
            header = f.readline()
            if not header:
                return

            # Validate header format
            if header.rstrip("\n") != EXPECTED_HEADER:
                raise ValueError(
                    f"Invalid header in {self.filepath}. "
                    f"Expected: {EXPECTED_HEADER!r}, "
                    f"Got: {header.rstrip()!r}"
                )

            # Parse data rows
            for line in f:
                line = line.strip()
                if not line or line.startswith("#"):
                    continue

                parts = line.split("\t")
                if len(parts) != 6:
                    continue

                yield tuple(parts)

    def get_edges(self):
        """Generate gene-phenotype relationship edges."""
        for gene_id, gene_symbol, hpo_id, hpo_name, frequency, disease_id in self._iter_gene_phenotype_rows():
            # Map to Ensembl ID
            ensembl_id = self._map_entrez_to_ensembl(gene_id)
            if not ensembl_id:
                continue

            # Create edge identifiers
            source_id = f"ENSEMBL:{ensembl_id}"
            target_id = hpo_id

            # Build properties
            props = {}
            if self.write_properties:
                props["gene_symbol"] = gene_symbol
                props["phenotype_name"] = hpo_name
                if frequency and frequency != "-":
                    props["frequency"] = frequency
                if disease_id and disease_id != "-":
                    props["disease_id"] = disease_id

            if self.add_provenance:
                props["source"] = self.source
                props["source_url"] = self.source_url

            yield source_id, target_id, self.label, props
