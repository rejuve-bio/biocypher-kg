# Alliance Gene-Orthology Adapter
# Creates edges between genes indicating orthology from Alliance for Genome Resources data
# Supports multiple species and filters by taxon ID

import gzip
import csv
from typing import Optional
from biocypher_metta.adapters import Adapter
from biocypher_metta.processors import HGNCProcessor, BioMartEnsemblProcessor
from biocypher._logger import logger

# Column indices for the TSV file
COLUMNS = {
    'gene1_id': 0,
    'gene1_symbol': 1,
    'gene1_taxon': 2,
    'gene1_species_name': 3,
    'gene2_id': 4,
    'gene2_symbol': 5,
    'gene2_taxon': 6,
    'gene2_species_name': 7,
}

SUPPORTED_TAXA = {
    '6239': 'cel',
    '7227': 'dmel',
    '9606': 'hsa',
    '10090': 'mmu',
    '10116': 'rno',
}

# Species whose Alliance IDs only need the "DB:" prefix stripped.
# FlyBase / WormBase IDs are used directly as gene IDs in Ensembl Metazoa.
_STRIP_PREFIX = {
    '7227': 'FB:',   # FB:FBgn... → FBgn...
    '6239': 'WB:',   # WB:WBGene... → WBGene...
}

# BioMart-based species: taxon_id → (dataset, species_id_attr, species_code)
# To add a new species: append one entry here — no other change needed.
_BIOMART_CONFIGS = {
    '10116': ('rnorvegicus_gene_ensembl', 'rgd_id', 'rno'),
    '10090': ('mmusculus_gene_ensembl',   'mgi_id', 'mmu'),
}


class AllianceGeneOrthologyAdapter(Adapter):
    def __init__(
        self,
        filepath,
        label,
        taxon_id,
        hgnc_cache_dir: str = 'aux_files/hsa/hgnc',
        biomart_cache_base_dir: str = 'aux_files',
        write_properties=None,
        add_provenance=None,
    ):
        """
        Constructs Alliance gene-orthology adapter.

        :param filepath:               Path to ORTHOLOGY-ALLIANCE_COMBINED.tsv.gz
        :param label:                  Edge label
        :param taxon_id:               NCBI taxon ID of the source species
        :param hgnc_cache_dir:         Cache dir for HGNCProcessor (hsa)
        :param biomart_cache_base_dir: Base dir for BioMart processor caches.
                                       Each species stores its cache under
                                       {base}/{species_code}/biomart_ensembl/
        :param write_properties:       Whether to write edge properties
        :param add_provenance:         Whether to add provenance information

        Adding support for a new species:
          - Strip-prefix only (dmel/cel pattern): add to _STRIP_PREFIX above.
          - BioMart-based (rno/mmu pattern): add one entry to _BIOMART_CONFIGS above.
        """
        self.filepath = filepath
        self.label = label
        self.taxon_id = str(taxon_id) if taxon_id else None
        self.source = "Alliance for Genome Resources"
        self.source_url = "https://www.alliancegenome.org/"

        self._hgnc_processor = HGNCProcessor(cache_dir=hgnc_cache_dir)

        self._biomart_processors = {
            taxon: BioMartEnsemblProcessor(
                dataset=dataset,
                species_id_attr=species_id_attr,
                cache_dir=f"{biomart_cache_base_dir}/{sp_code}/biomart_ensembl",
            )
            for taxon, (dataset, species_id_attr, sp_code) in _BIOMART_CONFIGS.items()
        }

        super(AllianceGeneOrthologyAdapter, self).__init__(write_properties, add_provenance)

    def _resolve_gene_id(self, gene_id: str, taxon: str) -> Optional[str]:
        """Convert an Alliance gene ID to the CURIE-prefixed gene ID used in the KG."""
        resolved = self._resolve_bare_gene_id(gene_id, taxon)
        if resolved is None:
            return None
        return f"{Adapter.CURIE_PREFIX[int(taxon)]}:{resolved}"

    def _resolve_bare_gene_id(self, gene_id: str, taxon: str) -> Optional[str]:
        """Convert an Alliance gene ID to the bare (unprefixed) Ensembl/FlyBase/WormBase ID."""
        if taxon in _STRIP_PREFIX:
            prefix = _STRIP_PREFIX[taxon]
            return gene_id[len(prefix):] if gene_id.startswith(prefix) else gene_id

        if taxon == '9606':
            return self._hgnc_processor.get_ensembl_id(gene_id)

        if taxon in self._biomart_processors:
            proc = self._biomart_processors[taxon]
            # Try the full Alliance ID first (e.g. MGI:96677, which BioMart also uses).
            # Fall back to the stripped ID (e.g. 2129 from RGD:2129, which BioMart stores
            # without prefix for the 'rgd' attribute).
            result = proc.get_ensembl_gene(gene_id)
            if result is None and ':' in gene_id:
                result = proc.get_ensembl_gene(gene_id.split(':', 1)[1])
            return result

        logger.debug(f"No resolver for taxon {taxon}, returning Alliance ID as-is: {gene_id}")
        return gene_id

    def get_edges(self):
        """
        Yields edges between genes indicating orthology.

        Filters rows where Gene1 belongs to self.taxon_id and Gene2 belongs
        to any other supported taxon.
        """
        target_taxon = f"NCBITaxon:{self.taxon_id}"
        other_taxa = [f"NCBITaxon:{t}" for t in SUPPORTED_TAXA if t != self.taxon_id]

        with gzip.open(self.filepath, "rt") as fp:
            reader = csv.reader(fp, delimiter="\t")

            for row in reader:
                if not row or row[0].startswith("#"):
                    continue
                if row[COLUMNS['gene1_id']] == "Gene1ID":
                    continue

                gene1_taxon = row[COLUMNS['gene1_taxon']]
                gene2_taxon = row[COLUMNS['gene2_taxon']]

                if gene1_taxon != target_taxon:
                    continue
                if gene2_taxon not in other_taxa:
                    continue

                taxon1 = gene1_taxon.split(':', 1)[1]
                taxon2 = gene2_taxon.split(':', 1)[1]

                gene1_id = self._resolve_gene_id(row[COLUMNS['gene1_id']], taxon1)
                gene2_id = self._resolve_gene_id(row[COLUMNS['gene2_id']], taxon2)

                if not gene1_id or not gene2_id:
                    continue

                _props = {}
                if self.write_properties:
                    _props = {
                        "source_organism": row[COLUMNS['gene1_species_name']],
                        "taxon_id": int(taxon1),
                        "target_organism_taxon_id": int(taxon2),
                    }

                if self.add_provenance:
                    _props["source"] = self.source
                    _props["source_url"] = self.source_url

                yield ("gene", gene1_id), ("gene", gene2_id), self.label, _props
