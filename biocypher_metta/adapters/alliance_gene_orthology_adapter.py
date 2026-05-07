# Alliance Gene-Orthology Adapter
# Creates edges between genes indicating orthology from Alliance for Genome Resources data
# Supports multiple species and filters by taxon ID

import gzip
import csv
from biocypher_metta.adapters import Adapter
from biocypher_metta.processors import HGNCProcessor

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

class AllianceGeneOrthologyAdapter(Adapter):
    def __init__(self, filepath, label, taxon_id, write_properties=None, add_provenance=None):
        """
        Constructs Alliance gene-orthology adapter.
        
        :param filepath: Path to ORTHOLOGY-ALLIANCE_COMBINED.tsv.gz file
        :param label: Edge label 
        :param taxon_id: NCBI taxon ID of the "source" species (e.g., '7227' for fly).
        :param write_properties: Whether to write edge properties
        :param add_provenance: Whether to add provenance information
        """
        self.filepath = filepath
        self.label = label
        self.taxon_id = str(taxon_id) if taxon_id else None
        self.hgnc_processor = HGNCProcessor()
        self.source = "Alliance for Genome Resources"
        self.source_url = "https://www.alliancegenome.org/"
        
        super(AllianceGeneOrthologyAdapter, self).__init__(write_properties, add_provenance)

    def get_edges(self):
        """
        Yields edges between genes indicating orthology.
        
        Filters the data to only include orthology relationships where:
        - The source gene (Gene1) belongs to the taxon specified in self.taxon_id.
        - The target gene (Gene2) belongs to any of the other supported taxa.
        """
        target_taxon = f"NCBITaxon:{self.taxon_id}"
        other_taxa = [f"NCBITaxon:{t}" for t in SUPPORTED_TAXA.keys() if t != self.taxon_id]

        with gzip.open(self.filepath, "rt") as fp:
            reader = csv.reader(fp, delimiter="\t")
            
            for row in reader:
                if not row or row[0].startswith("#"):
                    continue
                if row[COLUMNS['gene1_id']] == "Gene1ID":
                    continue
                
                gene1_taxon = row[COLUMNS['gene1_taxon']]
                gene2_taxon = row[COLUMNS['gene2_taxon']]
                
                # Filtering logic
                if gene1_taxon != target_taxon:
                    continue
                
                if gene2_taxon not in other_taxa:
                    continue
                
                gene1_id = row[COLUMNS['gene1_id']]
                gene1_symbol = row[COLUMNS['gene1_symbol']]
                gene2_id = row[COLUMNS['gene2_id']]
                gene2_symbol = row[COLUMNS['gene2_symbol']]
                
                gene1_species = row[COLUMNS['gene1_species_name']]
                gene2_species = row[COLUMNS['gene2_species_name']]

                # ID handling for human genes
                if gene1_taxon == "NCBITaxon:9606":
                    gene1_id = self.hgnc_processor.get_ensembl_id(gene1_id)
                if gene2_taxon == "NCBITaxon:9606":
                    gene2_id = self.hgnc_processor.get_ensembl_id(gene2_id)
                
                if not gene1_id or not gene2_id:
                    continue

                _source = ("gene", gene1_id)
                _target = ("gene", gene2_id)
                _label = self.label
                _props = {}
                
                if self.write_properties:
                    _props = {
                        "source_organism": gene1_species,
                        "taxon_id": int(gene1_taxon.split(":")[1]),
                        "target_organism_taxon_id": int(gene2_taxon.split(":")[1]),
                    }
                         
                if self.add_provenance:
                    _props["source"] = self.source
                    _props["source_url"] = self.source_url
                    
                yield _source, _target, _label, _props
