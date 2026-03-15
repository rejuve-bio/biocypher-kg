# Alliance Gene-Disease Adapter
# Creates edges between genes and diseases from Alliance for Genome Resources data
# Supports multiple species and four association types:
# - biomarker_via_orthology
# - implicated_via_orthology
# - is_implicated_in
# - is_model_of

import gzip
import csv
from biocypher_metta.adapters import Adapter

# Column indices for the TSV file
COLUMNS = {
    'taxon': 0,
    'species_name': 1,
    'db_object_type': 2,
    'db_object_id': 3,
    'db_object_symbol': 4,
    'association_type': 5,
    'doid': 6,
    'do_term_name': 7,
    'with_ortholog': 8,
    'inferred_from_id': 9,
    'inferred_from_symbol': 10,
    'experimental_condition': 11,
    'modifier': 12,
    'evidence_code': 13,
    'evidence_code_name': 14,
    'reference': 15,
    'date': 16,
    'source': 17,
}

# Map association types to edge labels
ASSOCIATION_TYPE_MAP = {
    'biomarker_via_orthology': 'biomarker_via_orthology',
    'implicated_via_orthology': 'implicated_via_orthology',
    'is_implicated_in': 'is_implicated_in',
    'is_model_of': 'is_model_of',
}


class AllianceGeneDiseaseAdapter(Adapter):
    def __init__(self, filepath, taxon_ids=None, write_properties=None, add_provenance=None):
        """
        Constructs Alliance gene-disease adapter.
        
        :param filepath: Path to DISEASE-ALLIANCE_COMBINED.tsv.gz file
        :param taxon_ids: List of NCBI taxon IDs to include (e.g., [9606] for human).
                         If None, includes all species.
        :param write_properties: Whether to write edge properties
        :param add_provenance: Whether to add provenance information
        """
        self.filepath = filepath
        self.taxon_ids = taxon_ids
        self.source = "Alliance for Genome Resources"
        self.source_url = "https://www.alliancegenome.org/"
        self.version = "latest"
        
        super(AllianceGeneDiseaseAdapter, self).__init__(write_properties, add_provenance)

    def get_edges(self):
        """
        Yields edges between genes and diseases.
        
        Filters by:
        - Taxon IDs (if specified)
        - Association types (only the four supported types)
        - Gene objects (DBobjectType == 'gene')
        """
        with gzip.open(self.filepath, "rt") as fp:
            reader = csv.reader(fp, delimiter="\t")
            
            for row in reader:
                if not row or row[0].startswith("#"):
                    continue
                
                taxon = row[COLUMNS['taxon']]
                db_object_type = row[COLUMNS['db_object_type']]
                association_type = row[COLUMNS['association_type']]
                
                if self.taxon_ids and taxon not in self.taxon_ids:
                    continue
                
                if db_object_type != 'gene':
                    continue
                
                if association_type not in ASSOCIATION_TYPE_MAP:
                    continue
                
                db_object_id = row[COLUMNS['db_object_id']]
                db_object_symbol = row[COLUMNS['db_object_symbol']]
                doid = row[COLUMNS['doid']]
                do_term_name = row[COLUMNS['do_term_name']]
                evidence_code = row[COLUMNS['evidence_code']]
                evidence_code_name = row[COLUMNS['evidence_code_name']]
                reference = row[COLUMNS['reference']]
                date = row[COLUMNS['date']]
                source = row[COLUMNS['source']]
                species_name = row[COLUMNS['species_name']]
                with_ortholog = row[COLUMNS['with_ortholog']]
                inferred_from_id = row[COLUMNS['inferred_from_id']]
                inferred_from_symbol = row[COLUMNS['inferred_from_symbol']]
                    
                # Create edge
                _source = ("gene", db_object_id)
                _target = ("disease", doid)
                _label = ASSOCIATION_TYPE_MAP[association_type]
                _props = {}
                
                if self.write_properties:
                    _props = {
                        "taxon_id": int(taxon.replace("NCBITaxon:", "")),
                        "species_name": species_name,
                        "gene_symbol": db_object_symbol,
                        "disease_name": do_term_name,
                        "evidence_code": evidence_code,
                        "evidence_code_name": evidence_code_name,
                        "reference": reference,
                        "date": date,
                        "source": source,
                        "with_ortholog": with_ortholog if with_ortholog else None,
                        "inferred_from_id": inferred_from_id if inferred_from_id else None,
                        "inferred_from_symbol": inferred_from_symbol if inferred_from_symbol else None,
                    }
                        
                if self.add_provenance:
                    _props["source"] = self.source
                    _props["source_url"] = self.source_url
                    
                yield _source, _target, _label, _props
    
