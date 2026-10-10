# Alliance Gene-Disease Adapter
# Creates edges between genes and diseases from Alliance for Genome Resources data
# Supports multiple species and four association types:
# - biomarker_via_orthology
# - implicated_via_orthology
# - is_implicated_in
# - is_model_of
# - is_marker_for

import gzip
import csv
from typing import Optional
from biocypher_metta.adapters import Adapter
from biocypher_metta.processors import HGNCProcessor, BioMartEnsemblProcessor

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

# Species whose Alliance IDs only need the "DB:" prefix stripped.
_STRIP_PREFIX = {
    '7227': 'FB:',   # FB:FBgn... → FBgn...
    '6239': 'WB:',   # WB:WBGene... → WBGene...
}

# BioMart-based species: taxon_id → (dataset, species_id_attr, species_code)
_BIOMART_CONFIGS = {
    '10116': ('rnorvegicus_gene_ensembl', 'rgd_id', 'rno'),
    '10090': ('mmusculus_gene_ensembl',   'mgi_id', 'mmu'),
}

# Map association types to edge labels
ASSOCIATION_TYPE_MAP = {
    'biomarker_via_orthology': 'biomarker_via_orthology',
    'implicated_via_orthology': 'implicated_via_orthology',
    'is_implicated_in': 'is_implicated_in',
    'is_model_of': 'is_model_of',
    'is_marker_for': 'is_marker_for',
}


class AllianceGeneDiseaseAdapter(Adapter):
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
        Constructs Alliance gene-disease adapter.

        :param filepath: Path to DISEASE-ALLIANCE_COMBINED.tsv.gz file
        :param label: One of the supported association types:
                      - biomarker_via_orthology
                      - implicated_via_orthology
                      - is_implicated_in
                      - is_model_of
                      - is_marker_for
        :param taxon_id: NCBI taxon ID (e.g., '9606' for human).
        :param hgnc_cache_dir: Cache directory for HGNCProcessor (hsa only).
        :param biomart_cache_base_dir: Base directory for BioMart caches.
        :param write_properties: Whether to write edge properties
        :param add_provenance: Whether to add provenance information
        """
        self.filepath = filepath
        self.label = label
        self.taxon_id = str(taxon_id)
        self.source = "Alliance for Genome Resources"
        self.source_url = "https://www.alliancegenome.org/"
        self.version = "latest"

        self._hgnc_processor = HGNCProcessor(cache_dir=hgnc_cache_dir)

        self._biomart_processors = {
            taxon: BioMartEnsemblProcessor(
                dataset=dataset,
                species_id_attr=species_id_attr,
                cache_dir=f"{biomart_cache_base_dir}/{sp_code}/biomart_ensembl",
            )
            for taxon, (dataset, species_id_attr, sp_code) in _BIOMART_CONFIGS.items()
        }

        super(AllianceGeneDiseaseAdapter, self).__init__(write_properties, add_provenance)

    def _resolve_gene_id(self, gene_id: str) -> Optional[str]:
        """Convert an Alliance gene ID to the CURIE-prefixed gene ID used in the KG."""
        resolved = self._resolve_bare_gene_id(gene_id)
        if resolved is None:
            return None
        return f"{Adapter.CURIE_PREFIX[int(self.taxon_id)]}:{resolved}"

    def _resolve_bare_gene_id(self, gene_id: str) -> Optional[str]:
        """Convert an Alliance gene ID to the bare (unprefixed) Ensembl/FlyBase/WormBase ID."""
        if self.taxon_id in _STRIP_PREFIX:
            prefix = _STRIP_PREFIX[self.taxon_id]
            return gene_id[len(prefix):] if gene_id.startswith(prefix) else gene_id

        if self.taxon_id == '9606':
            return self._hgnc_processor.get_ensembl_id(gene_id)

        if self.taxon_id in self._biomart_processors:
            proc = self._biomart_processors[self.taxon_id]
            result = proc.get_ensembl_gene(gene_id)
            if result is None and ':' in gene_id:
                result = proc.get_ensembl_gene(gene_id.split(':', 1)[1])
            return result

        return gene_id

    def get_edges(self):
        """
        Yields edges between genes and diseases.
        
        Filters by:
        - Label (association type)
        - Taxon ID
        - Gene objects (DBobjectType == 'gene')
        """
        if self.label not in ASSOCIATION_TYPE_MAP:
            raise ValueError(f"Invalid label: {self.label}. Must be one of {list(ASSOCIATION_TYPE_MAP.keys())}")

        with gzip.open(self.filepath, "rt") as fp:
            reader = csv.reader(fp, delimiter="\t")
            
            for row in reader:
                if not row or row[0].startswith("#"):
                    continue
                if row[COLUMNS['taxon']] == "Taxon":
                    continue
                
                taxon = row[COLUMNS['taxon']].replace("NCBITaxon:", "")
                db_object_type = row[COLUMNS['db_object_type']]
                association_type = row[COLUMNS['association_type']]
                
                if taxon != self.taxon_id:
                    continue
                
                if db_object_type != 'gene':
                    continue
                
                if association_type != self.label:
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
                resolved_id = self._resolve_gene_id(db_object_id)
                if resolved_id is None:
                    continue
                _source = ("gene", resolved_id)
                _target = ("disease", doid)
                _label = self.label
                _props = {}
                
                if self.write_properties:
                    date_fmt = f"{date[:4]}-{date[4:6]}-{date[6:]}" if len(date) == 8 else date
                    _props = {
                        "taxon_id": int(taxon),
                        "evidence_code": evidence_code.replace('ECO:', 'ECO_'),
                        "evidence_code_name": evidence_code_name,
                        "reference": reference,
                        "date": date_fmt,
                        "data_source": source,
                        "with_ortholog": with_ortholog if with_ortholog else None,
                        "inferred_from_id": inferred_from_id if inferred_from_id else None,
                        "inferred_from_symbol": inferred_from_symbol if inferred_from_symbol else None,
                    }

                if self.add_provenance:
                    _props["source"] = self.source
                    _props["source_url"] = self.source_url
                    
                yield _source, _target, _label, _props
    
