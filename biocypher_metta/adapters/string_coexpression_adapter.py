# STRING Coexpression Adapter
# Creates edges between proteins based on coexpression scores from STRING detailed links file
# Filters for coexpression values > 400 (medium confidence threshold)

from biocypher_metta.adapters import Adapter
import pickle
from biocypher_metta.processors import EnsemblUniProtProcessor
import csv
import gzip
from biocypher_metta.adapters.helpers import to_float

# STRING detailed links file format:
# protein1 protein2 neighborhood fusion cooccurrence coexpression experimental database textmining combined_score
# 9606.ENSP00000000233 9606.ENSP00000356607 0 0 0 0 0 0 0 173
# The coexpression column (index 5) contains values 0-1000 (scaled)
# Values > 400 indicate medium to high confidence coexpression


class StringCoexpressionAdapter(Adapter):
    def __init__(self, filepath, taxon_id, 
                 label,ensembl_to_uniprot_map=None, coexpression_threshold=400,
                 write_properties=None, add_provenance=None,
                 ensembl_uniprot_processor=None):
        """
        Constructs StringCoexpression adapter that returns edges between proteins
        based on coexpression scores from STRING detailed links file.
        
        :param filepath: Path to the detailed links file downloaded from STRING
        :param ensembl_to_uniprot_map: DEPRECATED - use ensembl_uniprot_processor instead
        :param taxon_id: NCBI taxonomy ID
        :param label: Edge label
        :param coexpression_threshold: Minimum coexpression score to create edge (default: 400)
        :param write_properties: Whether to write edge properties
        :param add_provenance: Whether to add provenance information
        :param ensembl_uniprot_processor: EnsemblUniProtProcessor instance for ID mapping
        """
        self.filepath = filepath
        self.taxon_id = taxon_id
        self.coexpression_threshold = coexpression_threshold

        # Use provided processor or create new one; fallback to pickle for non-human
        if ensembl_uniprot_processor is not None:
            self.processor = ensembl_uniprot_processor
        elif ensembl_to_uniprot_map is not None and taxon_id != 9606:
            self.processor = None
            with open(ensembl_to_uniprot_map, "rb") as f:
                self.ensembl2uniprot = pickle.load(f)
        else:
            self.processor = EnsemblUniProtProcessor()
            self.processor.load_or_update()

        if hasattr(self, 'processor') and self.processor is not None:
            self.ensembl2uniprot = self.processor.mapping

        self.label = label
        self.source = "STRING"
        self.source_url = "https://string-db.org/"
        self.version = "v12.0"
        super(StringCoexpressionAdapter, self).__init__(write_properties, add_provenance)

    def get_edges(self):
        """
        Yields edges for protein pairs with coexpression scores above threshold.
        
        Coexpression column index: 5 (0-indexed)
        Score range: 0-1000 (scaled)
        Threshold: 400 (medium confidence)
        """
        with gzip.open(self.filepath, "rt") as fp:
            table = csv.reader(fp, delimiter=" ", quotechar='"')
            table.__next__()  
            
            for row in table:
                try:
                    protein1 = row[0].split(".")[1]
                    protein2 = row[1].split(".")[1]
                    coexpression_score = int(row[5])  
                    
                    if coexpression_score <= self.coexpression_threshold:
                        continue
                    
                    if protein1 not in self.ensembl2uniprot or protein2 not in self.ensembl2uniprot:
                        continue
                    
                    protein1_uniprot = self.ensembl2uniprot[protein1]
                    protein2_uniprot = self.ensembl2uniprot[protein2]

                    _source = ("protein", protein1_uniprot)
                    _target = ("protein", protein2_uniprot)
                    _props = {}
                    
                    if self.write_properties:
                        # Normalize coexpression score to 0-1 range
                        _props = {
                            "coexpression_score": to_float(coexpression_score) / 1000,
                        }
                        _props['taxon_id'] = f'{self.taxon_id}'
                        
                        if self.add_provenance:
                            _props["source"] = self.source
                            _props["source_url"] = self.source_url
                    
                    yield _source, _target, self.label, _props
                except KeyError:
                    continue
                    
