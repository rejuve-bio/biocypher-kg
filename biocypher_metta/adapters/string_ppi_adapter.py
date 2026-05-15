# Author Abdulrahman S. Omar <xabush@singularitynet.io>
from biocypher_metta.adapters import Adapter
import pickle
from biocypher_metta.processors import EnsemblUniProtProcessor
import csv
import gzip
from biocypher_metta.adapters.helpers import to_float

# Imports STRING Protein-Protein interactions

# protein1 protein2 combined_score
# 9606.ENSP00000000233 9606.ENSP00000356607 173
# 9606.ENSP00000000233 9606.ENSP00000427567 154
# 9606.ENSP00000000233 9606.ENSP00000253413 151
# 9606.ENSP00000000233 9606.ENSP00000493357 471
# 9606.ENSP00000000233 9606.ENSP00000324127 201
# 9606.ENSP00000000233 9606.ENSP00000325266 180
# 9606.ENSP00000000233 9606.ENSP00000320935 181

class StringPPIAdapter(Adapter):
    def __init__(self, filepath, ensembl_to_uniprot_map=None, taxon_id=9606, label='interacts_with',
                 write_properties=None, add_provenance=None,
                 ensembl_uniprot_processor=None):
        """
        Constructs StringPPI adapter that returns edges between proteins
        :param filepath: Path to the TSV file downloaded from String
        :param ensembl_to_uniprot_map: DEPRECATED - use ensembl_uniprot_processor instead
        :param ensembl_uniprot_processor: EnsemblUniProtProcessor instance for ID mapping
        """
        self.filepath = filepath
        self.taxon_id = taxon_id

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
        super(StringPPIAdapter, self).__init__(write_properties, add_provenance)

    def get_edges(self):
        with gzip.open(self.filepath, "rt") as fp:
            table = csv.reader(fp, delimiter=" ", quotechar='"')
            table.__next__() # skip header
            for row in table:
                protein1 = row[0].split(".")[1]
                protein2 = row[1].split(".")[1]
                if protein1 in self.ensembl2uniprot and protein2 in self.ensembl2uniprot:
                    protein1_uniprot = self.ensembl2uniprot[protein1]
                    protein2_uniprot = self.ensembl2uniprot[protein2]
                    _source = f"{protein1_uniprot}"
                    _target = f"{protein2_uniprot}"
                    _props = {}
                    if self.write_properties:
                        _props = {
                            "score": to_float(row[2]) / 1000, # divide by 1000 to normalize score
                        }
                        _props['taxon_id'] = f'{self.taxon_id}'
                        if self.add_provenance:
                            _props["source"] = self.source
                            _props["source_url"] = self.source_url

                    yield _source, _target, self.label, _props