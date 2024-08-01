# Author Abdulrahman S. Omar <xabush@singularitynet.io>
from biocypher_metta.adapters import Adapter
import pickle
import csv
import gzip

# Imports STRING Protein-Protein interactions

# protein1 protein2 combined_score
# 9606.ENSP00000000233 9606.ENSP00000356607 173
# 9606.ENSP00000000233 9606.ENSP00000427567 154
# 9606.ENSP00000000233 9606.ENSP00000253413 151
# 9606.ENSP00000000233 9606.ENSP00000493357 471
# 9606.ENSP00000000233 9606.ENSP00000324127 201
# 9606.ENSP00000000233 9606.ENSP00000325266 180
# 9606.ENSP00000000233 9606.ENSP00000320935 181

# protein1 protein2 combined_score
#
# 7227.FBpp0070001 7227.FBpp0082651 375
# 7227.FBpp0070001 7227.FBpp0078514 379
# 7227.FBpp0070001 7227.FBpp0083155 384
# 7227.FBpp0070001 7227.FBpp0304379 242
# 7227.FBpp0070001 7227.FBpp0076311 173
# 7227.FBpp0070001 7227.FBpp0074378 228


class StringPPIAdapter(Adapter):
    def __init__(self, dmel_filepath, dmel_ensembl_to_uniprot_map, hsa_filepath, hsa_ensembl_to_uniprot_map,
                 write_properties, add_provenance):
        """
        Constructs StringPPI adapter that returns edges between proteins
        :param filepath: Path to the TSV file downloaded from String
        :param ensembl_to_uniprot_map: file containing pickled dictionary mapping Ensemble Protein IDs to Uniprot IDs
        """

        self.dmel_filepath = dmel_filepath
        self.hsa_filepath = hsa_filepath

        with open(dmel_ensembl_to_uniprot_map, "rb") as f:
            self.dmel_ensembl_to_uniprot_map = pickle.load(f)

        with open(hsa_ensembl_to_uniprot_map, "rb") as f:
            self.hsa_ensembl_to_uniprot_map = pickle.load(f)

        self.label = "interacts_with"
        self.source = "STRING"
        self.source_url = "https://string-db.org/"
        self.version = "v12.0"
        super(StringPPIAdapter, self).__init__(write_properties, add_provenance)

    def get_edges(self):
        with gzip.open(self.dmel_filepath, "rt") as fp:
            table = csv.reader(fp, delimiter=" ", quotechar='"')
            table.__next__() # skip header
            for row in table:
                #print(row)
                protein1 = row[0].split(".")[1]
                protein2 = row[1].split(".")[1]
                if protein1 in self.dmel_ensembl_to_uniprot_map and protein2 in self.dmel_ensembl_to_uniprot_map:
                    protein1_uniprot = self.dmel_ensembl_to_uniprot_map[protein1]
                    protein2_uniprot = self.dmel_ensembl_to_uniprot_map[protein2]
                    _source = protein1_uniprot
                    _target = protein2_uniprot
                    _props = {
                    }
                    if self.write_properties:
                        _props = {
                            "score": float(row[2]) / 1000, # divide by 1000 to normalize score
                        }
                        if self.add_provenance:
                            _props["source"] = self.source
                            _props["source_url"] = self.source_url
                    _props['taxon_id'] = 7227  # mandatory property

                    yield _source, _target, self.label, _props

        with gzip.open(self.hsa_filepath   , "rt") as fp:
            table = csv.reader(fp, delimiter=" ", quotechar='"')
            table.__next__() # skip header
            for row in table:
                protein1 = row[0].split(".")[1]
                protein2 = row[1].split(".")[1]
                if protein1 in self.hsa_ensembl_to_uniprot_map and protein2 in self.hsa_ensembl_to_uniprot_map:
                    protein1_uniprot = self.hsa_ensembl_to_uniprot_map[protein1]
                    protein2_uniprot = self.hsa_ensembl_to_uniprot_map[protein2]
                    _source = protein1_uniprot
                    _target = protein2_uniprot
                    _props = {
                    }
                    if self.write_properties:
                        _props = {
                            "score": float(row[2]) / 1000, # divide by 1000 to normalize score
                        }
                        if self.add_provenance:
                            _props["source"] = self.source
                            _props["source_url"] = self.source_url
                    _props['taxon_id'] = 9606  # mandatory property

                    yield _source, _target, self.label, _props
