# Author Abdulrahman S. Omar <xabush@singularitynet.io>
import csv
import gzip
import os.path
import pickle
from biocypher_metta.adapters import Adapter
from biocypher_metta.adapters.helpers import check_genomic_location

# Example roadmap csv input files
# rsid,dataset,cell,tissue,datatype
# rs10000007,erc2-H3-all,E032 Primary B cells from peripheral blood,Blood,H3K36me3
# rs10000007,erc2-H3-all,E032 Primary B cells from peripheral blood,Blood,H3K4me1
# rs10000007,erc2-H3-all,E050 Primary hematopoietic stem cells G-CSF-mobili,Blood,H3K27me3
# rs10000007,erc2-H3-all,E083 Fetal Heart,Fetal Heart,H3K36me3

COL_DICT = {'rsid': 0, 'dataset': 1, 'cell': 2, 'tissue': 3, 'datatype': 4}

class RoadMapH3MarkAdapter(Adapter):

    def __init__(self, filepath, cell_to_ontology_id_map, 
                 dbsnp_rsid_map, write_properties, add_provenance,
                 chr=None, start=None, end=None):
        """
        :param filepath: path to the directory containing epigenomic data
        :param dbsnp_rsid_map: a dictionary mapping dbSNP rsid to genomic position
        :param chr: chromosome name
        :param start: start position
        :param end: end position
        """
        self.filepath = filepath
        assert os.path.isdir(self.filepath), "The path to the directory containing epigenomic data is not directory"
        self.cell_to_ontology_id_map = pickle.load(open(cell_to_ontology_id_map, 'rb'))
        self.dbsnp_rsid_map = dbsnp_rsid_map
        self.chr = chr
        self.start = start
        self.end = end

        self.source = 'Roadmap Epigenomics Project'
        self.source_url = "https://forgedb.cancer.gov/api/forge2.erc2-H3-all/v1.0/forge2.erc2-H3-all.{0-9}.forgedb.csv.gz" # {0-9} indicates this dataset is split into 10 parts
        self.label = "histone_modification"

        super(RoadMapH3MarkAdapter, self).__init__(write_properties, add_provenance)


    def get_edges(self):
        for file_name in os.listdir(self.filepath):
            with gzip.open(os.path.join(self.filepath, file_name), "rt") as fp:
                next(fp)
                reader = csv.reader(fp, delimiter=',')
                for row in reader:
                    try:
                        _id = row[0]
                        chr = self.dbsnp_rsid_map[_id]["chr"]
                        pos = self.dbsnp_rsid_map[_id]["pos"]
                        cell_id = row[COL_DICT['cell']].replace('"', '').replace("'", '')
                        biological_context = self.cell_to_ontology_id_map.get(cell_id, None)
                        if check_genomic_location(self.chr, self.start, self.end, chr, pos, pos):
                            _source = _id
                            _target = biological_context
                            _props = {}
                            if biological_context == None:
                                print(f"{cell_id} not found in ontology map skipping...")
                                continue

                            if self.write_properties:
                                _props["modification"] = row[COL_DICT['datatype']]
                                if self.add_provenance:
                                    _props['source'] = self.source
                                    _props['source_url'] = self.source_url
                                    
                            yield _source, _target, self.label, _props

                    except Exception as e:
                        print(f"error while parsing row: {row}, error: {e} skipping...")
                        continue