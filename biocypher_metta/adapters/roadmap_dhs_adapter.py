# Author Abdulrahman S. Omar <xabush@singularitynet.io>
import csv
import gzip
import os.path
import pickle
from biocypher_metta.adapters import Adapter
from biocypher_metta.adapters.helpers import check_genomic_location

# Example roadmap csv input files
# rsid,dataset,cell,tissue,datatype
# rs10,erc2-DHS,"E050 Primary hematopoietic stem cells G-CSF-mobili",Blood,"DNase I Hotspot"
# rs10,erc2-DHS,"E028 Breast variant Human Mammary Epithelial Cells",Breast,"DNase I Hotspot"
# rs10000009,erc2-DHS,"E094 Gastric",Gastric,"DNase I Hotspot"
# rs1000001,erc2-DHS,"E090 Fetal Muscle Leg","Fetal Muscle Leg","DNase I Hotspot"

COL_DICT = {'rsid': 0, 'dataset': 1, 'cell': 2, 'tissue': 3, 'datatype': 4}

class RoadMapDHSAdapter(Adapter):
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
        self.cell_to_ontology_id_map = pickle.load(open(cell_to_ontology_id_map, 'rb'))
        self.dbsnp_rsid_map = dbsnp_rsid_map
        self.chr = chr
        self.start = start
        self.end = end

        self.source = "Roadmap Epigenomics Project"
        self.source_url = "https://forgedb.cancer.gov/api/forge2.erc2-DHS/v1.0/forge2.erc2-DHS.forgedb.csv.gz" # {0-9} indicates this dataset is split into 10 parts
        self.label = "in_dnase_I_hotspot"

        super(RoadMapDHSAdapter, self).__init__(write_properties, add_provenance)

    def get_edges(self):
        with gzip.open(self.filepath, "rt") as fp:
            next(fp)
            reader = csv.reader(fp, delimiter=',')
            for row in reader:
                try:
                    _id = row[0]
                    chr = self.dbsnp_rsid_map[_id]["chr"]
                    pos = self.dbsnp_rsid_map[_id]["pos"]
                    #tissue = row[COL_DICT['tissue']].replace('"', '').replace("'", '')
                    cell_id = row[COL_DICT['cell']].split()[0]
                    biological_context = self.cell_to_ontology_id_map.get(cell_id, None)
                    if check_genomic_location(self.chr, self.start, self.end, chr, pos, pos):
                        _props = {}
                        if biological_context == None:
                            print(f"{cell_id} not found in ontology map skipping...")
                            continue
                        
                        _source = _id
                        _target = biological_context[1]

                        if self.write_properties and self.add_provenance:
                            _props['source'] = self.source
                            _props['source_url'] = self.source_url
                                
                        yield _source, _target, self.label, _props

                except Exception as e:
                    # print(f"error while parsing row: {row}, error: {e} skipping...")
                    continue