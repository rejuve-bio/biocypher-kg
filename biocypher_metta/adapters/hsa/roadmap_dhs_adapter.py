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


class RoadMapDHSAdapter(Adapter):
    COL_DICT = {'rsid': 0, 'dataset': 1, 'cell': 2, 'tissue': 3, 'datatype': 4}
    ONTOLOGIES_PREFIX_TO_TYPE = {
        'BTO': 'tissue',
        'CL': 'cell_type',
        'CLO': 'cell_line',
        'EFO': 'experimental_factor',
        'UBERON': 'anatomy',
    }

    def __init__(self, filepath, cell_to_ontology_id_map, tissue_to_ontology_id_map, label,
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
        self.tissue_to_ontology_id_map = pickle.load(open(tissue_to_ontology_id_map, 'rb'))
        self.dbsnp_rsid_map = dbsnp_rsid_map
        self.chr = chr
        self.start = start
        self.end = end
        self.source = "Roadmap Epigenomics Project"
        self.source_url = "https://forgedb.cancer.gov/api/forge2.erc2-DHS/v1.0/forge2.erc2-DHS.forgedb.csv.gz" # {0-9} indicates this dataset is split into 10 parts
        self.label = label
        super(RoadMapDHSAdapter, self).__init__(write_properties, add_provenance)

    def get_edges(self):
        with gzip.open(self.filepath, "rt") as fp:
            next(fp)
            reader = csv.reader(fp, delimiter=',')
            for row in reader:
                try:
                    _id = row[0]
                    chr = self.dbsnp_rsid_map.get(_id, {}).get("chr", None) 
                    pos = self.dbsnp_rsid_map.get(_id, {}).get("pos", None)
                    if chr == None:
                        # print(f"roadmap_dhs: chr is None for {_id}. Skipping it {_id}...")
                        continue
                    if pos == None:
                        # print(f"roadmap_dhs: pos is None for {_id}. Skipping it {_id}...")
                        continue                    
                    #tissue = row[COL_DICT['tissue']].replace('"', '').replace("'", '')
                    cell_id = row[self.COL_DICT['cell']].split()[0]
                    biological_context = self.cell_to_ontology_id_map.get(cell_id, None)
                    if biological_context == None:
                        print(f"{cell_id} not found in ontology map. Skipping it...")
                        continue
                    
                    if check_genomic_location(self.chr, self.start, self.end, chr, pos, pos):
                        _props = {}                        
                        _source = _id
                        prefix = biological_context[1].split('_')[0]
                        _target = (self.ONTOLOGIES_PREFIX_TO_TYPE[prefix], biological_context[1])

                        # for tissue linking, we need to get the tissue id from the biological context
                        tissue = row[self.COL_DICT['tissue']]
                        tissue_id = self.tissue_to_ontology_id_map.get(tissue, None)
                        if tissue_id == None:
                            print(f"{tissue} not found in ontology map. Skipping it...")
                            continue
                        
                        tissue_type = self.ONTOLOGIES_PREFIX_TO_TYPE[tissue_id.split('_')[0]]
                        tissue_target = (tissue_type, tissue_id)

                        if self.write_properties and self.add_provenance:
                            _props['source'] = self.source
                            _props['source_url'] = self.source_url
                                
                        yield _source, _target, self.label, _props
                        yield _source, tissue_target, self.label, _props                        
                except Exception as e:
                    print(f"error while parsing row: {row}, error: {e.args} skipping... {biological_context}")
                    continue