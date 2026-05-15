# Author Abdulrahman S. Omar <xabush@singularitynet.io>
import csv
import gzip
import os.path
import pickle
from biocypher_metta.adapters import Adapter
from biocypher_metta.adapters.helpers import check_genomic_location

# Example roadmap csv input files
# rsid,dataset,cell,tissue,datatype
# rs10000007,erc2-chromatin15state-all,E063 Adipose Nuclei,Adipose,TxWk
# rs10000007,erc2-chromatin15state-all,E080 Fetal Adrenal Gland,Adrenal,Quies
# rs10000007,erc2-chromatin15state-all,E029 Primary monocytes from peripheral blood,Blood,Quies


class RoadMapChromatinStateAdapter(Adapter):
    COL_DICT = {'rsid': 0, 'dataset': 1, 'cell': 2, 'tissue': 3, 'datatype': 4}
    ONTOLOGIES_PREFIX_TO_TYPE = {
        'BTO': 'tissue',
        'CL': 'cell_type',
        'CLO': 'cell_line',
        'EFO': 'experimental_factor',
        'UBERON': 'anatomy',
    }


    def __init__(self, filepath, cell_to_ontology_id_map,  tissue_to_ontology_id_map, label,
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
        self.tissue_to_ontology_id_map = pickle.load(open(tissue_to_ontology_id_map, 'rb'))
        self.dbsnp_rsid_map = dbsnp_rsid_map
        self.chr = chr
        self.start = start
        self.end = end

        self.source = "Roadmap Epigenomics Project"
        self.source_url = "https://forgedb.cancer.gov/api/forge2.erc2-chromatin15state-all/v1.0/forge2.erc2-chromatin15state-all.{0-9}.forgedb.csv.gz" # {0-9} indicates this dataset is split into 10 parts
        self.label = label

        super(RoadMapChromatinStateAdapter, self).__init__(write_properties, add_provenance)

    def get_edges(self):
        seen_edges = set()
        for file_name in os.listdir(self.filepath):
            _last_id = _last_chr = _last_pos = None
            with gzip.open(os.path.join(self.filepath, file_name), "rt") as fp:
                next(fp)
                reader = csv.reader(fp, delimiter=',')
                for row in reader:
                    try:
                        _id = row[0]
                        if _id != _last_id:
                            rsid_data = self.dbsnp_rsid_map.get(_id)
                            if rsid_data is not None:
                                _last_chr = rsid_data.get("chr")
                                _last_pos = rsid_data.get("pos")
                            else:
                                _last_chr = _last_pos = None
                            _last_id = _id
                        chr, pos = _last_chr, _last_pos

                        if chr is None or pos is None:
                            continue
                        if not check_genomic_location(self.chr, self.start, self.end, chr, pos, pos):
                            continue

                        cell_id = row[self.COL_DICT['cell']].split()[0]
                        biological_context = self.cell_to_ontology_id_map.get(cell_id, [None])[-1]
                        if biological_context is None:
                            print(f"{cell_id} not found in ontology map. Skipping it...")
                            continue

                        _source = _id
                        prefix = biological_context.split('_')[0]
                        _target = (self.ONTOLOGIES_PREFIX_TO_TYPE[prefix], biological_context)

                        tissue = row[self.COL_DICT['tissue']]
                        tissue_id = self.tissue_to_ontology_id_map.get(tissue)
                        if tissue_id is None:
                            print(f"{tissue} not found in ontology map. Skipping it...")
                            continue

                        tissue_type = self.ONTOLOGIES_PREFIX_TO_TYPE[tissue_id.split('_')[0]]
                        tissue_target = (tissue_type, tissue_id)
                        _props = {}
                        if self.write_properties:
                            _props["state"] = row[self.COL_DICT['datatype']]
                            if self.add_provenance:
                                _props['source'] = self.source
                                _props['source_url'] = self.source_url

                        edge_key = (_source, _target)
                        edge_key2 = (_source, tissue_target)
                        if edge_key not in seen_edges:
                            seen_edges.add(edge_key)
                            yield _source, _target, self.label, _props
                        if edge_key2 not in seen_edges:
                            seen_edges.add(edge_key2)
                            yield _source, tissue_target, self.label, _props

                    except Exception as e:
                        print(f"error while parsing row: {row}, error: {e.args}. Skipping...")
                        continue