from collections import defaultdict
import csv
import gzip
import os
import pickle
from biocypher_metta.adapters import Adapter
from biocypher_metta.adapters.helpers import check_genomic_location

# Example RNAcentral bed input file:
# chr1    10244    10273    URS000035F234_9606    0    -    10244    10273    63,125,151    2    19,5    0,24    .    piRNA    PirBase
# chr1    14396    29331    URS0002033896_9606    0    -    14396    29331    63,125,151    8    433,69,970,198,510,147,99,4594    0,573,1399,2461,2836,3518,3871,10341    .    lncRNA    ENA,GeneCards
# chr1    14404    119856    URS0001A132AA_9606    0    -    14404    119856    63,125,151    12    425,69,515,159,198,510,147,99,154,904,150,211    0,565,1391,2202,2453,2828,3510,3863,10333,76321,77686,105241    .    lncRNA    ENA,GeneCards,MalaCards

# Example RNAcentral rfam annotation input file:
# URS0000000006_1317357    GO:0003735    Rfam:RF02541
# URS0000000006_1317357    GO:0005840    Rfam:RF02541
# URS0000000008_381046    GO:0030533    Rfam:RF00005

class RNACentralAdapter(Adapter):
    INDEX = {'chr': 0, 'coord_start': 1, 'coord_end': 2, 'id': 3, 'rna_type': 13}

    def __init__(self, filepath, rfam_filepath, write_properties, add_provenance,
                 type=None, label=None, 
                 chr=None, start=None, end=None, 
                 mapping_file='aux_files/go_subontology_mapping.pkl'):
        self.filepath = filepath
        self.rfam_filepath = rfam_filepath
        self.chr = chr
        self.start = start
        self.end = end
        self.type = type
        self.label = label
        self.write_properties = write_properties
        self.add_provenance = add_provenance
        self.source = 'RNAcentral'
        self.version = '24'
        self.source_url = 'https://rnacentral.org/downloads'

        self.subontology = None
        self.subontology_mapping = None

        # Determine subontology based on label
        if label == 'biological_process_rna':
            self.subontology = 'biological_process'
        elif label == 'molecular_function_rna':
            self.subontology = 'molecular_function'
        elif label == 'cellular_component_rna':
            self.subontology = 'cellular_component'

        # Load subontology mapping
        if os.path.exists(mapping_file):
            with open(mapping_file, 'rb') as f:
                self.subontology_mapping = pickle.load(f)

        self.seen_edges = set()

        super(RNACentralAdapter, self).__init__(write_properties, add_provenance)

    def get_nodes(self):
        with gzip.open(self.filepath, 'rt') as input:
            for line in input:
                infos = line.split('\t')
                rna_id = infos[RNACentralAdapter.INDEX['id']].split('_')[0]
                chr = infos[RNACentralAdapter.INDEX['chr']]
                start = int(infos[RNACentralAdapter.INDEX['coord_start']].strip())+1 # +1 since it is 0 indexed coordinate
                end = int(infos[RNACentralAdapter.INDEX['coord_end']].strip())
                props = {}
                
                if check_genomic_location(self.chr, self.start, self.end, chr, start, end):
                    if self.write_properties:
                        props['chr'] = chr
                        props['start'] = start
                        props['end'] = end
                        props['rna_type'] = infos[RNACentralAdapter.INDEX['rna_type']].strip()
                    
                    if self.add_provenance:
                        props['source'] = self.source
                        props['source_url'] = self.source_url
                    
                    yield rna_id, self.label, props

    def get_edges(self):
        with gzip.open(self.rfam_filepath, 'rt') as input:
            reader = csv.reader(input, delimiter='\t')
            for line in reader:
                rna_id, go_term, rfam = line
                
                if not rna_id.endswith('_9606'):
                    continue
                
                rna_id = rna_id.split('_')[0]

                # Skip if subontology doesn't match
                if self.subontology and self.subontology_mapping:
                    go_subontology = self.subontology_mapping.get(go_term)
                    if go_subontology != self.subontology:
                        continue

                edge = (rna_id, go_term, self.label)
                if edge in self.seen_edges:
                    continue
                self.seen_edges.add(edge)

                props = {}
                if self.write_properties:
                    props['rfam'] = rfam
                    if self.add_provenance:
                        props['source'] = self.source
                        props['source_url'] = self.source_url
                
                yield rna_id, go_term, self.label, props