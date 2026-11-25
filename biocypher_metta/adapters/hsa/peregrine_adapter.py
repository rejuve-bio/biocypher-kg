import csv
import gzip
import pickle

from biocypher_metta.adapters import Adapter
from biocypher_metta.adapters.helpers import build_regulatory_region_id, check_genomic_location, to_float
from biocypher_metta.processors import HGNCProcessor
# Example PEREGRINE input files:

# PEREGRINEenhancershg38
# chr1	99534632	99534837	1
# chr1	99638888	99639130	2
# chr1	99673438	99673767	5

# enhancer_gene_link_18.tsv
# enhancer	gene	linkID	assay	tissue	p-value	eQTL_SNP_ID	score	Zscore	CDFscore
# 1	HUMAN|HGNC=15846|UniProtKB=Q9NP74	1	3	64		
# 1	HUMAN|HGNC=15846|UniProtKB=Q9NP74	1	3	65		
# 1	HUMAN|HGNC=15846|UniProtKB=Q9NP74	1	3	66		
# 1	HUMAN|HGNC=15846|UniProtKB=Q9NP74	1	3	67		

# PEREGRINE datasources
# 1	FANTOM
# 2	FANTOM
# 87657	Ensembl
# 87659	Ensembl
# EH37E0436909	ENCODE
# EH37E0436910	ENCODE

class PEREGRINEAdapter(Adapter):
    # Coordinate convention of the source file(s); see README 'Genomic coordinates'.
    # Verification: docs/knowledge-graph/genomic-coordinates.md (PEREGRINE section).
    # Stored KG position = source position + offset.
    COORDINATES = {
        'source_convention': 'mixed',
        'source_assembly': 'GRCh38',
        'components': {
            'FANTOM': {'source_convention': '0-based half-open', 'start_offset': 1, 'end_offset': 0},
            'ENCODE': {'source_convention': '0-based half-open', 'start_offset': 1, 'end_offset': 0},
            'Ensembl': {'source_convention': '1-based closed', 'start_offset': 0, 'end_offset': 0},
            'VISTA': {'source_convention': '1-based closed', 'start_offset': 0, 'end_offset': 0},
        },
        'note': ('Per-record sub-source from PEREGRINEenhancersources. Verified Nov 2026 by exact '
                 'positional match after hg19->hg38 liftOver (check_peregrine_coords.py): ENCODE vs '
                 'ENCFF788SJC (100%), FANTOM vs FANTOM5 permissive phase 1+2 (100%), Ensembl vs GRCh37 '
                 'Regulatory Build 20161117/release 87 (99.6% of 28,173 matched); VISTA verified against '
                 'vista-data. PEREGRINE does not document its convention; the file mixes conventions.'),
    }

    # Start offset (source -> KG 1-based closed) per PEREGRINE sub-source; must match COORDINATES['components'].
    START_OFFSET_BY_SOURCE = {'FANTOM': 1, 'ENCODE': 1, 'Ensembl': 0, 'VISTA': 0}

    ALLOWED_TYPES = ['enhancer', 'enhancer to gene association']
    ALLOWED_LABELS = ['enhancer', 'enhancer_gene']
    ALLOWED_KEYS = []
    INDEX = {'enhancer': 0, 'gene': 1, 'tissue': 4, 'score': 7, 'chr': 0, 'start': 1, 'end': 2, 'id': 3}

    def __init__(self, enhancers_file, enhancer_gene_link,
                 source_file, hgnc_ensembl_map=None,
                 tissue_ontology_map=None, write_properties=None, add_provenance=None, label='enhancer',
                 type='enhancer', delimiter='\t',
                 chr=None, start=None, end=None, hgnc_processor=None, taxon_id=9606):

        self.taxon_id = taxon_id
        self.enhancers_file = enhancers_file
        self.enhancer_gene_link = enhancer_gene_link
        self.source_file = source_file

        # Use provided processor or create new one
        if hgnc_processor is not None:
            self.hgnc_processor = hgnc_processor
        else:
            self.hgnc_processor = HGNCProcessor()
            self.hgnc_processor.load_or_update()

        self.tissue_ontology_map = pickle.load(open(tissue_ontology_map, 'rb'))
        self.type = type
        self.label = label
        self.delimiter = delimiter
        self.chr = chr
        self.start = start
        self.end = end

        self.source = 'PEREGRINE'
        self.version = ''
        self.source_url = 'https://www.peregrineproj.org/'

        super(PEREGRINEAdapter, self).__init__(write_properties, add_provenance)

    def handle_gene(self, gene):
        gene = gene.split('|')[1]
        gene = ':'.join(gene.split('='))
        return gene
        
    def _read_sources(self):
        source_map = {}
        with gzip.open(self.source_file, 'rt') as f:
            for line in f:
                enhancer_id, source = line.strip().split('\t')
                source_map[enhancer_id] = source
        return source_map

    def _read_enhancers(self, source_map):
        """Return {enhancer_id: {'chr', 'start', 'end'}} in 1-based closed coordinates."""
        enhancer_info = {}
        with gzip.open(self.enhancers_file, 'rt') as f:
            reader = csv.reader(f, delimiter=self.delimiter)
            for line in reader:
                chr = line[self.INDEX['chr']]
                start = int(line[self.INDEX['start']])
                end = int(line[self.INDEX['end']])
                enhancer_id = line[self.INDEX['id']]
                data_source = source_map.get(enhancer_id)
                if data_source not in self.START_OFFSET_BY_SOURCE:
                    raise ValueError(
                        f"PEREGRINE enhancer {enhancer_id}: unknown data source {data_source!r}; "
                        f"add its coordinate convention to START_OFFSET_BY_SOURCE and COORDINATES")
                enhancer_info[enhancer_id] = {
                    'chr': chr,
                    'start': start + self.START_OFFSET_BY_SOURCE[data_source],
                    'end': end,
                }
        return enhancer_info

    def get_nodes(self):
        source_map = self._read_sources()
        enhancer_info = self._read_enhancers(source_map)

        for enhancer_id, info in enhancer_info.items():
            data_source = source_map[enhancer_id]
            chr = info['chr']
            start = info['start']
            end = info['end']
            enhancer_region_id = f"PEREGRINE:{build_regulatory_region_id(chr, start, end)}"
            props = {}
            if check_genomic_location(self.chr, self.start, self.end, chr, start, end):
                if self.write_properties:
                    props['enhancer_id'] = enhancer_id
                    props['chr'] = chr
                    props['start'] = start
                    props['end'] = end
                    props['data_source'] = data_source
                    props['taxon_id'] = self.taxon_id

                    if self.add_provenance:
                        props['source'] = self.source
                        props['source_url'] = self.source_url
                
                yield enhancer_region_id, self.label, props

    def get_edges(self):
        enhancer_id_map = {}
        for id, info in self._read_enhancers(self._read_sources()).items():
            chr, start, end = info['chr'], info['start'], info['end']
            if check_genomic_location(self.chr, self.start, self.end, chr, start, end):
                enhancer_id_map[id] = build_regulatory_region_id(chr, start, end)

        with gzip.open(self.enhancer_gene_link, 'rt') as f:
            reader = csv.reader(f, delimiter=self.delimiter)
            next(reader)    # Skip header
            for line in reader:
                id = line[self.INDEX['enhancer']]
                if id not in enhancer_id_map:
                    continue
                enhancer_region_id = f"PEREGRINE:{enhancer_id_map[id]}"
                gene_hgnc_id = self.handle_gene(line[self.INDEX['gene']])
                # CURIE ID Format: ENSEMBL:ENSG00000123456
                ensembl_id = self.hgnc_processor.get_ensembl_id(gene_hgnc_id)
                if ensembl_id is None:
                    continue
                gene = f"ENSEMBL:{ensembl_id}"
                tissue_id = line[self.INDEX['tissue']]
                score = None
                if self.INDEX['score'] < len(line):
                    score = line[self.INDEX['score']]
                if tissue_id not in self.tissue_ontology_map:
                    continue

                props = {}
                if self.write_properties:
                    props['biological_context'] = self.tissue_ontology_map[tissue_id][0]
                    if score:
                        props['score'] = to_float(score)
                    props['taxon_id'] = self.taxon_id

                    if self.add_provenance:
                        props['source'] = self.source
                        props['source_url'] = self.source_url
                
                yield enhancer_region_id, gene, self.label, props
            
