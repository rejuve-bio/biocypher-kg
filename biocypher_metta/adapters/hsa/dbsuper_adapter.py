from collections import defaultdict
import csv
import gzip
import pickle
from biocypher_metta.adapters import Adapter
from biocypher_metta.processors import HGNCProcessor
from biocypher_metta.adapters.helpers import build_regulatory_region_id, check_genomic_location
# Input: dbSUPER_SuperEnhancers_hg38.tsv.gz, produced by the downloader (liftover: block in
# hsa_data_source_config.yaml) from the hg19-only dbSUPER file with UCSC liftOver
# (hg19ToHg38.over.chain.gz, -minMatch 0.90). chrom/start/stop hold the hg38 interval; the
# original hg19 interval is appended as orig_chrom/orig_start/orig_end. Rows that did not lift
# are already dropped (listed in <file>.unmapped.tsv).
# Coordinates: BED-style, 0-based half-open (start), so start + 1 is stored.
# Original hg19 file, for reference:
# chrom	 start	 stop	 se_id	 gene_symbol	 cell_name	 rank
# chr1	120485363	120615071	SE_00001	NOTCH2	Adipose Nuclei	1
# chr13	110838836	111112228	SE_00002	COL4A2	Adipose Nuclei	2

class DBSuperAdapter(Adapter):
    # Coordinate convention of the source file(s); see README 'Genomic coordinates'.
    # Stored KG position = source position + offset.
    COORDINATES = {
        'source_convention': '0-based half-open',
        'source_assembly': 'hg19',
        'start_offset': 1,
        'end_offset': 0,
        'liftover': 'hg19 -> GRCh38 by the downloader (UCSC liftOver, minMatch 0.90); the adapter reads the lifted file',
        'note': 'Undocumented by dbSUPER; BED-like (MACS/ROSE output)',
    }

    INDEX = {'chr': 0, 'coord_start': 1, 'coord_end': 2, 'se_id': 3, 'gene_id': 4, 'cell_name': 5}

    def __init__(self, filepath, hgnc_to_ensembl_map=None, dbsuper_tissues_map=None, label='super_enhancer',
                 write_properties=None, add_provenance=None,
                 type='super enhancer', delimiter='\t',
                 chr=None, start=None, end=None, hgnc_processor=None, taxon_id=9606):
        self.filePath = filepath
        self.taxon_id = taxon_id

        # Use provided processor or create new one
        if hgnc_processor is not None:
            self.hgnc_processor = hgnc_processor
        else:
            self.hgnc_processor = HGNCProcessor()
            self.hgnc_processor.load_or_update()
        self.dbsuper_tissues_map = pickle.load(open(dbsuper_tissues_map, 'rb'))
        self.type = type
        self.delimiter = delimiter
        self.chr = chr
        self.start = start
        self.end = end
        self.label = label

        self.source = 'dbSuper'
        self.version = ''
        self.source_url = 'https://asntech.org/dbsuper/download.php'

        super(DBSuperAdapter, self).__init__(write_properties, add_provenance)


    def get_nodes(self):
        with gzip.open(self.filePath, 'rt') as f:
            reader = csv.reader(f, delimiter=self.delimiter)
            next(reader)
            for line in reader:
                se_id = line[DBSuperAdapter.INDEX['se_id']]
                chr = line[DBSuperAdapter.INDEX['chr']]
                start = int(line[DBSuperAdapter.INDEX['coord_start']]) + 1 # +1 since it is 0-based genomic coordinate
                end = int(line[DBSuperAdapter.INDEX['coord_end']])
                #CURIE ID For super enhancer region
                #"SO" provides standardized terms for genomic features, including regulatory regions  
                se_region_id = f"SO:{build_regulatory_region_id(chr, start, end)}"
                if check_genomic_location(self.chr, self.start, self.end, chr, start, end):
                    props = {}
                    if self.write_properties:
                        props['taxon_id'] = self.taxon_id
                        props['se_id'] = se_id
                        props['chr'] = chr
                        props['start'] = start
                        props['end'] = end
                        if self.add_provenance:
                            props['source'] = self.source
                            props['source_url'] = self.source_url

                    yield se_region_id, self.label, props

    
    def get_edges(self):
        with gzip.open(self.filePath, 'rt') as f:
            reader = csv.reader(f, delimiter=self.delimiter)
            next(reader)
            for line in reader:
                gene_id = line[DBSuperAdapter.INDEX['gene_id']]
                #CURIE ID For gene - Get Ensembl ID from HGNC symbol
                ensembl_id = self.hgnc_processor.get_ensembl_id(gene_id)
                if ensembl_id is None:
                    continue
                ensembl_gene_id = f"ENSEMBL:{ensembl_id}"
                chr = line[DBSuperAdapter.INDEX['chr']]
                start = int(line[DBSuperAdapter.INDEX['coord_start']]) + 1 # +1 since it is 0-based genomic coordinate
                end = int(line[DBSuperAdapter.INDEX['coord_end']])
                cell_name = line[DBSuperAdapter.INDEX['cell_name']]
                biological_id = self.dbsuper_tissues_map[cell_name]
                
                if ensembl_gene_id is None:
                    continue
                se_region_id = f"SO:{build_regulatory_region_id(chr, start, end)}"
                if check_genomic_location(self.chr, self.start, self.end, chr, start, end):
                    props = {}
                    if self.write_properties:
                        props['taxon_id'] = self.taxon_id
                        props['biological_context'] = biological_id
                        if self.add_provenance:
                            props['source'] = self.source
                            props['source_url'] = self.source_url

                    yield se_region_id, ensembl_gene_id, self.label, props