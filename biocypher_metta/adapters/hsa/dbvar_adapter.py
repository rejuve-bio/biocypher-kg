import gzip
from biocypher_metta.adapters import Adapter
from biocypher_metta.adapters.helpers import check_genomic_location
# Example dbVar input file:
#CHROM	POS	ID	REF	ALT	QUAL	FILTER	INFO
# 1	10000	nssv16889290	N	<DUP>	.	.	DBVARID=nssv16889290;SVTYPE=DUP;END=52000;SVLEN=42001;EXPERIMENT=1;SAMPLESET=1;REGIONID=nsv6138160;AC=1453;AF=0.241208;AN=6026
# 1	10001	nssv14768	T	<DUP>	.	.	DBVARID=nssv14768;SVTYPE=DUP;IMPRECISE;END=88143;CIPOS=0,0;CIEND=0,0;SVLEN=78143;EXPERIMENT=1;SAMPLE=NA12155;REGIONID=nsv7879
# 1	10001	nssv14781	T	<DUP>	.	.	DBVARID=nssv14781;SVTYPE=DUP;IMPRECISE;END=82189;CIPOS=0,0;CIEND=0,0;SVLEN=72189;EXPERIMENT=1;SAMPLE=NA18860;REGIONID=nsv7879

class DBVarVariantAdapter(Adapter):
    INDEX = {'chr': 0, 'coord_start': 1, 'id': 2, 'type': 4, 'info': 7}
    VARIANT_TYPES = {'<CNV>': 'copy number variation', '<DEL>': 'deletion', '<DUP>': 'duplication', '<INS>': 'insertion', '<INV>': 'inversion'}

    def __init__(self, filepath, write_properties, add_provenance, 
                 label, delimiter='\t',
                 chr=None, start=None, end=None, feature_files=None):
        self.filepath = filepath
        self.delimiter = delimiter
        self.chr = chr
        self.start = start
        self.end = end
        self.label = label
        self.feature_files = feature_files

        self.source = 'dbVar'
        self.version = ''
        self.source_url = 'https://www.ncbi.nlm.nih.gov/dbvar/content/ftp_manifest/'

        super(DBVarVariantAdapter, self).__init__(write_properties, add_provenance)

    def get_nodes(self):
        with gzip.open(self.filepath, 'rt') as f:
            for line in f:
                if line.startswith('#'):
                    continue
                data = line.strip().split(self.delimiter)
                #Using SO:0001059 as the CURIE prefix for structural variants since dbVar lacks a standard ID format
                variant_id = f"SO:0001059_{data[DBVarVariantAdapter.INDEX['id']]}"
                variant_type_key = data[DBVarVariantAdapter.INDEX['type']]
                if variant_type_key not in DBVarVariantAdapter.VARIANT_TYPES:
                    continue
                variant_type = DBVarVariantAdapter.VARIANT_TYPES[variant_type_key]
                chr = 'chr' + data[DBVarVariantAdapter.INDEX['chr']]
                start = int(data[DBVarVariantAdapter.INDEX['coord_start']])
                info = data[DBVarVariantAdapter.INDEX['info']].split(';')
                end = start
                for i in range(len(info)):
                    if info[i].startswith('END='):
                        end = int(info[i].split('=')[1])
                        break
                
                if check_genomic_location(self.chr, self.start, self.end, chr, start, end):
                    props = {}

                    if self.write_properties:
                        props['chr'] = chr
                        props['start'] = start
                        props['end'] = end
                        props['variant_type'] = variant_type

                    if self.add_provenance:
                        props['source'] = self.source
                        props['source_url'] = self.source_url


                    yield variant_id, self.label, props

    def get_edges(self):
        if not self.feature_files:
            raise FileNotFoundError("Feature files for overlap calculation not provided in configuration.")
            
        svs = {}
        for sv_id, chr, start, end, label in self._parse_vcf(self.filepath):
            if not check_genomic_location(self.chr, self.start, self.end, chr, start, end):
                continue
            if chr not in svs: svs[chr] = []
            svs[chr].append({'id': sv_id, 'start': start, 'end': end, 'label': label})
            
        for feat_config in self.feature_files:
            path = feat_config['path']
            label = feat_config['label']
            file_type = feat_config['type']
            delimiter = feat_config.get('delimiter', '\t')
            
            if file_type == 'gtf':
                iterator = self._parse_gtf(path)
            elif file_type == 'bed':
                iterator = self._parse_bed(path, delimiter, label)
            else:
                continue
                
            for feat_id, chr, start, end in iterator:
                if chr in svs:
                    for sv in svs[chr]:
                        if self._check_overlap(start, end, sv['start'], sv['end']):
                            props = {
                                'overlap_start': max(start, sv['start']),
                                'overlap_end': min(end, sv['end'])
                            }
                            if self.add_provenance:
                                props['source'] = 'Overlap calculation'
                            
                            # Dynamic granular labels
                            feat_to_sv_label = f"{label}_located_in_structural_variant"
                            sv_to_feat_label = f"structural_variant_located_in_{label}"
                            
                            yield feat_id, sv['id'], feat_to_sv_label, props
                            yield sv['id'], feat_id, sv_to_feat_label, props

    def _parse_vcf(self, path):
        import re
        with gzip.open(path, 'rt') as f:
            for line in f:
                if line.startswith('#'): continue
                parts = line.split('\t')
                chr = parts[0]
                if not chr.startswith('chr'): chr = 'chr' + chr
                start = int(parts[1])
                sv_id = parts[2]
                
                #Reconstruct ID
                if sv_id == '.':
                    variant_id = f"SO:0001059_{parts[0]}_{parts[1]}"
                else:
                    variant_id = f"SO:0001059_{sv_id}"
                
                info = parts[7]
                end = start
                match = re.search(r'END=(\d+)', info)
                if match:
                    end = int(match.group(1))
                yield variant_id, chr, start, end, self.label

    def _check_overlap(self, s1, e1, s2, e2):
        return max(s1, s2) <= min(e1, e2)

    def _parse_gtf(self, path):
        with gzip.open(path, 'rt') as f:
            for line in f:
                if line.startswith('#'): continue
                parts = line.split('\t')
                if parts[2] not in ['gene', 'transcript', 'exon']: continue
                chr = parts[0]
                if not chr.startswith('chr'): chr = 'chr' + chr
                start = int(parts[3])
                end = int(parts[4])
                
                info_parts = parts[8].strip().split(';')
                info = {}
                for part in info_parts:
                    if not part.strip(): continue
                    key_value = part.strip().split(' ')
                    if len(key_value) == 2:
                        key, value = key_value
                        info[key] = value.replace('"', '')
                
                if parts[2] == 'gene' and 'gene_id' in info:
                    gene_id = info['gene_id'].split('.')[0]
                    feat_id = f"ENSEMBL:{gene_id}"
                    if info['gene_id'].endswith('_PAR_Y'):
                        feat_id += '_PAR_Y'
                    yield feat_id, chr, start, end
                elif parts[2] == 'transcript' and 'transcript_id' in info:
                    transcript_id = info['transcript_id'].split('.')[0]
                    feat_id = f"ENSEMBL:{transcript_id}"
                    if info['transcript_id'].endswith('_PAR_Y'):
                        feat_id += '_PAR_Y'
                    yield feat_id, chr, start, end
                elif parts[2] == 'exon' and 'exon_id' in info:
                    exon_id = info['exon_id'].split('.')[0]
                    feat_id = f"ENSEMBL:{exon_id}"
                    if info['exon_id'].endswith('_PAR_Y'):
                        feat_id += '_PAR_Y'
                    yield feat_id, chr, start, end

    def _parse_bed(self, path, delimiter, label):
        import csv
        with gzip.open(path, 'rt') as f:
            # EPD uses multiple spaces as delimiter sometimes, handle it robustly
            if delimiter == ' ':
                reader = (line.split() for line in f if not line.startswith('#'))
            else:
                reader = csv.reader(f, delimiter=delimiter)
                
            for row in reader:
                if not row or row[0].startswith('#'): continue
                chr_raw = row[0]
                chr = 'chr' + chr_raw if not chr_raw.startswith('chr') else chr_raw
                start = int(row[1]) + 1 
                end = int(row[2])
                
                if label == 'promoter':
                    # EPD ID logic: build_regulatory_region_id matches EPDAdapter
                    feat_id = f"SO:{build_regulatory_region_id(chr, start, end)}"
                elif label == 'non_coding_rna':
                    # RNAcentral ID logic: split on underscore. 
                    # Official adapter DOES NOT add RNACENTRAL: prefix in get_nodes
                    feat_id = row[3].split('_')[0]
                else:
                    feat_id = row[3]
                    
                yield feat_id, chr, start, end
