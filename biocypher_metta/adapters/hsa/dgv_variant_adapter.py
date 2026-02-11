import gzip
from biocypher_metta.adapters import Adapter
from biocypher_metta.adapters.helpers import build_regulatory_region_id, check_genomic_location
# Example dgv input file:
# variantaccession	chr	start	end	varianttype	variantsubtype	reference	pubmedid	method	platform	mergedvariants	supportingvariants	mergedorsample	frequency	samplesize	observedgains	observedlosses	cohortdescription	genes	samples
# dgv1n82	1	10001	22118	CNV	duplication	Sudmant_et_al_2013	23825009	Oligo aCGH,Sequencing			nsv945697,nsv945698	M		97	10	0		""	HGDP00456,HGDP00521,HGDP00542,HGDP00665,HGDP00778,HGDP00927,HGDP00998,HGDP01029,HGDP01284,HGDP01307
# nsv7879	1	10001	127330	CNV	gain+loss	Perry_et_al_2008	18304495	Oligo aCGH			nssv14786,nssv14785,nssv14773,nssv14772,nssv14781,nssv14771,nssv14775,nssv14762,nssv14764,nssv18103,nssv14766,nssv14770,nssv14777,nssv14789,nssv14782,nssv14788,nssv18117,nssv14790,nssv14791,nssv14784,nssv14776,nssv14787,nssv21423,nssv14783,nssv14763,nssv14780,nssv14774,nssv14768,nssv18113,nssv18093	M		31	25	1		""	NA07029,NA07048,NA10839,NA10863,NA12155,NA12802,NA12872,NA18502,NA18504,NA18517,NA18537,NA18552,NA18563,NA18853,NA18860,NA18942,NA18972,NA18975,NA18980,NA19007,NA19132,NA19144,NA19173,NA19221,NA19240
# nsv482937	1	10001	2368561	CNV	loss	Iafrate_et_al_2004	15286789	BAC aCGH,FISH			nssv2995976	M		39	0	1		""	

class DGVVariantAdapter(Adapter):
    INDEX = {'variant_accession': 0, 'chr': 1, 'coord_start': 2, 'coord_end': 3, 'type': 5, 'pubmedid': 7, 'genes': 17}

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

        self.source = 'dgv'
        self.version = ''
        self.source_url = 'http://dgv.tcag.ca/dgv/app/downloads'

        super(DGVVariantAdapter, self).__init__(write_properties, add_provenance)

    def get_nodes(self):
        with gzip.open(self.filepath, 'rt') as f:
            next(f)
            for line in f:
                data = line.strip().split(self.delimiter)
                variant_accession = data[DGVVariantAdapter.INDEX['variant_accession']]
                chr = 'chr' + data[DGVVariantAdapter.INDEX['chr']]
                start = int(data[DGVVariantAdapter.INDEX['coord_start']]) + 1 # +1 since it is 0-indexed genomic coordinate
                end = int(data[DGVVariantAdapter.INDEX['coord_end']])
                variant_type = data[DGVVariantAdapter.INDEX['type']]
                pubmedid = data[DGVVariantAdapter.INDEX['pubmedid']]
                #Using SO:0001059 as the CURIE prefix for structural variants since dbVar lacks a standard ID format
                region_id =f"SO:0001059_{build_regulatory_region_id(chr, start, end)}"
                if not check_genomic_location(self.chr, self.start, self.end, chr, start, end):
                    continue
                props = {}

                if self.write_properties:
                    props['variant_accession'] = variant_accession
                    props['chr'] = chr
                    props['start'] = start
                    props['end'] = end
                    props['variant_type'] = variant_type
                    props['evidence'] = 'pubmed:'+pubmedid

                    if self.add_provenance:
                        props['source'] = self.source
                        props['source_url'] = self.source_url


                yield region_id, self.label, props

    def get_edges(self):
        if not self.feature_files:
            raise FileNotFoundError("Feature files for overlap calculation not provided in configuration.")
            
        svs = {}
        for sv_id, chr, start, end, label in self._parse_dgv(self.filepath):
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

    def _parse_dgv(self, path):
        with gzip.open(path, 'rt') as f:
            next(f)
            for line in f:
                data = line.strip().split(self.delimiter)
                chr = 'chr' + data[DGVVariantAdapter.INDEX['chr']]
                start = int(data[DGVVariantAdapter.INDEX['coord_start']]) + 1 
                end = int(data[DGVVariantAdapter.INDEX['coord_end']])
                
                #Reconstruct ID
                region_id = f"SO:0001059_{build_regulatory_region_id(chr, start, end)}"
                
                yield region_id, chr, start, end, self.label

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
