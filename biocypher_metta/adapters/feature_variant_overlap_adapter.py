import gzip
import csv
import re
from biocypher_metta.adapters import Adapter

#adapter to find overlaps between genomic features and structural variants
class FeatureVariantOverlapAdapter(Adapter):
    def __init__(self, feature_files, sv_files, write_properties, add_provenance, taxon_id):
        self.feature_files = feature_files
        self.sv_files = sv_files
        self.taxon_id = taxon_id
        super(FeatureVariantOverlapAdapter, self).__init__(write_properties, add_provenance)

    def get_edges(self):
        svs = {}
        for sv_config in self.sv_files:
            path = sv_config['path']
            label = sv_config['label']
            file_type = sv_config['type']
            
            if file_type == 'vcf':
                for sv_id, chr, start, end in self._parse_vcf(path):
                    if chr not in svs: svs[chr] = []
                    svs[chr].append({'id': sv_id, 'start': start, 'end': end, 'label': label})
            elif file_type == 'bed' or file_type == 'txt':
                for sv_id, chr, start, end in self._parse_bed_or_txt(path):
                    if chr not in svs: svs[chr] = []
                    svs[chr].append({'id': sv_id, 'start': start, 'end': end, 'label': label})

        for feat_config in self.feature_files:
            path = feat_config['path']
            label = feat_config['label']
            file_type = feat_config['type']
            
            if file_type == 'gtf':
                iterator = self._parse_gtf(path)
            elif file_type == 'bed':
                iterator = self._parse_bed(path)
            else:
                continue
                
            for feat_id, chr, start, end in iterator:
                if chr in svs:
                    for sv in svs[chr]:
                        if self._check_overlap(start, end, sv['start'], sv['end']):
                            props = {
                                'overlap_start': max(start, sv['start']),
                                'overlap_end': min(end, sv['end']),
                                'taxon_id': str(self.taxon_id)
                            }
                            if self.add_provenance:
                                props['source'] = 'Overlap calculation'
                            
                            yield feat_id, sv['id'], "feature_located_in_structural_variant", props
                            yield sv['id'], feat_id, "structural_variant_located_in_feature", props

    def _check_overlap(self, s1, e1, s2, e2):
        return max(s1, s2) <= min(e1, e2)

    def _parse_vcf(self, path):
        with gzip.open(path, 'rt') as f:
            for line in f:
                if line.startswith('#'): continue
                parts = line.split('\t')
                chr = parts[0]
                if not chr.startswith('chr'): chr = 'chr' + chr
                start = int(parts[1])
                sv_id = parts[2]
                if sv_id == '.':
                    sv_id = f"dbVar:{parts[0]}_{parts[1]}"
                else:
                    sv_id = f"dbVar:{sv_id}"
                
                info = parts[7]
                end = start
                match = re.search(r'END=(\d+)', info)
                if match:
                    end = int(match.group(1))
                yield sv_id, chr, start, end

    def _parse_bed_or_txt(self, path):
        with gzip.open(path, 'rt') as f:
            first_line = f.readline()
            if not first_line.startswith('chr'): 
                pass
            else:
                f.seek(0)
            
            reader = csv.reader(f, delimiter='\t')
            for row in reader:
                if not row: continue
                chr = row[0]
                if not chr.startswith('chr'): chr = 'chr' + chr
                start = int(row[1]) + 1
                end = int(row[2])
                sv_id = f"DGV:{row[3]}"
                yield sv_id, chr, start, end

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
                
                if parts[2] == 'gene':
                    match = re.search(r'gene_id "(ENSH?G\d+)"', parts[8])
                    if match: yield f"ENSEMBL:{match.group(1)}", chr, start, end
                elif parts[2] == 'transcript':
                    match = re.search(r'transcript_id "(ENSH?T\d+)"', parts[8])
                    if match: yield f"ENSEMBL:{match.group(1)}", chr, start, end
                elif parts[2] == 'exon':
                    match = re.search(r'exon_id "(ENSH?E\d+)"', parts[8])
                    if match: yield f"ENSEMBL:{match.group(1)}", chr, start, end

    def _parse_bed(self, path):
        with gzip.open(path, 'rt') as f:
            reader = csv.reader(f, delimiter='\t')
            for row in reader:
                if not row: continue
                chr = row[0]
                if not chr.startswith('chr'): chr = 'chr' + chr
                start = int(row[1]) + 1 
                end = int(row[2])
                feat_id = row[3].split('_')[0]
                
                if 'URS' in feat_id:
                    feat_id = f"RNACENTRAL:{feat_id}"
                elif row[3].count('_') > 0: 
                    feat_id = f"SO:{row[0]}_{start}_{end}_GRCh38" 
                yield feat_id, chr, start, end
