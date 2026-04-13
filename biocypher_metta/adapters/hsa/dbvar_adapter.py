import gzip
import numpy as np
from ncls import NCLS
from biocypher_metta.adapters import Adapter
from biocypher_metta.adapters.helpers import check_genomic_location, build_regulatory_region_id
from biocypher._logger import logger
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

        # ── Phase 1: load all feature files into per-chromosome lists ────────
        # Features (~3-4M total across all types) fit comfortably in memory and
        # loading them upfront lets us stream the 32M-entry VCF exactly once.
        feat_by_chrom = {}  # label → chr → {'ids': [], 'starts': [], 'ends': []}
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

            logger.info(f"Loading feature file: {path}")
            chrom_feats = {}
            for feat_id, chr, start, end in iterator:
                if chr not in chrom_feats:
                    chrom_feats[chr] = {'ids': [], 'starts': [], 'ends': []}
                chrom_feats[chr]['ids'].append(feat_id)
                chrom_feats[chr]['starts'].append(start)
                chrom_feats[chr]['ends'].append(end)
            feat_by_chrom[label] = chrom_feats
            logger.info(f"Loaded {label} features across {len(chrom_feats)} chromosomes")

        # ── Phase 2: stream VCF once, process one chromosome at a time ───────
        # The VCF is chromosome-sorted so we buffer SVs for the current
        # chromosome and flush when it changes. Peak memory = all features
        # (~400 MB) + one chromosome's SVs (~125 MB) instead of all 32M SVs.
        logger.info("Streaming VCF for overlap calculation...")
        current_chr = None
        sv_ids_buf, sv_starts_buf, sv_ends_buf = [], [], []

        for sv_id, chr, start, end, _label in self._parse_vcf(self.filepath):
            if not check_genomic_location(self.chr, self.start, self.end, chr, start, end):
                continue
            if chr != current_chr:
                yield from self._query_overlaps_ncls(
                    current_chr, sv_ids_buf, sv_starts_buf, sv_ends_buf, feat_by_chrom
                )
                current_chr = chr
                sv_ids_buf, sv_starts_buf, sv_ends_buf = [], [], []
            sv_ids_buf.append(sv_id)
            sv_starts_buf.append(start)
            sv_ends_buf.append(end)

        # flush last chromosome
        yield from self._query_overlaps_ncls(
            current_chr, sv_ids_buf, sv_starts_buf, sv_ends_buf, feat_by_chrom
        )

    def _query_overlaps_ncls(self, chr, sv_ids, sv_starts, sv_ends, feat_by_chrom):
        if not sv_ids or chr is None:
            return

        logger.info(f"Processing overlaps for {chr} ({len(sv_ids):,} SVs)")

        sv_starts_arr = np.array(sv_starts, dtype=np.int64)
        sv_ends_arr   = np.array(sv_ends,   dtype=np.int64)
        sv_idx_arr    = np.arange(len(sv_ids), dtype=np.int64)

        # NCLS uses half-open intervals [begin, end) — add 1 to include the last base
        tree = NCLS(sv_starts_arr, sv_ends_arr + 1, sv_idx_arr)

        for label, chrom_feats in feat_by_chrom.items():
            if chr not in chrom_feats:
                continue
            feats = chrom_feats[chr]
            if not feats['ids']:
                continue

            feat_starts_arr = np.array(feats['starts'], dtype=np.int64)
            feat_ends_arr   = np.array(feats['ends'],   dtype=np.int64)
            feat_idx_arr    = np.arange(len(feats['ids']), dtype=np.int64)

            # Batch query: returns all overlapping (feature_idx, sv_idx) pairs
            fi_arr, si_arr = tree.all_overlaps_both(
                feat_starts_arr, feat_ends_arr + 1, feat_idx_arr
            )

            feat_to_sv_label = f"{label}_overlaps_structural_variant"
            sv_to_feat_label = f"structural_variant_overlaps_{label}"

            for fi, si in zip(fi_arr, si_arr):
                feat_id = feats['ids'][fi]
                sv_id   = sv_ids[si]

                props = {}
                if self.write_properties:
                    props = {
                        'overlap_start': max(feats['starts'][fi], sv_starts[si]),
                        'overlap_end':   min(feats['ends'][fi],   sv_ends[si])
                    }
                    if self.add_provenance:
                        props['source'] = 'Overlap calculation'

                yield feat_id, sv_id, feat_to_sv_label, props
                yield sv_id, feat_id, sv_to_feat_label, props

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

    def _parse_gtf(self, path):
        with gzip.open(path, 'rt') as f:
            for line_num, line in enumerate(f, 1):
                try:
                    if line.startswith('#'): continue
                    parts = line.strip().split('\t')

                    # GTF files must have exactly 9 tab-separated columns
                    if len(parts) < 9:
                        continue

                    if parts[2] not in ['gene', 'transcript', 'exon']: continue
                    chr = parts[0]
                    if not chr.startswith('chr'): chr = 'chr' + chr
                    start = int(parts[3])
                    end = int(parts[4])

                    info_parts = parts[8].strip().split(';')
                    info = {}
                    for part in info_parts:
                        if not part.strip(): continue
                        key_value = part.strip().split(' ', 1)  # maxsplit=1 to handle values with spaces
                        if len(key_value) >= 2:
                            key, value = key_value[0], key_value[1]
                            info[key] = value.strip().replace('"', '')

                    if parts[2] == 'gene' and 'gene_id' in info:
                        gene_id = info['gene_id'].split('.')[0] if '.' in info['gene_id'] else info['gene_id']
                        feat_id = f"ENSEMBL:{gene_id}"
                        if info['gene_id'].endswith('_PAR_Y'):
                            feat_id += '_PAR_Y'
                        yield feat_id, chr, start, end
                    elif parts[2] == 'transcript' and 'transcript_id' in info:
                        transcript_id = info['transcript_id'].split('.')[0] if '.' in info['transcript_id'] else info['transcript_id']
                        feat_id = f"ENSEMBL:{transcript_id}"
                        if info['transcript_id'].endswith('_PAR_Y'):
                            feat_id += '_PAR_Y'
                        yield feat_id, chr, start, end
                    elif parts[2] == 'exon' and 'exon_id' in info:
                        exon_id = info['exon_id'].split('.')[0] if '.' in info['exon_id'] else info['exon_id']
                        feat_id = f"ENSEMBL:{exon_id}"
                        if info['exon_id'].endswith('_PAR_Y'):
                            feat_id += '_PAR_Y'
                        yield feat_id, chr, start, end
                except Exception as e:
                    # Skip malformed lines but log the error
                    print(f"Warning: Skipping malformed GTF line {line_num} in {path}: {str(e)[:100]}")
                    continue

    def _parse_bed(self, path, delimiter, label):
        import csv
        opener = gzip.open if path.endswith('.gz') else open
        with opener(path, 'rt') as f:
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
