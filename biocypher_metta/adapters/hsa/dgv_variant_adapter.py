import gzip
import numpy as np
from ncls import NCLS
from biocypher_metta.adapters import Adapter
from biocypher_metta.adapters.helpers import build_regulatory_region_id, check_genomic_location
from biocypher._logger import logger
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
        opener = gzip.open if self.filepath.endswith('.gz') else open
        with opener(self.filepath, 'rt') as f:
            next(f)
            for line in f:
                data = line.strip().split(self.delimiter)
                variant_accession = data[DGVVariantAdapter.INDEX['variant_accession']]
                chr = 'chr' + data[DGVVariantAdapter.INDEX['chr']]
                start = int(data[DGVVariantAdapter.INDEX['coord_start']]) + 1 # +1 since it is 0-indexed genomic coordinate
                end = int(data[DGVVariantAdapter.INDEX['coord_end']])
                variant_type = data[DGVVariantAdapter.INDEX['type']]
                pubmedid = data[DGVVariantAdapter.INDEX['pubmedid']]
                region_id = f"DGV:{build_regulatory_region_id(chr, start, end)}"
                if not check_genomic_location(self.chr, self.start, self.end, chr, start, end):
                    continue
                props = {}

                if self.write_properties:
                    props['variant_accession'] = variant_accession
                    props['chr'] = chr
                    props['start'] = start
                    props['end'] = end
                    props['variant_type'] = variant_type
                    props['evidence'] = 'PMID_' + pubmedid

                    if self.add_provenance:
                        props['source'] = self.source
                        props['source_url'] = self.source_url

                yield region_id, self.label, props

    def get_edges(self):
        if not self.feature_files:
            raise FileNotFoundError("Feature files for overlap calculation not provided in configuration.")

        # ── Phase 1: load all feature files into per-chromosome lists ────────
        # Features (~3-4M total across all types) fit comfortably in memory and
        # loading them upfront lets us stream the DGV file exactly once.
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

        # ── Phase 2: stream DGV file once, process one chromosome at a time ──
        # DGV is chromosome-sorted so we buffer SVs for the current chromosome
        # and flush when it changes. Peak memory = all features (~400 MB) +
        # one chromosome's SVs (~50 MB) instead of all SVs at once.
        logger.info("Streaming DGV file for overlap calculation...")
        current_chr = None
        sv_ids_buf, sv_starts_buf, sv_ends_buf = [], [], []

        for sv_id, chr, start, end, _label in self._parse_dgv(self.filepath):
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

    def _parse_dgv(self, path):
        opener = gzip.open if path.endswith('.gz') else open
        with opener(path, 'rt') as f:
            next(f)
            for line in f:
                data = line.strip().split(self.delimiter)
                chr = 'chr' + data[DGVVariantAdapter.INDEX['chr']]
                start = int(data[DGVVariantAdapter.INDEX['coord_start']]) + 1
                end = int(data[DGVVariantAdapter.INDEX['coord_end']])

                #Reconstruct ID
                region_id = f"DGV:{build_regulatory_region_id(chr, start, end)}"

                yield region_id, chr, start, end, self.label

    def _parse_gtf(self, path):
        opener = gzip.open if path.endswith('.gz') else open
        with opener(path, 'rt') as f:
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
                    feat_id = f"EPD:{build_regulatory_region_id(chr, start, end)}"
                elif label == 'non_coding_rna':
                    # RNAcentral ID logic: split on underscore.
                    # Official adapter DOES NOT add RNACENTRAL: prefix in get_nodes
                    feat_id = row[3].split('_')[0]
                else:
                    feat_id = row[3]

                yield feat_id, chr, start, end
