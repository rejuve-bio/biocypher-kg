import gzip
import csv
from io import StringIO
from math import log10
from biocypher_metta.adapters import Adapter
from biocypher_metta.adapters.helpers import build_regulatory_region_id, check_genomic_location, to_float
from biocypher_metta.processors import HGNCProcessor

# Human data:
# https://genome.ucsc.edu/cgi-bin/hgTables

# Example data
# Description for each field can be found here: http://genome.ucsc.edu/cgi-bin/hgTables
#bin,chrom,chromStart,chromEnd,name,score,sourceCount,sourceIds,sourceScores
# "1","chr1","8388344","8388800","ZBTB33","341","1","471","341"
# "1","chr1","16776930","16777306","ATF2","113","1","274","113"
# "1","chr1","58719516","58720395","ZFX","235","1","428","235"
# "3","chr1","176160080","176160980","CTCF","101","1","1155","101"

# Fly data (ENCODE modERN):
# https://www.encodeproject.org/
# Tab-separated with header row, one row per peak.
# TF names mapped to FlyBase gene IDs via a separate map file.
# chr  start  end  tf  score  signalValue  pValue  qValue  peak  file_accession  biosample  output_type

# Mouse data:
# https://genome.ucsc.edu/cgi-bin/hgTables



class TfbsAdapter(Adapter):
    INDEX = {'bin': 0, 'chr': 1, 'start': 2, 'end': 3, 'tf': 4, 'score': 5}
    MODERN_COLUMNS = {'chr': 'chr', 'start': 'start', 'end': 'end', 'tf': 'tf', 'score': 'score'}

    def __init__(self, write_properties, add_provenance, filepath,
                 hgnc_to_ensembl=None, label=None, taxon_id=9606, chr=None, start=None, end=None,
                 hgnc_processor=None,
                 input_format='ucsc_cluster', tf_map_path=None,
                 score_column=None, score_transform=None,
                 assembly='GRCh38', gene_prefix='ENSEMBL',
                 region_prefix='ENCODE_TFBS',
                 source=None, source_url=None):
        self.filepath = filepath
        self.input_format = input_format
        self.assembly = assembly
        self.gene_prefix = gene_prefix
        self.region_prefix = region_prefix
        self.chr = chr
        self.start = start
        self.end = end
        self.label = label
        self.taxon_id = taxon_id

        self.score_column = score_column or 'score'
        if score_transform is None:
            self.score_transform = 'divide_1000' if input_format == 'ucsc_cluster' else 'raw'
        else:
            self.score_transform = score_transform

        if source is not None:
            self.source = source
        else:
            self.source = 'ENCODE'

        if source_url is not None:
            self.source_url = source_url
        elif input_format == 'modern_tsv':
            self.source_url = 'https://www.encodeproject.org/'
        else:
            self.source_url = 'https://genome.ucsc.edu/cgi-bin/hgTables'

        self._tf_map = None
        self.tf_map_path = tf_map_path

        if input_format == 'ucsc_cluster':
            # Use provided processor or create new one
            if hgnc_processor is not None:
                self.hgnc_processor = hgnc_processor
            else:
                self.hgnc_processor = HGNCProcessor()
                self.hgnc_processor.load_or_update()
        else:
            self.hgnc_processor = None

        super(TfbsAdapter, self).__init__(write_properties, add_provenance)

    def _read_csv_gz(self):
        # Real source file is tab-delimited with no header row (UCSC table
        # export) — sourceIds/sourceScores columns contain embedded commas,
        # so a comma-delimited reader shreds nearly every row.
        with gzip.open(self.filepath, 'rt') as f:
            reader = csv.reader(f, delimiter='\t')

            for row in reader:
                if row:
                    yield row

    def _read_modern_tsv(self):
        open_func = gzip.open if self.filepath.endswith('.gz') else open
        with open_func(self.filepath, 'rt') as f:
            reader = csv.DictReader(f, delimiter='\t')
            for row in reader:
                if row:
                    yield row

    def _load_tf_map(self):
        if self._tf_map is not None:
            return self._tf_map

        # Tab-separated: tf_name \t fbgn_id[;fbgn_id...]
        self._tf_map = {}
        with open(self.tf_map_path, 'r') as f:
            reader = csv.reader(f, delimiter='\t')
            for row in reader:
                if len(row) >= 2:
                    tf_name = row[0].strip()
                    gene_ids = [g.strip() for g in row[1].split(';') if g.strip()]
                    self._tf_map[tf_name] = gene_ids
        return self._tf_map

    def _transform_score(self, raw_score):
        val = to_float(raw_score)
        if self.score_transform == 'divide_1000':
            return val / 1000
        elif self.score_transform == 'log10':
            return log10(val) if val > 0 else 0.0
        else:
            return val

    def get_nodes(self):
        if self.input_format == 'modern_tsv':
            for row in self._read_modern_tsv():
                chr = row[self.MODERN_COLUMNS['chr']]
                start = int(row[self.MODERN_COLUMNS['start']])
                end = int(row[self.MODERN_COLUMNS['end']])
                tfbs_id = f"{self.region_prefix}:{build_regulatory_region_id(chr, start, end, self.assembly)}"
                props = {}

                if check_genomic_location(self.chr, self.start, self.end, chr, start, end):
                    if self.write_properties:
                        props['chr'] = chr
                        props['start'] = start
                        props['end'] = end
                        props['taxon_id'] = f'{self.taxon_id}'
                        if self.add_provenance:
                            props['source'] = self.source
                            props['source_url'] = self.source_url

                    yield tfbs_id, self.label, props
        else:
            for data in self._read_csv_gz():
                chr = data[TfbsAdapter.INDEX['chr']].strip('"')
                start = int(data[TfbsAdapter.INDEX['start']].strip('"'))
                end = int(data[TfbsAdapter.INDEX['end']].strip('"'))
                tfbs_id = f"{self.region_prefix}:{build_regulatory_region_id(chr, start, end, self.assembly)}"
                props = {}

                if check_genomic_location(self.chr, self.start, self.end, chr, start, end):
                    if self.write_properties:
                        props['chr'] = chr
                        props['start'] = start
                        props['end'] = end
                        props['taxon_id'] = f'{self.taxon_id}'
                        if self.add_provenance:
                            props['source'] = self.source
                            props['source_url'] = self.source_url

                    yield tfbs_id, self.label, props

    def get_edges(self):
        if self.input_format == 'modern_tsv':
            tf_map = self._load_tf_map()

            for row in self._read_modern_tsv():
                chr = row[self.MODERN_COLUMNS['chr']]
                start = int(row[self.MODERN_COLUMNS['start']])
                end = int(row[self.MODERN_COLUMNS['end']])
                tf = row[self.MODERN_COLUMNS['tf']]

                gene_ids = tf_map.get(tf)
                if not gene_ids:
                    continue

                tfbs_id = f"{self.region_prefix}:{build_regulatory_region_id(chr, start, end, self.assembly)}"

                score_col = self.score_column if self.score_column in row else 'score'
                score = self._transform_score(row[score_col])

                if not check_genomic_location(self.chr, self.start, self.end, chr, start, end):
                    continue

                for gene_id in gene_ids:
                    gene_curie = f"{self.gene_prefix}:{gene_id}"
                    props = {}
                    if self.write_properties:
                        props['score'] = score
                        props['taxon_id'] = f'{self.taxon_id}'
                        if self.add_provenance:
                            props['source'] = self.source
                            props['source_url'] = self.source_url

                    yield gene_curie, tfbs_id, self.label, props
        else:
            for data in self._read_csv_gz():
                chr = data[TfbsAdapter.INDEX['chr']].strip('"')
                start = int(data[TfbsAdapter.INDEX['start']].strip('"'))
                end = int(data[TfbsAdapter.INDEX['end']].strip('"'))
                tf = data[TfbsAdapter.INDEX['tf']].strip('"')
                ensembl_id = self.hgnc_processor.get_ensembl_id(tf)
                if ensembl_id is None:
                    continue
                tf_ensembl = f"{self.gene_prefix}:{ensembl_id}"
                tfbs_id = f"{self.region_prefix}:{build_regulatory_region_id(chr, start, end, self.assembly)}"

                score_str = data[TfbsAdapter.INDEX['score']].strip('"')
                score = self._transform_score(score_str)

                props = {}

                if check_genomic_location(self.chr, self.start, self.end, chr, start, end):
                    if self.write_properties:
                        props['score'] = score
                        props['taxon_id'] = f'{self.taxon_id}'
                        if self.add_provenance:
                            props['source'] = self.source
                            props['source_url'] = self.source_url

                    yield tf_ensembl, tfbs_id, self.label, props