import gzip
import re
from collections import defaultdict

from biocypher_metta.adapters import Adapter
from biocypher_metta.adapters.helpers import (
    build_regulatory_region_id,
    check_genomic_location,
    assembly_for_taxon,
)
# Ensembl Regulatory Features (GFF3) — human and mouse.
#
# Data sources:
#   Human (GRCh38, taxon 9606):
#     https://ftp.ensembl.org/pub/release-116/regulation/homo_sapiens/GRCh38/
#     annotation/Homo_sapiens.GRCh38.regulatory_features.v116.gff3.gz
#
#   Mouse (GRCm39, taxon 10090):
#     https://ftp.ensembl.org/pub/release-116/regulation/mus_musculus/GRCm39/
#     annotation/Mus_musculus.GRCm39.regulatory_features.v116.gff3.gz
#
# GFF3 column layout (tab-separated, 1-based closed coordinates):
#   col 0: seqname    – chromosome (bare number/letter, e.g. "1", "X", "MT")
#   col 1: source     – "Ensembl"
#   col 2: feature    – regulatory type ("enhancer", "promoter", …)
#   col 3: start      – 1-based inclusive
#   col 4: end        – 1-based inclusive
#   col 5–7: score, strand, frame – always "."
#   col 8: attributes – semicolon-separated key=value pairs
#                       e.g. ID=ENSR…;gene_id=ENSG…;color=#faca00
#

class ENCODERe2GAdapter(Adapter):

    ENHANCER_FEATURE = 'enhancer'

    def __init__(self, filepath, taxon_id, write_properties, add_provenance,
                 chr=None, start=None, end=None,
                 gencode_filepath=None, **kwargs):
        self.filepath = filepath
        self.chr = chr
        self.start = start
        self.end = end
        self.taxon_id = str(taxon_id) if taxon_id is not None else None
        self._taxon_id_int = int(taxon_id) if taxon_id is not None else 9606
        self.gencode_filepath = gencode_filepath

        self._assembly = assembly_for_taxon(self._taxon_id_int)

        self.source = 'Ensembl-Regulatory'
        self.source_url = 'https://ftp.ensembl.org/pub/release-116/regulation/'

        self._gene_index = None

        super().__init__(write_properties, add_provenance)

    @staticmethod
    def _parse_gff3_attributes(attr_string: str) -> dict:
        """Parse GFF3 col 8 (semicolon-delimited ``key=value`` pairs) → dict."""
        attrs = {}
        if not attr_string or attr_string.strip() in ('.', ''):
            return attrs
        for token in attr_string.strip().rstrip(';').split(';'):
            token = token.strip()
            if '=' in token:
                key, _, value = token.partition('=')
                attrs[key.strip()] = value.strip()
        return attrs

    @staticmethod
    def _strip_version(ensembl_id: str) -> str:
        """Strip Ensembl version suffix, e.g. ``ENSG00000001.5`` → ``ENSG00000001``."""
        return ensembl_id.split('.')[0] if ensembl_id else ensembl_id

    @staticmethod
    def _normalize_chr(raw_chr: str) -> str:
        """Strip ``chr`` prefix so GFF3 and GTF chromosome names compare consistently."""
        return raw_chr.lstrip('chr')

    def _open_file(self):
        """Return an open text handle for ``self.filepath``, auto-detecting gzip."""
        path = str(self.filepath)
        return gzip.open(path, 'rt') if path.endswith('.gz') else open(path, 'rt')


    def _get_gene_index(self) -> dict:
        
        if self._gene_index is not None:
            return self._gene_index

        if not self.gencode_filepath:
            return {}

        index = defaultdict(list)
        path = str(self.gencode_filepath)
        opener = gzip.open if path.endswith('.gz') else open
        gene_id_re = re.compile(r'gene_id\s+"([^"]+)"')

        with opener(path, 'rt') as fh:
            for line in fh:
                if line.startswith('#'):
                    continue
                parts = line.rstrip('\n').split('\t')
                if len(parts) < 9 or parts[2] != 'gene':
                    continue
                chrom = self._normalize_chr(parts[0])
                try:
                    g_start, g_end = int(parts[3]), int(parts[4])
                except ValueError:
                    continue
                m = gene_id_re.search(parts[8])
                if not m:
                    continue
                raw_id = self._strip_version(m.group(1))
                curie = Adapter.CURIE_PREFIX.get(self._taxon_id_int, 'ENSEMBL')
                index[chrom].append((g_start, g_end, f'{curie}:{raw_id}'))

        self._gene_index = dict(index)
        return self._gene_index

    def _find_overlapping_genes(self, chrom: str, enh_start: int, enh_end: int) -> list:

        genes = self._get_gene_index().get(chrom, [])
        return [
            gid for g_start, g_end, gid in genes
            if g_start <= enh_end and g_end >= enh_start
        ]

    def _build_props(self, extra: dict | None = None) -> dict:
        """Return the property dict common to nodes and edges."""
        if not self.write_properties:
            return {}
        props = extra.copy() if extra else {}
        props['taxon_id'] = self.taxon_id
        if self.add_provenance:
            props['source'] = self.source
            props['source_url'] = self.source_url
        return props


    def get_nodes(self):
        """Yield ``(node_id, label, props)`` for every enhancer row in the GFF3."""
        with self._open_file() as fh:
            for line in fh:
                if line.startswith('#'):
                    continue
                parts = line.rstrip('\n').split('\t')
                if len(parts) < 9 or parts[2].lower() != self.ENHANCER_FEATURE:
                    continue

                chrom = self._normalize_chr(parts[0])
                try:
                    start, end = int(parts[3]), int(parts[4])
                except ValueError:
                    continue

                if not check_genomic_location(self.chr, self.start, self.end,
                                              chrom, start, end):
                    continue

                region_id = (
                    f'ENSEMBL_REGULATORY:'
                    f'{build_regulatory_region_id(chrom, start, end, self._assembly)}'
                )

                props = self._build_props({'chr': chrom, 'start': start, 'end': end})
                yield region_id, 'enhancer', props

    def get_edges(self):

        with self._open_file() as fh:
            for line in fh:
                if line.startswith('#'):
                    continue
                parts = line.rstrip('\n').split('\t')
                if len(parts) < 9 or parts[2].lower() != self.ENHANCER_FEATURE:
                    continue

                chrom = self._normalize_chr(parts[0])
                try:
                    start, end = int(parts[3]), int(parts[4])
                except ValueError:
                    continue

                if not check_genomic_location(self.chr, self.start, self.end,
                                              chrom, start, end):
                    continue

                attrs = self._parse_gff3_attributes(parts[8])
                region_id = (
                    f'ENSEMBL_REGULATORY:'
                    f'{build_regulatory_region_id(chrom, start, end, self._assembly)}'
                )
                props = self._build_props()

                raw_gene_ids = attrs.get('gene_id', '')
                if raw_gene_ids:
                    curie = Adapter.CURIE_PREFIX.get(self._taxon_id_int, 'ENSEMBL')
                    for raw_gid in raw_gene_ids.split(','):
                        raw_gid = raw_gid.strip()
                        if not raw_gid:
                            continue
                        gene_id = f'{curie}:{self._strip_version(raw_gid)}'
                        yield region_id, gene_id, 'enhancer_gene', props
                else:
                    if not self.gencode_filepath:
                        continue
                    edge_label = 'enhancer_overlaps_gene'
                    for gene_id in self._find_overlapping_genes(chrom, start, end):
                        yield region_id, gene_id, edge_label, props
