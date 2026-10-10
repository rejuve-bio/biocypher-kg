import gzip
import numpy as np
from ncls import NCLS
from biocypher_metta.adapters import Adapter
from biocypher._logger import logger

# RNAcentral doesn't publish a direct ncRNA->gene cross-reference for most
# species, but its per-species BED file (genome_coordinates/bed/*.bed.gz)
# carries genomic coordinates for every ncRNA — so a gene edge is computable
# by overlapping those intervals against a GTF's gene features. Mirrors the
# existing DGV/dbVar overlap pattern (biocypher_metta/adapters/hsa/dgv_variant_adapter.py)
# but is taxon-aware (works for dmel/cel's FlyBase/WormBase gene ids too,
# not just hsa/mmu/rno's Ensembl ids) and, since both input files are small
# per species (tens of MB), loads both fully into memory rather than
# streaming one chromosome-sorted file against buffered features.


class RnaCentralGeneOverlapAdapter(Adapter):
    def __init__(self, bed_filepath, gtf_filepath, write_properties, add_provenance,
                 taxon_id, label='ncrna_overlaps_gene', reverse_label='gene_overlaps_ncrna'):
        self.bed_filepath = bed_filepath
        self.gtf_filepath = gtf_filepath
        self.taxon_id = taxon_id
        self.label = label
        self.reverse_label = reverse_label

        self.source = 'RNAcentral'
        self.source_url = 'https://rnacentral.org/'
        super(RnaCentralGeneOverlapAdapter, self).__init__(write_properties, add_provenance)

    def _opener(self, path):
        return gzip.open(path, 'rt') if str(path).endswith('.gz') else open(path, 'rt')

    def _load_genes_by_chrom(self):
        prefix = Adapter.CURIE_PREFIX[self.taxon_id]
        genes = {}
        with self._opener(self.gtf_filepath) as f:
            for line in f:
                if line.startswith('#'):
                    continue
                parts = line.rstrip('\n').split('\t')
                if len(parts) < 9 or parts[2] != 'gene':
                    continue
                chr_ = parts[0] if parts[0].startswith('chr') else f'chr{parts[0]}'
                start, end = int(parts[3]), int(parts[4])

                info = {}
                for field in parts[8].strip().split(';'):
                    field = field.strip()
                    if not field:
                        continue
                    kv = field.split(' ', 1)
                    if len(kv) == 2:
                        info[kv[0]] = kv[1].strip().strip('"')
                gene_id = info.get('gene_id')
                if not gene_id:
                    continue
                gene_id = gene_id.split('.')[0]

                bucket = genes.setdefault(chr_, {'ids': [], 'starts': [], 'ends': []})
                bucket['ids'].append(f"{prefix}:{gene_id}")
                bucket['starts'].append(start)
                bucket['ends'].append(end)
        return genes

    def _load_ncrna_by_chrom(self):
        ncrna = {}
        with self._opener(self.bed_filepath) as f:
            for line in f:
                parts = line.rstrip('\n').split('\t')
                if len(parts) < 4:
                    continue
                chr_ = parts[0] if parts[0].startswith('chr') else f'chr{parts[0]}'
                start = int(parts[1].strip()) + 1  # BED is 0-indexed
                end = int(parts[2].strip())
                rna_id = parts[3].split('_')[0]

                bucket = ncrna.setdefault(chr_, {'ids': [], 'starts': [], 'ends': []})
                bucket['ids'].append(rna_id)
                bucket['starts'].append(start)
                bucket['ends'].append(end)
        return ncrna

    def get_edges(self):
        genes_by_chrom = self._load_genes_by_chrom()
        ncrna_by_chrom = self._load_ncrna_by_chrom()
        logger.info(
            f"rna_central overlap: {sum(len(v['ids']) for v in genes_by_chrom.values())} genes, "
            f"{sum(len(v['ids']) for v in ncrna_by_chrom.values())} ncRNAs"
        )

        for chr_, genes in genes_by_chrom.items():
            ncrna = ncrna_by_chrom.get(chr_)
            if not ncrna or not ncrna['ids'] or not genes['ids']:
                continue

            gene_starts = np.array(genes['starts'], dtype=np.int64)
            gene_ends = np.array(genes['ends'], dtype=np.int64)
            gene_idx = np.arange(len(genes['ids']), dtype=np.int64)
            # NCLS uses half-open intervals [begin, end) — add 1 to include the last base
            tree = NCLS(gene_starts, gene_ends + 1, gene_idx)

            ncrna_starts = np.array(ncrna['starts'], dtype=np.int64)
            ncrna_ends = np.array(ncrna['ends'], dtype=np.int64)
            ncrna_idx = np.arange(len(ncrna['ids']), dtype=np.int64)

            # tree was built from genes; query is ncRNA — all_overlaps_both
            # returns (query_idx, tree_idx), i.e. (ncrna_idx, gene_idx) here.
            ni_arr, gi_arr = tree.all_overlaps_both(ncrna_starts, ncrna_ends + 1, ncrna_idx)

            for gi, ni in zip(gi_arr, ni_arr):
                gene_id = genes['ids'][gi]
                rna_id = ncrna['ids'][ni]

                props = {}
                if self.write_properties:
                    props = {
                        'overlap_start': max(genes['starts'][gi], ncrna['starts'][ni]),
                        'overlap_end': min(genes['ends'][gi], ncrna['ends'][ni]),
                        'taxon_id': self.taxon_id,
                    }
                    if self.add_provenance:
                        props['source'] = self.source
                        props['source_url'] = self.source_url

                yield rna_id, gene_id, self.label, props
                yield gene_id, rna_id, self.reverse_label, props
