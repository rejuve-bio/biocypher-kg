import gzip
from biocypher_metta.adapters import Adapter
from .helpers import to_float, check_genomic_location, build_variant_id

# sample data from the dataset
# chr    start   end     ref  num_alt  A   A_score_hdiv  A_pred_hdiv  A_score_hvar  A_pred_hvar  C   C_score_hdiv  C_pred_hdiv  C_score_hvar  C_pred_hvar  G   G_score_hdiv  G_pred_hdiv  G_score_hvar  G_pred_hvar  T   T_score_hdiv  T_pred_hdiv  T_score_hvar  T_pred_hvar
# chr1	69090	69091	A	1	C	0.0	B	0.0	B	G	0.0	B	0.0	B	T	0.0	B	0.0	B
# chr1	69091	69092	T	1	A	0.0	B	0.001	B	C	0.0	B	0.0	B	G	0.001	B	0.003	B
# chr1	69092	69093	G	1	A	0.0	B	0.0	B	C	0.0	B	0.0	B	T	0.0	B	0.0	B
# chr1	69093	69094	G	2	A	0.845	P	0.33	B	C	0.123	B	0.017	B	T	0.123	B	0.017	B

class PolyPhen2Adapter(Adapter):
    INDEX = {
        'chr': 0, 'start': 1, 'end': 2, 'ref': 3, 'num_alt': 4,
        'alt_a': 5, 'score_a_hdiv': 6, 'pred_a_hdiv': 7, 'score_a_hvar': 8, 'pred_a_hvar': 9,
        'alt_c': 10, 'score_c_hdiv': 11, 'pred_c_hdiv': 12, 'score_c_hvar': 13, 'pred_c_hvar': 14,
        'alt_g': 15, 'score_g_hdiv': 16, 'pred_g_hdiv': 17, 'score_g_hvar': 18, 'pred_g_hvar': 19,
        'alt_t': 20, 'score_t_hdiv': 21, 'pred_t_hdiv': 22, 'score_t_hvar': 23, 'pred_t_hvar': 24
    }

    def __init__(self, filepath, write_properties, add_provenance, label='snp',
                 chr=None, start=None, end=None):
        self.filepath = filepath
        self.chr = chr
        self.start = start
        self.end = end
        self.label = label
        self.source = 'PolyPhen-2'
        self.version = ''
        self.source_url = 'https://hgdownload.soe.ucsc.edu/gbdb/hg38/dbNsfp/dbNsfpPolyPhen2.bb'
        super(PolyPhen2Adapter, self).__init__(write_properties, add_provenance)

    def _get_prediction(self, prediction):
        if prediction == 'D':
            return 'probably damaging'
        elif prediction == 'P':
            return 'possibly damaging'
        elif prediction == 'B':
            return 'benign'
        else:
            return 'unknown'

    def get_nodes(self):
        processed_snps = set()
        with gzip.open(self.filepath, 'rt') as f:
            for line in f:
                data = line.strip().split('\t')
                chr = data[self.INDEX['chr']] 
                start = int(data[self.INDEX['start']]) + 1 # +1 since it is 0-based genomic coordinate
                end = int(data[self.INDEX['end']])
                ref = data[self.INDEX['ref']]
                
                if not check_genomic_location(self.chr, self.start, self.end, chr, start, end):
                    continue

                alt_alleles = ['A', 'C', 'G', 'T']
                for alt in alt_alleles:
                    if alt == ref:
                        continue

                    base_index = self.INDEX[f'alt_{alt.lower()}']
                    if base_index >= len(data):
                        continue

                    score_hdiv = data[self.INDEX[f'score_{alt.lower()}_hdiv']]
                    pred_hdiv = data[self.INDEX[f'pred_{alt.lower()}_hdiv']]
                    score_hvar = data[self.INDEX[f'score_{alt.lower()}_hvar']]
                    pred_hvar = data[self.INDEX[f'pred_{alt.lower()}_hvar']]

                    if score_hdiv == '.' or pred_hdiv == '.' or score_hvar == '.' or pred_hvar == '.':
                        continue

                    try:
                        score_hdiv = to_float(score_hdiv)
                        score_hvar = to_float(score_hvar)
                    except ValueError:
                        continue

                    node_id = build_variant_id(chr, start, ref, alt)
                    
                    # Skip if we've already processed this SNP
                    if node_id in processed_snps:
                        continue
                    processed_snps.add(node_id)

                    props = {}
                    if self.write_properties:
                        props['ref'] = ref
                        props['alt'] = alt
                        props['polyphen2_humdiv_score'] = score_hdiv
                        props['polyphen2_humdiv_prediction'] = self._get_prediction(pred_hdiv)
                        props['polyphen2_humvar_score'] = score_hvar
                        props['polyphen2_humvar_prediction'] = self._get_prediction(pred_hvar)
                    
                    if self.add_provenance:
                        props['source'] = self.source
                        props['source_url'] = self.source_url

                    yield node_id, self.label, props