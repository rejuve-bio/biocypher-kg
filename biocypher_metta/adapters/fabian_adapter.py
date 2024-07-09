import pickle
from biocypher_metta.adapters import Adapter
from biocypher._logger import logger
from biocypher_metta.adapters.helpers import to_float
# Example fabian variant tsv input file:
# description of each column can be found in the link below:
# https://www.genecascade.org/fabian/documentation#download-format
# variant tf model_id database model_db wt_score mt_score start_wt end_wt start_mt end_mt strand_wt strand_mt prediction score
# chr16:53763996C>G.1	TFAP2A	M1838_1.02	cisbp_1.02	0.6345	0.6345	2	16	2	16	plus	plus	NA	0.0000
# chr16:53763996C>G.1	NFIL3	M1857_1.02	cisbp_1.02	0.6728	0.6728	5	15	5	15	plus	plus	NA	0.0000
# chr16:53763996C>G.1	HLF	M1875_1.02	cisbp_1.02	0.7558	0.7734	-14	-3	-3	8	minus	minus	gain	0.0373
# chr16:53763996C>G.1	NHLH1	M1880_1.02	cisbp_1.02	0.5744	0.6694	-12	-1	-7	4	plus	minus	gain	0.1517


class FabianAdapter(Adapter):
    INDEX = {'variant': 0, 'tf': 1, 'prediction': 12, 'score': 13}
    def __init__(self, filepath, hgnc_to_ensembl, dbsnp_pos_map, label, write_properties, add_provenance):
        self.filepath = filepath
        self.hgnc_to_ensembl_map = pickle.load(open(hgnc_to_ensembl, 'rb'))
        self.dbsnp_pos_map = dbsnp_pos_map
        self.label = label

        self.source = 'Fabian'
        self.version = ''
        self.source_url = 'https://www.genecascade.org/fabian/'
        super(FabianAdapter, self).__init__(write_properties, add_provenance)
    
    def get_rsid(self, variant_info):
        chr = variant_info.split(':')[0]
        pos = variant_info.split(':')[1].split('>')[0][:-1]
        return self.dbsnp_pos_map.get(f"{chr}_{pos}", None)
    
    def get_edges(self):
        with open(self.filepath) as f:
            for line in f:
                data = line.strip().split('\t')
                effect = data[FabianAdapter.INDEX['prediction']]
                if effect == 'NA':
                    continue
                variant_info = data[FabianAdapter.INDEX['variant']]
                variant_rsid = self.get_rsid(variant_info)
                tf = data[FabianAdapter.INDEX['tf']]
                tf_ensembl = self.hgnc_to_ensembl_map.get(tf)
                if variant_rsid == None:
                    logger.warning(f"Couldn't find rsid for variant {variant_info}")
                    continue
                if tf_ensembl == None:
                    continue
                score = data[FabianAdapter.INDEX['score']]
                
                props = {}
                if self.write_properties:
                    props['effect'] = effect
                    props['score'] = to_float(score)
                    if self.add_provenance:
                        props['source'] = self.source
                        props['source_url'] = self.source_url

                yield tf_ensembl, variant_rsid, self.label, props
        