import gzip
from biocypher_metta.adapters import Adapter
from biocypher_metta.adapters.helpers import to_float
from collections import defaultdict

# Exaple bgee tsv input file:
# Gene ID	"Gene name"	Anatomical entity ID	"Anatomical entity name"	Developmental stage ID	"Developmental stage name"	Sex	Strain	Expression	Call quality	FDR	Expression score	Expression rank
# ENSG00000000003	"TSPAN6"	CL:0000015	"male germ cell"	HsapDv:0000240	"sixth decade stage (human)"	male	White	present	gold quality	0.00221961024320294	89.08	5.09e3
# ENSG00000000003	"TSPAN6"	CL:0000019	"sperm"	UBERON:0018241	"prime adult stage"	male	wild-type	present	gold quality	0.00167287722066534	99.96	20.5
# ENSG00000000003	"TSPAN6"	CL:0000023	"oocyte"	UBERON:0000113	"post-juvenile"	female	wild-type	absent	gold quality	0.371619489632094	61.43	1.80e4
# ENSG00000000003	"TSPAN6"	CL:0000083	"epithelial cell of pancreas"	UBERON:0000104	"life cycle"	any	wild-type	present	gold quality	0.005262469333648452	83.30	7.79e3
# ENSG00000000003	"TSPAN6"	CL:0000089 ∩ UBERON:0000473	"male germ line stem cell (sensu Vertebrata) in testis"	UBERON:0000104	"life cycle"	male	wild-type	present	gold quality	5.212829496997609E-8	86.11	6.48e3

class BgeeAdapter(Adapter):
    INDEX = {'gene': 0, 'anatomical_entity': 2, 'expression': 8, 'fdr': 10, 'expression_score': 11}
    def __init__(self, filepath, write_properties, add_provenance):
        self.filepath = filepath
        self.label = 'expressed_in'

        self.source = 'bgee'
        self.source_url = 'https://www.bgee.org/download/gene-expression-calls?id=9606'
        super(BgeeAdapter, self).__init__(write_properties, add_provenance)
    
    def get_edges(self):
        edge_dict = defaultdict(lambda: {"score": float("-inf"), "props": {}})
        try:
            with gzip.open(self.filepath, 'rt') as f:
                next(f)  # skip header
                for line in f:
                    data = line.strip().split('\t')
                    if data[BgeeAdapter.INDEX['expression']] != 'present':
                        continue

                    source_id = data[BgeeAdapter.INDEX['gene']]
                    target_id = data[BgeeAdapter.INDEX['anatomical_entity']].split(' ∩ ')[0]
                    score = float(data[BgeeAdapter.INDEX['expression_score']])

                    # Add properties, including the score
                    props = {
                        "score": score,
                        "p_value": float(data[BgeeAdapter.INDEX['fdr']])
                    }

                    if self.add_provenance:
                        props.update({
                            "source": self.source,
                            "source_url": self.source_url,
                        })

                    # Update edge if new score is higher
                    edge_key = (source_id, target_id)
                    if score > edge_dict[edge_key]["score"]:
                        edge_dict[edge_key] = {"score": score, "props": props}

            # Yield deduplicated edges
            for (source_id, target_id), edge_data in edge_dict.items():
                yield source_id, target_id, self.label, edge_data["props"]

        except OSError as e:
            raise RuntimeError(f"Error opening the file: {e}")

