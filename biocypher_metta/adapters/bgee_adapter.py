import gzip
from biocypher_metta.adapters import Adapter
from biocypher_metta.adapters.helpers import to_float
from collections import defaultdict

# Example bgee tsv input files:
# Human:
# https://www.bgee.org/download/gene-expression-calls?id=9606
# Gene ID	"Gene name"	Anatomical entity ID	"Anatomical entity name"	Developmental stage ID	"Developmental stage name"	Sex	Strain	Expression	Call quality	FDR	Expression score	Expression rank
# ENSG00000000003	"TSPAN6"	CL:0000015	"male germ cell"	HsapDv:0000240	"sixth decade stage (human)"	male	White	present	gold quality	0.00221961024320294	89.08	5.09e3
# ENSG00000000003	"TSPAN6"	CL:0000019	"sperm"	UBERON:0018241	"prime adult stage"	male	wild-type	present	gold quality	0.00167287722066534	99.96	20.5
# ENSG00000000003	"TSPAN6"	CL:0000023	"oocyte"	UBERON:0000113	"post-juvenile"	female	wild-type	absent	gold quality	0.371619489632094	61.43	1.80e4
# ENSG00000000003	"TSPAN6"	CL:0000083	"epithelial cell of pancreas"	UBERON:0000104	"life cycle"	any	wild-type	present	gold quality	0.005262469333648452	83.30	7.79e3
# ENSG00000000003	"TSPAN6"	CL:0000089 ∩ UBERON:0000473	"male germ line stem cell (sensu Vertebrata) in testis"	UBERON:0000104	"life cycle"	male	wild-type	present	gold quality	5.212829496997609E-8	86.11	6.48e3

# soutce url: for fly: # https://www.bgee.org/download/gene-expression-calls?id=7227
# data file:
# https://www.bgee.org/ftp/current/download/calls/expr_calls/Drosophila_melanogaster_expr_simple_all_conditions.tsv.gz
# Gene ID	"Gene name"	Anatomical entity ID	"Anatomical entity name"	Developmental stage ID	"Developmental stage name"	Sex	Strain	Expression	Call quality	FDR	Expression score	Expression rank
# FBgn0000003	7SLRNA:CR32864	UBERON:0000473	testis	FBdv:00007079	day 4 of adulthood (fruit fly)	male	Oregon-R	present	gold quality	3.21563472059612E-9	86.28	2.28e3
# FBgn0000003	7SLRNA:CR32864	UBERON:0000473	testis	FBdv:00007079	day 4 of adulthood (fruit fly)	male	wild-type	present	gold quality	1.0E-14	93.63	1.06e3
# FBgn0000003	7SLRNA:CR32864	UBERON:0000473	testis	UBERON:0000066	fully formed stage	male	Oregon-R	present	gold quality	7.17484535789564E-10	88.06	1.99e3
# FBgn0000003	7SLRNA:CR32864	UBERON:0000922	embryo	FBdv:00004450	late extended germ band stage (fruit fly)	any	wild-type	present	gold quality	1.0E-14	99.98	4.50
# FBgn0000003	7SLRNA:CR32864	UBERON:6003007	insect adult head	FBdv:00007095	day 20 of adulthood (fruit fly)	male	wild-type	present	gold quality	1.0E-14	87.71	2.04e3
# FBgn0000008	a	CL:0000023	oocyte	UBERON:0000066	fully formed stage	female	yw	present	gold quality	0.000490828564953416	82.90	2.84e3
# FBgn0000008	a	CL:0000025	egg cell	FBdv:00005287	unfertilized egg stage (fruit fly)	any	Oregon-R	present	gold quality	0.000490828564953416	76.25	3.95e3



class BgeeAdapter(Adapter):
    FIELD_INDEX = {'gene': 0, 'anatomical_entity': 2, 'developmental stage': 4, 'expression': 8, 'fdr': 10, 'expression_score': 11}

    def __init__(self, filepath, write_properties, add_provenance, taxon_id, label = 'expressed_in'):
        self.filepath = filepath
        self.label = label
        self.taxon_id = taxon_id

        self.source = 'bgee'
        self.source_url = f'https://www.bgee.org/download/gene-expression-calls?id={self.taxon_id}'
        super(BgeeAdapter, self).__init__(write_properties, add_provenance)
    
    
    def get_edges(self):
        edge_dict = defaultdict(lambda: {"score": float("-inf"), "props": {}})
        try:
            with gzip.open(self.filepath, 'rt') as f:
                next(f)  # skip header
                for line in f:
                    if self.label == 'expressed_in':
                        # skip lines with no expression data
                        data = line.strip().split('\t')
                        if data[BgeeAdapter.FIELD_INDEX['expression']] != 'present':
                            continue
                        
                        #CURIE format for source ID (subject)
                        source_id =f"ENSEMBL:{data[BgeeAdapter.FIELD_INDEX['gene']]}"
                        # if ' ∩ ' in data[BgeeAdapter.FIELD_INDEX['anatomical_entity']]:
                        anatomical_entities = self.split_by_intersection(data[BgeeAdapter.FIELD_INDEX['anatomical_entity']])
                        
                        for anatomical_entity in anatomical_entities:                            
                            target_id = anatomical_entity
                            score = float(data[BgeeAdapter.FIELD_INDEX['expression_score']])
                        # target_id = data[BgeeAdapter.FIELD_INDEX['anatomical_entity']].split(' ∩ ')[0]
                        # score = float(data[BgeeAdapter.FIELD_INDEX['expression_score']])

                            # Add properties, including the score
                            props = {
                                "score": score,
                                "p_value": float(data[BgeeAdapter.FIELD_INDEX['fdr']]),
                                "developmental_stage": data[BgeeAdapter.FIELD_INDEX['developmental stage']],
                                "taxon_id": f'NCBITaxon:{self.taxon_id}',
                            }

                            if self.add_provenance:
                                props.update({
                                    "source": self.source,
                                    "source_url": self.source_url,                                
                                })
                        # elif self.label == 'expressed_in_developmental_stage':

                            # Update edge if new score is higher
                            edge_key = (source_id, target_id)
                            if score > edge_dict[edge_key]["score"]:
                                edge_dict[edge_key] = {"score": score, "props": props}

            # Yield deduplicated edges
            for (source_id, target_id), edge_data in edge_dict.items():
                yield source_id, target_id, self.label, edge_data["props"]

        except OSError as e:
            raise RuntimeError(f"Error opening the file: {e}")

    def split_by_intersection(self, s: str) -> list[str]:
        """
        Split a string by the Unicode intersection separator '∩' and return a list of IDs.
        Trims whitespace and drops empty parts.
        Example:
        "FBbt:00003725 ∩ UBERON:6000004 ∩ CL:0000540"
        -> ["FBbt:00003725", "UBERON:6000004", "CL:0000540"]
        """
        return [part.strip() for part in s.split('∩') if part.strip()]