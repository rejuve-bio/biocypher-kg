'''

# https://wiki.flybase.org/wiki/FlyBase:Downloads_Overview#Genetic_interactions_.28allele_genetic_interactions_.2A.tsv.29

  # In this first version, the interaction description ia stored in the node that is identified by a string that works
  # like a "primary key": "allele_genetic_interaction_0", "allele_genetic_interaction_1", "allele_genetic_interaction_2",...
  #
  # TODO:
  # In the next iteration, this class will extract any allele symbol occuring in an "interaction" text and will br created
  # an edge from the "source allele" given by this node and the allele corresponding to  any allele symbol in the interaction text.
allele genetic interaction:
  description: >-
    An allelic interaction is a controlled vocabulary (i.e. not free text) genetic interaction data associated with alleles.
  #is_a: annotation
  is_a: biological entity
  #inherit_properties: true
  represented_as: node
  input_label: allele_genetic_interaction
  #source: allele
  #target: annotation                        # FB Controlled Vocabulary-based text
  properties:
    allele_id: str
    interaction_text: str                    # FB Controlled Vocabulary-based text
    source: str
    source_url: str


##allele_symbol	allele_FBal#	interaction	FBrf#
1038[1038]	FBal0189927	1038[1038] is an enhancer of haltere phenotype of vg[83b27]/vg[1]	FBrf0187637
1038[1038]	FBal0189927	1038[1038] is an enhancer of visible phenotype of vg[83b27]/vg[1]	FBrf0187637
1038[1038]	FBal0189927	1038[1038] is an enhancer of wing phenotype of vg[83b27]/vg[1]	FBrf0187637
14-3-3epsilon[18A2]	FBal0049166	14-3-3ε[18A2], Raf[12] has lethal | dominant phenotype	FBrf0086382
14-3-3epsilon[18A2]	FBal0049166	14-3-3ε[18A2], Raf[12] has lethal | dominant phenotype	FBrf0093395
14-3-3epsilon[Delta24]	FBal0122336	14-3-3ε[Δ24] is a suppressor of embryonic/first instar larval cuticle | maternal effect phenotype of tor[12D]	FBrf0158996
14-3-3epsilon[Delta24]	FBal0122336	14-3-3ε[Δ24], Raf[Su2] has lethal phenotype	FBrf0129944
14-3-3epsilon[EP3578]	FBal0157548	14-3-3ε[EP3578], Scer\GAL4[GMR.PF] is a suppressor of visible phenotype of Scer\GAL4[GMR.PF], foxo[UAS.Tag:FLAG]	FBrf0207166
14-3-3epsilon[GD4108]	FBal0198581	14-3-3ε[GD4108], Scer\GAL4[elav.PU] is an enhancer of abnormal locomotor behavior | adult stage | progressive phenotype of Hsap\HTT[128Q.1-336.UAS], Scer\GAL4[elav.PU]	FBrf0218881
14-3-3epsilon[GD4108]	FBal0198581	Scer\GAL80[ts.αTub84B], 14-3-3ε[GD4108], Scer\GAL4[da.PU] is a non-enhancer of lethal - all die during pupal stage | heat sensitive phenotype of Scer\GAL4[da.PU], Scer\GAL80[ts.αTub84B], fzr[RNAi.UAS.WIZ]	FBrf0237532
14-3-3epsilon[GD4108]	FBal0198581	Scer\GAL80[ts.αTub84B], 14-3-3ε[GD4108], Scer\GAL4[da.PU] is a non-suppressor of lethal - all die during pupal stage | heat sensitive phenotype of Scer\GAL4[da.PU], Scer\GAL80[ts.αTub84B], fzr[RNAi.UAS.WIZ]	FBrf0237532

'''

from biocypher_metta.adapters.dmel.flybase_tsv_reader import FlybasePrecomputedTable
from biocypher_metta.adapters import Adapter
from biocypher._logger import logger


class AlleleGeneticInteractionAdapter(Adapter):

    def __init__(self, write_properties, add_provenance, dmel_data_filepath):
        self.dmel_data_filepath = dmel_data_filepath
        self.label = 'allele_genetic_interaction'
        self.source = 'FLYBASE'
        self.source_url = 'https://flybase.org/'

        super(AlleleGeneticInteractionAdapter, self).__init__(write_properties, add_provenance)


    def get_nodes(self):
        fb_gg_table = FlybasePrecomputedTable(self.dmel_data_filepath)
        self.version = fb_gg_table.extract_date_string(self.dmel_data_filepath)
        #header:
        ##allele_symbol	allele_FBal#	interaction	FBrf#
        rows = fb_gg_table.get_rows()
        id = -1
        for row in rows:
            if "allele_FBal" in row[1]:     # to skip header (columns' names)
                continue
            id += 1
            props = {}
            props['allele_id'] = row[1]
            props['interaction_text'] = row[2]
            props['reference'] = row[3]
            props['taxon_id'] = 7227
            yield f'{self.label}_{id}', self.label, props
