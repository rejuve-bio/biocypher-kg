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
import re


class AlleleGeneticInteractionAdapter(Adapter):

    def __init__(self, write_properties, add_provenance, label, dmel_data_filepath, dmel_fbal_to_fbgn_file):
        self.dmel_data_filepath = dmel_data_filepath
        self.allele_symbol_to_fbal_dict = self.__build_allele_symbol_dict(dmel_fbal_to_fbgn_file)
        self.label = label              #  'allele_genetic_interaction'
        self.source = 'FLYBASE'
        self.source_url = 'https://flybase.org/'

        super(AlleleGeneticInteractionAdapter, self).__init__(write_properties, add_provenance)


    # def get_nodes(self):
    #     fb_AGI_table = FlybasePrecomputedTable(self.dmel_data_filepath)
    #     self.version = fb_AGI_table.extract_date_string(self.dmel_data_filepath)
    #     #header:
    #     ##allele_symbol	allele_FBal#	interaction	FBrf#
    #     rows = fb_AGI_table.get_rows()
    #     id = -1
    #     for row in rows:
    #         id += 1
    #         props = {}
    #         props['allele_id'] = row[1]
    #         props['interaction_text'] = row[2]
    #         props['reference'] = row[3]
    #         props['taxon_id'] = 7227
    #         yield f'{self.label}_{id}', self.label, props
    #     print('Finished allele interaction: \t\t\t\t\t\t\t\t\t\tget_nodes()')


    def get_edges(self):
        fb_AGI_table = FlybasePrecomputedTable(self.dmel_data_filepath)
        self.version = fb_AGI_table.extract_date_string(self.dmel_data_filepath)
        #header:
        ##allele_symbol	allele_FBal#	interaction	FBrf#        
        rows = fb_AGI_table.get_rows()
        id = -1
        for row in rows:
            id += 1
            props = {}            
            # props['allele_genetic_interaction_id'] = f'allele_genetic_interaction_{id}'   # useless for now...
            props['interaction_description'] = row[2]
            if self.add_provenance:
                props['source'] = self.source
                props['source_url'] = self.source_url
            props['taxon_id'] = 7227
            source_allele = row[1]
            interacting_alleles = self.__extract_allele_ids(row, source_allele)
            # print(f'allele: {row[1]}\nInteraction text:\n{row[2]}\nAlleles: {interacting_alleles}')            
            for allele in interacting_alleles:
              yield source_allele, allele, self.label, props
        print('Finished allele interaction: \t\t\t\t\t\t\t\t\t\tget_edges()')


    def __extract_allele_ids(self, row: list[str], source_allele: str) -> list[str]:
        """
        Extract allele IDs from the 'interaction' column of a row.
        source_allele is removed from the resulting list.
        Filters against known symbols in the allele dictionary.

        Args:
        - row (list[str]): A row of the input data table containing the 'interaction' column data.
        - source_allele: 'agent' (or one of them) of the interaction: it's not returned.

        Returns:
        - list: A list of valid allele IDs found in the 'interaction' text.
        """
        interaction_text = row[2]
        if not isinstance(interaction_text, str):
            print(f'Invalid ineraction text for row: {row}. Returning an empty list...')
            return []  # Return an empty list if the interaction text is not a valid string

        # Use regex to identify all potential allele symbols in the text
        # Symbols may include alphanumeric characters, dashes, colons, brackets, and dots
        potential_symbols = re.findall(r'[\w\-\+\[\]\:\\.]+', interaction_text, flags=re.UNICODE)
        # print(potential_symbols)
        # Filter the potential symbols to include those present in the allele dictionary
        # Return their corresponding AlleleID values
        valid_allele_ids = [self.allele_symbol_to_fbal_dict[symbol]['AlleleID'] for symbol in potential_symbols if symbol in self.allele_symbol_to_fbal_dict]
        if source_allele in valid_allele_ids:
            valid_allele_ids.remove(source_allele)
        return valid_allele_ids


    def __build_allele_symbol_dict(self, dmel_fbal_to_fbgn_file):
        allele_data_table = FlybasePrecomputedTable(dmel_fbal_to_fbgn_file)
        allele_df = allele_data_table.to_pandas_dataframe()
        
        # Create a dictionary mapping allele symbols to their IDs and GeneIDs
        # This will be used to validate and map extracted symbols
        allele_dict = {
            row['AlleleSymbol']: {'AlleleID': row['AlleleID'], 'GeneID': row['GeneID']}
            for _, row in allele_df.iterrows()
        }
        # print(allele_dict)
        return allele_dict
        