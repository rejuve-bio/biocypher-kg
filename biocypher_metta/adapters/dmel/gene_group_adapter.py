'''
From 2024_06 Flybase version, there are 3 types of  groups: gene groups, metabolic pathway groups, and signaling pathway groups.

gene group:
  represented_as: node
  input_label: gene_group
  is_a: biological entity
  properties:
    genes: gene[]            # FBgn# list to link the group
    group_symbol: str
    group_name: str
    parent_groups: gene group[]        # FBgg#
    parent_groups_symbols: str[]
    HGNC_family_ID: str        # https://wiki.flybase.org/wiki/FlyBase:Downloads_Overview#Gene_groups_with_HGNC_IDs_.28gene_groups_HGNC_fb_.2A.tsv.29
    taxon_id: int               # 7227 for dmel / 9606 for hsa


# https://wiki.flybase.org/wiki/FlyBase:Downloads_Overview#Gene_groups_with_HGNC_IDs_.28gene_groups_HGNC_fb_.2A.tsv.29:
    FB_group_id FB_group_symbol                                    FB_group_name HGNC_family_ID
    FBgg0000506              KV         VOLTAGE-GATED POTASSIUM CHANNEL SUBUNITS            274
    FBgg0001672          GLUS-U                        UNCLASSIFIED GLUCOSIDASES
    FBgg0001190             LMN                                         LAMININS            626

# https://wiki.flybase.org/wiki/FlyBase:Downloads_Overview#Gene_group_data_.28gene_group_data_fb_.2A.tsv.29:
    FB_group_id FB_group_symbol                                      FB_group_name Parent_FB_group_id Parent_FB_group_symbol Group_member_FB_gene_id Group_member_FB_gene_symbol
    FBgg0000393      TRNA-C-ALA                    CYTOSOLIC ALANINE TRANSFER RNAS        FBgg0000367                 TRNA-C
    FBgg0000846          DNALIG                                        DNA LIGASES        FBgg0000814                  PELIG             FBgn0262619                     DNAlig1
    FBgg0000846          DNALIG                                        DNA LIGASES        FBgg0000814                  PELIG             FBgn0286075                     DNAlig3
    FBgg0000846          DNALIG                                        DNA LIGASES        FBgg0000814                  PELIG             FBgn0030506                     DNAlig4
    FBgg0000299           CHRAC                    CHROMATIN ACCESSIBILITY COMPLEX        FBgg0000314                   ISWI             FBgn0043001                    Chrac-16
    FBgg0000299           CHRAC                    CHROMATIN ACCESSIBILITY COMPLEX        FBgg0000314                   ISWI             FBgn0043002                    Chrac-14
    FBgg0000299           CHRAC                    CHROMATIN ACCESSIBILITY COMPLEX        FBgg0000314                   ISWI             FBgn0011604                        Iswi
    FBgg0000299           CHRAC                    CHROMATIN ACCESSIBILITY COMPLEX        FBgg0000314                   ISWI             FBgn0027620                         Acf

#(*) In fact, a group could have more than one parent! E.g: FBgg0000275

'''

from biocypher_metta.adapters.dmel.flybase_tsv_reader import FlybasePrecomputedTable
from biocypher_metta.adapters import Adapter
from typing import Dict, List


class GeneGroupAdapter(Adapter):

    def __init__(self, write_properties, add_provenance, label,  dmel_filepath=None, dmel_groups_hgnc_filepath=None):

        self.dmel_filepath = dmel_filepath
        self.dmel_groups_hgnc_filepath = dmel_groups_hgnc_filepath
        self.label = label
        self.type = 'GeneGroup'
        self.source = 'FLYBASE'
        self.source_url = 'https://flybase.org/'

        super(GeneGroupAdapter, self).__init__(write_properties, add_provenance)


    def get_nodes(self):
        fb_gg_table = FlybasePrecomputedTable(self.dmel_filepath)
        self.version = fb_gg_table.extract_date_string(self.dmel_filepath)
        hgnc_dict = self.__build_hgnc_dict()
        #header:
        #FB_group_id    FB_group_symbol    FB_group_name    Parent_FB_group_id  Parent_FB_group_symbol  Group_member_FB_gene_id     Group_member_FB_gene_symbol
        rows = fb_gg_table.get_rows()
        parents = []
        genes_ids = []
        for i in range(0, len(rows)):
            group_id = rows[i][0]
            group_symbol = rows[i][1]
            group_name = rows[i][2]
            if rows[i][3] != '' and rows[i][3] not in parents:       # holds only group id
                parents.append(rows[i][3].lower())      # lower() should not be used...
            if rows[i][5] != '':
                genes_ids.append(rows[i][5].lower())      # lower() should not be used...
            if (i < len(rows) - 1) and (group_id == rows[i + 1][0]):
                continue
            else:
                props = {}
                if genes_ids != []:
                    props['genes'] = genes_ids
                props['group_symbol'] = group_symbol
                props['group_name'] = group_name
                if parents != []:
                    props['parent_groups'] = list(set(parents))
                try:
                    hgnc_id = hgnc_dict[group_id]
                except KeyError as k:
                    hgnc_id= None
                if hgnc_id != None:
                    props['HGNC_family_ID'] = hgnc_id
                props['taxon_id'] = 7227
                if self.add_provenance:
                    props['source'] = self.source
                    props['source_url'] = self.source_url
                parents = []
                genes_ids = []
                #print(f'group: {group_id}\n{props}')
                yield group_id, self.label, props


    def get_edges(self):
        fb_gg_table = FlybasePrecomputedTable(self.dmel_filepath)
        self.version = fb_gg_table.extract_date_string(self.dmel_filepath)
        #header:
        #FB_group_id    FB_group_symbol    FB_group_name    Parent_FB_group_id  Parent_FB_group_symbol  Group_member_FB_gene_id     Group_member_FB_gene_symbol
        rows = fb_gg_table.get_rows()
        for i in range(0, len(rows)):
            props = {}
            props['taxon_id'] = 7227
            if self.add_provenance:
                props['source'] = self.source
                props['source_url'] = self.source_url            
            group_id = rows[i][0]   # target (group)
            if rows[i][5] != '':    # source (gene)
                yield rows[i][5], group_id, self.label, props
                

    def __build_hgnc_dict(self) -> Dict[str, List[str]]:
        hgnc_dict: Dict[str, List[str]] = {}
        fb_hgnc_table = FlybasePrecomputedTable(self.dmel_groups_hgnc_filepath)
        # header:
        ## FB_group_id	FB_group_symbol	FB_group_name	HGNC_family_ID
        for row in fb_hgnc_table.get_rows():
            key = row[0]
            value = row[-1]
            if key not in hgnc_dict:
                hgnc_dict[key] = []
            hgnc_dict[key].append(value)
        return hgnc_dict

'''
gga = GeneGroupAdapter(False,True,
'/home/saulo/snet/hyperon/github/das-pk/shared_hsa_dmel2metta/data/toy/flybase/gene_group_data_fb_2024_02.tsv.gz',
'/home/saulo/snet/hyperon/github/das-pk/shared_hsa_dmel2metta/data/toy/flybase/gene_groups_HGNC_fb_2024_02.tsv.gz')

gga.get_nodes()
'''