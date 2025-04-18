'''

# https://wiki.flybase.org/wiki/FlyBase:Downloads_Overview#Genetic_interaction_table_.28gene_genetic_interactions_.2A.tsv.29
gene genetic association:
  description: >-
    An association between a gene and another gene, i.e., a gene-level genetic interactions in FlyBase. This data is computed from the allele-level genetic interaction data captured by FlyBase curators.
  is_a: regulatory association
  inherit_properties: true
  represented_as: edge
  input_label: gene_genetic
  source: gene
  target: gene
  properties:
    type: str                   # suppresses (“suppressible”) / enhances (“enhanceable”)
    reference: str              # FBrf#
    taxon_id: int                        # 7227 for dmel / 9606 for hsa

##Starting_gene(s)_symbol	Starting_gene(s)_FBgn	Interacting_gene(s)_symbol	Interacting_gene(s)_FBgn	Interaction_type	Publication_FBrf
Pfdn2	FBgn0010741	aPKC	FBgn0261854	suppressible	FBrf0231608
Col4a1	FBgn0000299	vkg	FBgn0016075	suppressible	FBrf0217234
dx	FBgn0000524	Su(dx)	FBgn0003557	suppressible	FBrf0068437
dsh	FBgn0000499	msn	FBgn0010909	suppressible	FBrf0111453
Rac1	FBgn0010333	upd1	FBgn0004956	suppressible	FBrf0191759
aos	FBgn0004569	pix	FBgn0086706	enhanceable	FBrf0190712
z	FBgn0004050	Scm	FBgn0003334	suppressible	FBrf0104734

'''

from biocypher_metta.adapters.dmel.flybase_tsv_reader import FlybasePrecomputedTable
from biocypher_metta.adapters import Adapter
from biocypher._logger import logger


class GeneGeneticAssociationAdapter(Adapter):

    def __init__(self, write_properties, add_provenance, dmel_data_filepath):
        self.dmel_data_filepath = dmel_data_filepath
        self.label = 'gene_genetic_association'
        self.source = 'FLYBASE'
        self.source_url = 'https://flybase.org/'

        super(GeneGeneticAssociationAdapter, self).__init__(write_properties, add_provenance)


    def get_edges(self):
        gene_genetic_table= FlybasePrecomputedTable(self.dmel_data_filepath)
        self.version = gene_genetic_table.extract_date_string(self.dmel_data_filepath)
        # header:
        #Starting_gene(s)_symbol	Starting_gene(s)_FBgn	Interacting_gene(s)_symbol	Interacting_gene(s)_FBgn	Interaction_type	Publication_FBrf
        rows = gene_genetic_table.get_rows()
        for row in rows:
            if "Starting_gene(s)_FBgn" in row[1]:     # to skip header (columns' names)
                continue
            props = {}
            #source_ids = row[1].split('|')
            source = [id for id in row[1].split('|')]
            target = [id for id in row[3].split('|')]
            # most frequent case:
            if len(source) == 1 and len(target) == 1:
                source = source[0]
                target = target[0]
                props['type'] = row[4]
                props['reference'] = row[5]
                props['taxon_id'] = 7227
                yield source, target, self.label, props
            #
            elif len(source) > 1 and len(target) == 1:
                target = target[0]
                for source_id in source:
                    props['type'] = row[4]
                    props['reference'] = row[5]
                    props['taxon_id'] = 7227
                    yield source_id, target, self.label, props
            #
            elif len(source) == 1 and len(target) > 1:
                source = source[0]
                for target_id in target:
                    props['type'] = row[4]
                    props['reference'] = row[5]
                    props['taxon_id'] = 7227
                    yield source, target_id, self.label, props
            #
            elif len(source) > 1 and len(target) > 1:
                for source_id in source:
                    for target_id in target:
                        props['type'] = row[4]
                        props['reference'] = row[5]
                        props['taxon_id'] = 7227
                        yield source_id, target_id, self.label, props