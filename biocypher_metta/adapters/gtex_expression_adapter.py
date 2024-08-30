import csv
import os
import pickle
from biocypher_metta.adapters import Adapter
from biocypher_metta.adapters.helpers import to_float, check_genomic_location
from biocypher._logger import logger
import gzip

# description for column headers can be found here: 
# https://storage.googleapis.com/adult-gtex/bulk-qtl/v8/single-tissue-cis-qtl/README_eQTL_v8.txt

#Example file
# rsid,gene_symbol,ensembl_gene_id,ensembl_versioned_gene_id,tss_distance,reference_allele,alternate_allele,num_alternate_alleles_per_site,ma_samples,ma_count,maf,slope,slope_se,pvalue_nominal,pvalue_nominal_threshold,min_pvalue_nominal,pvalue_beta,biosample,chromosome,position,variant_id_b37,variant_id_b38
# rs1000000,RP5-944M2.2,ENSG00000256927,ENSG00000256927.1,-19556,G,A,1,114,127,0.208197,-0.322815,0.0769928,3.8271e-05,4.79365e-05,4.31358e-07,0.00132434,Pancreas.v8.signif_variant_gene_pairs.txt,chr12,126406434,chr12_126890980_G_A_b37,chr12_126406434_G_A_b38
# rs10000003,HOPX,ENSG00000171476,ENSG00000171476.21,13582,A,G,1,182,215,0.288978,-0.172247,0.0417549,4.79443e-05,7.32855e-05,2.30038e-09,1.36078e-05,Heart_Atrial_Appendage.v8.signif_variant_gene_pairs.txt,chr4,56695481,chr4_57561647_A_G_b37,chr4_56695481_A_G_b38


class GTExExpressionAdapter(Adapter):

    index = {"rsid": 0, "gene_id": 2, "maf": 10, "slope": 11, 
                "p_value": 13, "tissue": 17, "chr": 18, "pos": 19}
    def __init__(self, filepath, gtex_tissue_ontology_map,
                 write_properties, add_provenance, label,
                 chr=None, start=None, end=None):
      
        self.filepath = filepath
        self.gtex_tissue_ontology_map = pickle.load(open(gtex_tissue_ontology_map, 'rb'))
        self.chr = chr
        self.start = start
        self.end = end
        self.label = label
        self.source = 'GTEx'
        self.source_url = 'https://forgedb.cancer.gov/api/gtex/v1.0/gtex.forgedb.csv.gz'
        self.version = 'v8'


        super(GTExExpressionAdapter, self).__init__(write_properties, add_provenance)

    def get_edges(self):
        with gzip.open(self.filepath, 'rt') as qtl:
            next(qtl) # skip header
            qtl_csv = csv.reader(qtl)
            for row in qtl_csv:
                try:
                    chr, pos = row[self.index["chr"]], row[self.index["pos"]]
                    pos = int(pos)
                    tissue = row[self.index["tissue"]].split(".")[0]
                    gene_id = row[self.index["gene_id"]]
                    ontology = self.gtex_tissue_ontology_map[tissue]
                    if check_genomic_location(self.chr, self.start, self.end, chr, pos, pos):
                        _source = gene_id
                        _target = ontology
                        _props = {
                            'p_value': to_float(row[self.index["p_value"]]),
                        }
                        if self.write_properties:
                            if self.add_provenance:
                                _props['source'] = self.source
                                _props['source_url'] = self.source_url

                        yield _source, _target, self.label, _props
                except Exception as e:
                    print(row)
                    print(e)
