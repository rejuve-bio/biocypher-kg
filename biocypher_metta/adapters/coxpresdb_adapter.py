
from biocypher_metta.adapters import Adapter
import pickle
import os

# All organisms data can be acessed from:
# https://coxpresdb.jp/download/

# Human data:
# https://coxpresdb.jp/download/Hsa-r.c6-0/coex/Hsa-r.v22-05.G16651-S235187.combat_pca.subagging.z.d.zip
# There is 16651 files. The file name is entrez gene id. The total genes annotated are 16651, one gene per file, each file contain logit score of other 16650 genes.
# There are two fields in each row: entrez gene id and logit score
        # entrez_to_ensembl.pkl (for hsa) is generated using this file:
        # Homo_sapiens.gene_info.gz file: https://ftp.ncbi.nih.gov/gene/DATA/GENE_INFO/Mammalia/Homo_sapiens.gene_info.gz
        # every gene has ensembl id in gencode file, every gene has hgnc id if available.
        # every gene has entrez gene id in gene_info file, every gene has ensembl id or hgcn id if available

# Fly data:
# https://zenodo.org/record/6861444/files/Dme-u.v22-05.G12209-S15610.combat_pca.subagging.z.d.zip
# There are 12208 files. The file name is ENTREZ gene id. The total genes annotated are 12208, one gene per file, each file contain logit score of other 12208 genes.
# There are two fields in each row: entrez gene id and logit score

# entrez_to_ensembl.pkl (for dmel) is generated using this provisory script: scripts/dmel_create_entrez_to_ensembl_map.py file:
# Drosophila melanogaster gene info from NCBI: https://ftp.ncbi.nih.gov/gene/DATA/GENE_INFO/Invertebrates/Drosophila_melanogaster.gene_info.gz
# every gene has entrez gene id in gene_info file, every gene has ensembl ID in the dbXrefs column

# CEL data

# Mouse data:


# Rat data:



class CoxpresdbAdapter(Adapter):

    def __init__(self, filepath, entrez_to_ensemble_path, label,
                 write_properties, add_provenance, taxon_id):  

        self.file_path = filepath
        self.entrez_to_ensemble_dict_path = entrez_to_ensemble_path
        self.dataset = 'coxpresdb'
        self.label = label
        self.source = 'CoXPresdb'
        self.source_url = 'https://coxpresdb.jp/'
        self.version = 'v8'
        self.label = label 
        self.taxon_id = taxon_id
        assert os.path.isdir(self.file_path), "coxpresdb file path is not a directory"
        super(CoxpresdbAdapter, self).__init__(write_properties, add_provenance)

    def get_edges(self):
        # entrez_to_ensembl.pkl (for hsa) is generated using those two files:
        # gencode file: https://ftp.ebi.ac.uk/pub/databases/gencode/Gencode_human/release_43/gencode.v43.chr_patch_hapl_scaff.annotation.gtf.gz
        # Homo_sapiens.gene_info.gz file: https://ftp.ncbi.nih.gov/gene/DATA/GENE_INFO/Mammalia/Homo_sapiens.gene_info.gz
        # every gene has ensembl id in gencode file, every gene has hgnc id if available.
        # every gene has entrez gene id in gene_info file, every gene has ensembl id or hgcn id if available

        # gene_ids = [f for f in os.listdir(self.file_path) if os.path.isfile(os.path.join(self.file_path, f))]
        gene_ids = [f for f in os.listdir(self.file_path) if os.path.isfile(os.path.join(self.file_path, f)) and f.isdigit()]

        with open(self.entrez_to_ensemble_dict_path, 'rb') as f:
            entrez_ensembl_dict = pickle.load(f)
        for gene_id in gene_ids:
            gene_file_path = os.path.join(self.file_path, gene_id)
            entrez_id = gene_id
            ensembl_id = entrez_ensembl_dict.get(entrez_id)
            if ensembl_id:
                with open(gene_file_path, 'r') as input:
                    for line in input:
                        (co_entrez_id, score) = line.strip().split()
                        co_ensembl_id = entrez_ensembl_dict.get(co_entrez_id)
                        if co_ensembl_id:
                            _id = entrez_id + '_' + co_entrez_id + '_' + self.label
                            source = f"ENSEMBL:{ensembl_id}"
                            target = f"ENSEMBL:{co_ensembl_id}"
                            _props = {'taxon_id': f'{self.taxon_id}'}
                            if self.write_properties:
                                _props['score'] = float(score)
                                if self.add_provenance:
                                    _props['source'] = self.source
                                    _props['source_url'] = self.source_url
                            yield source, target, self.label, _props
