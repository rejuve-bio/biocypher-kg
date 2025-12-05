
from biocypher_metta.adapters import Adapter
from biocypher_metta.processors import EntrezEnsemblProcessor
import os

# https://coxpresdb.jp/download/Hsa-r.c6-0/coex/Hsa-r.v22-05.G16651-S235187.combat_pca.subagging.z.d.zip
# There is 16651 files. The file name is entrez gene id. The total genes annotated are 16651, one gene per file, each file contain logit score of other 16650 genes.
# There are two fields in each row: entrez gene id and logit score


class CoxpresdbAdapter(Adapter):

    def __init__(self, filepath, ensemble_to_entrez_path=None,
                 write_properties=None, add_provenance=None,
                 entrez_ensembl_processor=None):

        self.file_path = filepath
        self.dataset = 'coxpresdb'
        self.label = 'coexpressed_with'
        self.source = 'CoXPresdb'
        self.source_url = 'https://coxpresdb.jp/'
        self.version = 'v8'

        assert os.path.isdir(self.file_path), "coxpresdb file path is not a directory"

        # Use provided processor or create new one
        if entrez_ensembl_processor is None:
            self.processor = EntrezEnsemblProcessor()
            self.processor.load_or_update()
        else:
            self.processor = entrez_ensembl_processor

        super(CoxpresdbAdapter, self).__init__(write_properties, add_provenance)

    def get_edges(self):
        # Entrez-to-Ensembl mapping is now handled by EntrezEnsemblProcessor
        # which automatically updates from:
        # - NCBI Gene Info: https://ftp.ncbi.nih.gov/gene/DATA/GENE_INFO/Mammalia/Homo_sapiens.gene_info.gz
        # - GENCODE: https://ftp.ebi.ac.uk/pub/databases/gencode/Gencode_human/

        gene_ids = [f for f in os.listdir(self.file_path) if os.path.isfile(os.path.join(self.file_path, f))]

        # Use processor mapping
        entrez_ensembl_dict = self.processor.mapping
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
                            _props = {}
                            if self.write_properties:
                                _props['score'] = float(score)
                                if self.add_provenance:
                                    _props['source'] = self.source
                                    _props['source_url'] = self.source_url
                            yield source, target, self.label, _props
