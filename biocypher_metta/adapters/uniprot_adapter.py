import gzip
from Bio import SeqIO, SwissProt
from biocypher_metta.adapters import Adapter

# Data file is uniprot_sprot_human.dat.gz and uniprot_trembl_human.dat.gz at https://ftp.uniprot.org/pub/databases/uniprot/current_release/knowledgebase/taxonomic_divisions/.
# We can use SeqIO from Bio to read the file.
# Each record in file will have those attributes: https://biopython.org/docs/1.75/api/Bio.SeqRecord.html
# id, name will be loaded for protein. Ensembl IDs(example: Ensembl:ENST00000372839.7) in dbxrefs will be used to create protein and transcript relationship.


translation_condition_map = {
    7227: lambda item, entry_name: bool(item[0].startswith('EnsemblMetazoa') and 'FBtr' in item[1] and  entry_name.endswith("DROME")),
    9606: lambda item, entry_name: bool(item[0].startswith('Ensembl') and 'ENST' in item[1] and entry_name.endswith("HUMAN")),
}



class UniprotAdapter(Adapter):
    
    ALLOWED_TYPES = ['translates to', 'translation of']
    ALLOWED_LABELS = ['translates_to', 'translation_of']

    # added "taxon_id" to the 'protein' schema
    def __init__(self, filepath, type, label,
                 write_properties, add_provenance, taxon_id):
        if type not in UniprotAdapter.ALLOWED_TYPES:
            raise ValueError('Invalid type. Allowed values: ' +
                             ', '.join(UniprotAdapter.ALLOWED_TYPES))
        if label not in UniprotAdapter.ALLOWED_LABELS:
            raise ValueError('Invalid label. Allowed values: ' +
                             ', '.join(UniprotAdapter.ALLOWED_LABELS))
        self.filepath = filepath
        self.dataset = label
        self.type = type
        self.label = label
        self.source = "UniProt"
        self.source_url = "https://www.uniprot.org/"
        self.taxon_id = taxon_id

        super(UniprotAdapter, self).__init__(write_properties, add_provenance)


    def get_edges(self):
        translation_conditions_hold = translation_condition_map[self.taxon_id]

        with gzip.open(self.filepath, 'rt') as input_file:
            records = SwissProt.parse(input_file)
            for record in records:
                if self.type == 'translates to':
                    # dbxrefs = record.dbxrefs
                    dbxrefs = record.cross_references
                    for item in dbxrefs:
                        # if item.startswith('Ensembl') and 'ENST' in item:                    
                        if translation_conditions_hold(item, record.entry_name):
                            try:
                                ensg_id = "ENSEMBL:" + item[1].split(':')[-1].split('.')[0]  # Added ENSEMBL prefix
                                uniprot_id = "UniProtKB:" + record.accessions[0].upper()  #record.id.upper()  # Added UniProtKB prefix
                                _source = ensg_id
                                _target = uniprot_id
                                _props = {}
                                _props['taxon_id'] = f'NCBITaxon:{self.taxon_id}'
                                if self.write_properties and self.add_provenance:
                                    _props['source'] = self.source
                                    _props['source_url'] = self.source_url
                                yield _source, _target, self.label, _props

                            except :
                                print(
                                    f'fail to process for edge translates to: {record.entry_name} / {item[0]} / {item[1]}')
                                pass
