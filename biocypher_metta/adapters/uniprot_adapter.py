import gzip
from Bio import SeqIO
from biocypher_metta.adapters import Adapter

# Data file is uniprot_sprot_human.dat.gz and uniprot_trembl_human.dat.gz at https://ftp.uniprot.org/pub/databases/uniprot/current_release/knowledgebase/taxonomic_divisions/.
# We can use SeqIO from Bio to read the file.
# Each record in file will have those attributes: https://biopython.org/docs/1.75/api/Bio.SeqRecord.html
# id, name will be loaded for protein. Ensembl IDs(example: Ensembl:ENST00000372839.7) in dbxrefs will be used to create protein and transcript relationship.

# saulo 
import re

condition_map = {
    7227: lambda item: re.match(r'^EnsemblMetazoa.*FBtr', item),    # fly data
    9606: lambda item: re.match(r'^Ensembl.*ENST', item),           # human data
}


class UniprotAdapter(Adapter):
    
    ALLOWED_TYPES = ['translates to', 'translation of']
    ALLOWED_LABELS = ['translates_to', 'translation_of']

    # added "taxon_id" to the 'protein' schema
    def __init__(self, filepath, type, label,
                 write_properties, add_provenance, taxon_id = 9606):
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
        self.source = "Uniprot"
        self.source_url = "https://www.uniprot.org/"
        self.taxon_id = taxon_id

        super(UniprotAdapter, self).__init__(write_properties, add_provenance)

    def get_edges(self):
        check_condition = condition_map[self.taxon_id]
        with gzip.open(self.filepath, 'rt') as input_file:
            records = SeqIO.parse(input_file, 'swiss')
            for record in records:
                if self.type == 'translates to':
                    dbxrefs = record.dbxrefs
                    for item in dbxrefs:                            
                        # if item.startswith('Ensembl') and 'ENST' in item:
                        if check_condition(item):
                            try:
                                ensg_id = item.split(':')[-1].split('.')[0]
                                _id = record.id + '_' + ensg_id
                                _source = ensg_id
                                _target = record.id
                                _props = {}
                                if self.write_properties and self.add_provenance:
                                    _props['source'] = self.source
                                    _props['source_url'] = self.source_url
                                _props['taxon_id'] = self.taxon_id
                                yield _source, _target, self.label, _props

                            except:
                                print(
                                    f'fail to process for edge translates to: {record.id}')
                                pass
                elif self.type == 'translation of':
                    dbxrefs = record.dbxrefs
                    for item in dbxrefs:
                        if item.startswith('Ensembl') and 'ENST' in item:
                            try:
                                ensg_id = item.split(':')[-1].split('.')[0]
                                _id = ensg_id + '_' + record.id
                                _target = ensg_id
                                _source = record.id
                                _props = {}
                                if self.write_properties and self.add_provenance:
                                    _props['source'] = self.source
                                    _props['source_url'] = self.source_url
                                yield  _source, _target, self.label, _props

                            except:
                                print(
                                    f'fail to process for edge translation of: {record.id}')
                                pass
