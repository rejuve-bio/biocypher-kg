import gzip
import re
from Bio import SeqIO
from biocypher_metta.adapters import Adapter

# Data file is uniprot_sprot_human.dat.gz and uniprot_trembl_human.dat.gz at https://ftp.uniprot.org/pub/databases/uniprot/current_release/knowledgebase/taxonomic_divisions/.
# We can use SeqIO from Bio to read the file.
# Each record in file will have those attributes: https://biopython.org/docs/1.75/api/Bio.SeqRecord.html
# id, name will be loaded for protein. Ensembl IDs(example: Ensembl:ENST00000372839.7) in dbxrefs will be used to create protein and transcript relationship.


class UniprotAdapter(Adapter):
    ALLOWED_TYPES = ['translates to', 'translation of']
    ALLOWED_LABELS = ['translates_to', 'translation_of']
    
    ISOFORM_PATTERN = re.compile(r'\[([\w\-]+)\]')
    
    def __init__(self, filepath, type, label, write_properties=True, add_provenance=True):
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
        super(UniprotAdapter, self).__init__(write_properties, add_provenance)
    
    def parse_ensembl_reference(self, dbxref_item):
        try:
            isoform_match = self.ISOFORM_PATTERN.search(dbxref_item)
            isoform_id = isoform_match.group(1) if isoform_match else None
            
            transcript_id = dbxref_item.split(':')[-1].split('.')[0].split()[0]
            
            if transcript_id.startswith('ENST'):
                return transcript_id, isoform_id
            return None, None
        except Exception as e:
            print(f"Error parsing dbxref: {dbxref_item}")
            print(f"Error details: {str(e)}")
            return None, None
    
    def get_edges(self):
        with gzip.open(self.filepath, 'rt') as input_file:
            records = SeqIO.parse(input_file, 'swiss')
            for record in records:
                dbxrefs = record.dbxrefs
                
                for item in dbxrefs:
                    if item.startswith('Ensembl') and 'ENST' in item:
                        try:
                            transcript_id, isoform_id = self.parse_ensembl_reference(item)
                            
                            if not transcript_id:
                                continue
                            
                            protein_id = isoform_id if isoform_id else record.id.upper()
                            
                            ensg_id = "ENSEMBL:" + transcript_id
                            uniprot_id = "UniProtKB:" + protein_id
                            
                            _props = {}
                            if self.write_properties and self.add_provenance:
                                _props['source'] = self.source
                                _props['source_url'] = self.source_url
                            
                            if self.type == 'translates to':
                                _source = ensg_id
                                _target = uniprot_id
                                yield _source, _target, self.label, _props
                            elif self.type == 'translation of':
                                _source = uniprot_id
                                _target = ensg_id
                                yield _source, _target, self.label, _props
                                
                        except Exception as e:
                            print(f'Failed to process edge {self.type}: {record.id}')
                            print(f'Error: {str(e)}')
                            continue