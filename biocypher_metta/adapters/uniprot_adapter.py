import gzip
import re
from Bio import SeqIO
from biocypher_metta.adapters import Adapter

class UniprotAdapter(Adapter):
    ALLOWED_TYPES = ['translates to', 'translation of']
    ALLOWED_LABELS = ['translates_to', 'translation_of']
    
    ISOFORM_PATTERN = re.compile(r'\[([\w\-]+)\]')

    def __init__(self, filepath, type, label, write_properties=True, add_provenance=True):
        if type not in self.ALLOWED_TYPES:
            raise ValueError('Invalid type. Allowed values: ' + 
                           ', '.join(self.ALLOWED_TYPES))
        if label not in self.ALLOWED_LABELS:
            raise ValueError('Invalid label. Allowed values: ' + 
                           ', '.join(self.ALLOWED_LABELS))
            
        self.filepath = filepath
        self.dataset = label
        self.type = type
        self.label = label
        self.source = "Uniprot"
        self.source_url = "https://www.uniprot.org/"
        super(UniprotAdapter, self).__init__(write_properties, add_provenance)

    def parse_ensembl_reference(self, line):
        try:
            # Extract isoform ID from square brackets if present
            isoform_match = self.ISOFORM_PATTERN.search(line)
            isoform_id = isoform_match.group(1) if isoform_match else None

            parts = line.split(';')
            if len(parts) >= 2:
                transcript = parts[1].strip().split('.')[0]  
                if transcript.startswith('ENST'):
                    return transcript, isoform_id
            return None, None
        except Exception as e:
            print(f"Error parsing line: {line}")
            print(f"Error details: {str(e)}")
            return None, None

    def get_edges(self):
        current_protein_id = None
        accession = None
        
        with gzip.open(self.filepath, 'rt') as input_file:
            for line in input_file:
                line = line.strip()
                
                if line.startswith('ID '):
                    current_protein_id = line.split()[1]
                    accession = None
                elif line.startswith('AC '):
                    accessions = line[5:].split()
                    if accessions and accessions[0]:
                        accession = accessions[0].rstrip(';')
                elif line.startswith('DR   Ensembl;') and accession:
                    if self.type == 'translates to':
                        try:
                            transcript_id, isoform_id = self.parse_ensembl_reference(line)
                            if transcript_id:
                                # Use isoform ID if available, otherwise use accession
                                target = isoform_id if isoform_id else accession
                                _props = {}
                                if self.write_properties and self.add_provenance:
                                    _props['source'] = self.source
                                    _props['source_url'] = self.source_url
                                yield transcript_id, target, self.label, _props
                        except Exception as e:
                            print(f'Failed to process edge translates to: {current_protein_id}')
                            print(f'Error: {str(e)}')
                            continue
                    elif self.type == 'translation of':
                        try:
                            transcript_id, isoform_id = self.parse_ensembl_reference(line)
                            if transcript_id:
                                source = isoform_id if isoform_id else accession
                                _props = {}
                                if self.write_properties and self.add_provenance:
                                    _props['source'] = self.source
                                    _props['source_url'] = self.source_url
                                yield source, transcript_id, self.label, _props
                        except Exception as e:
                            print(f'Failed to process edge translation of: {current_protein_id}')
                            print(f'Error: {str(e)}')
                            continue
