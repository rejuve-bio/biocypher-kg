import gzip
import json
import os
from biocypher_metta.adapters import Adapter
from Bio import SwissProt

class UniprotProteinAdapter(Adapter):
    ALLOWED_SOURCES = ['UniProtKB/Swiss-Prot', 'UniProtKB/TrEMBL']

    def __init__(self, filepath, write_properties, add_provenance):
        self.filepath = filepath
        self.dataset = 'UniProtKB_protein'
        self.label = 'protein'
        self.source = "Uniprot"
        self.source_url = "https://www.uniprot.org/"
        super(UniprotProteinAdapter, self).__init__(write_properties, add_provenance)

    def get_dbxrefs(self, cross_references):
        dbxrefs = []
        for cross_reference in cross_references:
            database_name = cross_reference[0]
            if database_name == 'EMBL':
                for item in cross_reference[1:3]:
                    if item != '-':
                        id = database_name + ':' + item
                        dbxrefs.append(id)
            elif database_name in ['RefSeq', 'Ensembl', 'MANE-Select']:
                for item in cross_reference[1:]:
                    if item != '-':
                        id = database_name + ':' + item.split('. ')[0]
                        dbxrefs.append(id)
            else:
                id = cross_reference[0] + ':' + cross_reference[1]
                dbxrefs.append(id)
        return sorted(list(set(dbxrefs)), key=str.casefold)

    def parse_isoforms(self, comment):
        """Parse isoform information from a comment string."""
        isoforms = []
        
        sections = [s.strip() for s in comment.split(';')]
        
        for section in sections:
            if section.startswith('Name='):
                current_name = section.split('=')[1].strip()
                
                for next_section in sections[sections.index(section):]:
                    if 'IsoId=' in next_section:
                        iso_ids = next_section.split('IsoId=')[1].split(',')
                        for iso_id in iso_ids:
                            clean_id = iso_id.split()[0].strip()
                            isoform = {
                                'name': current_name,
                                'id': clean_id
                            }
                            isoforms.append(isoform)
                        break
        
        return isoforms

    def get_nodes(self):
        with gzip.open(self.filepath, 'rt') as input_file:
            records = SwissProt.parse(input_file)
            for record in records:
                dbxrefs = self.get_dbxrefs(record.cross_references)
                base_id = record.accessions[0]
                
                base_props = {}
                if self.write_properties:
                    base_props = {
                        'protein_name': record.entry_name.split('_')[0],
                        'is_canonical': True
                    }
                    if len(record.accessions) > 1:
                        base_props['accessions'] = record.accessions[1:]
                    
                    if dbxrefs:
                        base_props['synonyms'] = dbxrefs
                    
                    if self.add_provenance:
                        base_props['source'] = self.source
                        base_props['source_url'] = self.source_url
                yield base_id, self.label, base_props
                
                for comment in record.comments:
                    if 'ALTERNATIVE PRODUCTS:' in comment:
                        isoforms = self.parse_isoforms(comment)
                        for isoform in isoforms:
                            isoform_props = {}
                            if self.write_properties:
                                isoform_props = {
                                    'protein_name': record.entry_name.split('_')[0],
                                    'is_isoform': True,
                                    'parent_protein_id': base_id,
                                    'isoform_name': isoform['name']
                                }
                                if self.add_provenance:
                                    isoform_props['source'] = self.source
                                    isoform_props['source_url'] = self.source_url
                            yield isoform['id'], self.label, isoform_props
                        break