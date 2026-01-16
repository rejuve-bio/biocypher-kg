import gzip
import pickle
import re
import json
import os
from biocypher_metta.adapters import Adapter
from Bio import SwissProt

# Data file is uniprot_sprot_human.dat.gz and uniprot_trembl_human.dat.gz at https://ftp.uniprot.org/pub/databases/uniprot/current_release/knowledgebase/taxonomic_divisions/.
# We can use SeqIO from Bio to read the file.
# Each record in file will have those attributes: https://biopython.org/docs/1.75/api/Bio.SeqRecord.html
# id, name will be loaded for protein. Ensembl IDs(example: ENST00000372839.7) in dbxrefs will be used to create protein and transcript relationship.

class UniprotProteinAdapter(Adapter):
    ALLOWED_SOURCES = ['UniProtKB/Swiss-Prot', 'UniProtKB/TrEMBL']

    def __init__(self, filepath, write_properties, add_provenance,taxon_id, label, dbxref=None, mapping_file=None):
        self.filepath = filepath
        self.dataset = 'UniProtKB_protein'
        self.label = label
        self.dbxref = dbxref
        self.go_subontology_mapping = pickle.load(open(mapping_file, 'rb')) if mapping_file else None 
        
        if self.dbxref == 'GO' and not self.go_subontology_mapping:
            raise ValueError("GO subontology mapping file must be provided for GO dbxref edges.")
        
        self.source = "UniProt"
        self.source_url = "https://www.uniprot.org/"
        self.taxon_id = taxon_id
        
        super(UniprotProteinAdapter, self).__init__(write_properties, add_provenance)
    
    def get_dbxrefs(self, cross_references):
        dbxrefs = []
        for cross_reference in cross_references:
            database_name = cross_reference[0].upper()
            if database_name == 'EMBL':
                for item in cross_reference[1:3]:
                    if item != '-':
                        id = database_name + ':' + item
                        dbxrefs.append(id)
            elif database_name in ['REFSEQ', 'ENSEMBL', 'MANE-SELECT']:
                for item in cross_reference[1:]:
                    if item != '-':
                        id = database_name + ':' + item.split('. ')[0]
                        dbxrefs.append(id)
            else:
                id = cross_reference[0].upper() + ':' + cross_reference[1]
                dbxrefs.append(id)
        
        return sorted(list(set(dbxrefs)), key=str.casefold)
    
    def parse_isoforms(self, comment):
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

    def _matches_ensembl_label(self, syn):
        """Return True only if syn matches the label (gene, transcript, protein)."""
        if "gene" in self.label and "ENSG" in syn:
            return True
        if "transcript" in self.label and "ENST" in syn:
            return True
        if "_protein" in self.label and "ENSP" in syn:
            return True
        return False


    def get_nodes(self):
        with gzip.open(self.filepath, 'rt') as input_file:
            records = SwissProt.parse(input_file)
            for record in records:
                if self.taxon_id == 7227 and not record.entry_name.endswith("DROME"):
                    continue
                dbxrefs = self.get_dbxrefs(record.cross_references)
                
                base_id = record.accessions[0].upper()
                props = {}

                if self.write_properties:
                    props = {
                        'protein_name': record.entry_name.split('_')[0],
                        'is_canonical': True
                    }

                    if len(record.accessions) > 1:
                        props['accessions'] = record.accessions[1:]


                    if self.add_provenance:
                        props['source'] = self.source
                        props['source_url'] = self.source_url

                yield base_id, self.label, props
                
                for comment in record.comments:
                    if 'ALTERNATIVE PRODUCTS:' in comment:
                        isoforms = self.parse_isoforms(comment)
                        
                        for isoform in isoforms:
                            isoform_id = isoform['id'].upper()
                            props = {}

                            if self.write_properties:
                                props = {
                                    'protein_name': record.entry_name.split('_')[0],
                                    'is_isoform': True,
                                    'canonical_accession': record.accessions[0],
                                    'isoform_name': isoform['name']
                                }


                                if self.add_provenance:
                                    props['source'] = self.source
                                    props['source_url'] = self.source_url

                            yield isoform_id, self.label, props
                        break

    def get_edges(self):
        with gzip.open(self.filepath, 'rt') as input_file:
            for record in SwissProt.parse(input_file):
                base_id = f"UniProtKB:{record.accessions[0].upper()}"

                if self.dbxref == "CHEBI":
                    for comment in record.comments:
                        if comment.startswith("CATALYTIC ACTIVITY:"):
                            chebi_ids = re.findall(r"ChEBI:CHEBI:(\d+)", comment)
                            evidence = re.findall(r"ECO:\d+", comment)
                            for cid in chebi_ids:
                                chebi_id = f"CHEBI:{cid}"
                                props = {}
                                if self.write_properties:
                                    props['evidence'] = evidence
                                    if self.add_provenance:
                                        props['source'] = self.source
                                        props['source_url'] = self.source_url
                                yield base_id, chebi_id, "protein_has_xref_catalytic_activity", props

                        elif comment.startswith("COFACTOR:"):
                            chebi_ids = re.findall(r"ChEBI:CHEBI:(\d+)", comment)
                            evidence = re.findall(r"ECO:\d+", comment)
                            for cid in chebi_ids:
                                chebi_id = f"CHEBI:{cid}"
                                props = {}
                                if self.write_properties:
                                    props['evidence'] = evidence
                                    if self.add_provenance:
                                        props['source'] = self.source
                                        props['source_url'] = self.source_url
                                yield base_id, chebi_id, "protein_has_xref_cofactor", props

                    for feature in record.features:
                        if feature.type == "BINDING":
                            ligand_id = feature.qualifiers.get('ligand_id')
                            evidence = re.findall(r"ECO:\d+", str(feature.qualifiers.get('evidence', '')))
                            
                            if ligand_id:
                                if isinstance(ligand_id, str):
                                    ligand_id = [ligand_id]
                                
                                for lid in ligand_id:
                                    cid_match = re.search(r"CHEBI:(\d+)", lid, re.IGNORECASE)
                                    if cid_match:
                                        lid = f"CHEBI:{cid_match.group(1)}"
                                    
                                    props = {}
                                    if self.write_properties:
                                        props['evidence'] = evidence
                                        if self.add_provenance:
                                            props['source'] = self.source
                                            props['source_url'] = self.source_url
                                    yield base_id, lid, "protein_has_xref_binding_site_ligand", props

                                    part_id = feature.qualifiers.get('ligand_part_id')
                                    if part_id:
                                        if isinstance(part_id, str):
                                            part_id = [part_id]
                                        for pid in part_id:
                                            pid_match = re.search(r"CHEBI:(\d+)", pid, re.IGNORECASE)
                                            if pid_match:
                                                pid = f"CHEBI:{pid_match.group(1)}"
                                                
                                            part_props = {}
                                            if self.write_properties:
                                                part_props['evidence'] = evidence
                                                if self.add_provenance:
                                                    part_props['source'] = self.source
                                                    part_props['source_url'] = self.source_url
                                            yield pid, lid, "chemical_substance_part_of_chemical_substance", part_props
                    continue

                dbxrefs = self.get_dbxrefs(record.cross_references)
                for syn in dbxrefs:
                    # Skip if not matching desired dbxref
                    if not syn.startswith(self.dbxref):
                        continue

                    # ENSEMBL-specific filtering
                    if self.dbxref == "ENSEMBL":
                        if not self._matches_ensembl_label(syn):
                            continue
                        syn = syn.split('.')[0]  # Remove version for ENSEMBL IDs
                    elif self.dbxref == "STRING":
                        syn = "STRING:" + syn.split('.')[1]
                    elif self.dbxref == "GO":
                        prefix, id_local = syn.split(':',1)
                        syn = id_local
                        
                        subontology = self.go_subontology_mapping.get(syn, None)   
                        if subontology not in self.label:
                            continue
                    props = {}
                    if self.write_properties:
                        props["dbxref"] = self.dbxref
                        if self.add_provenance:
                            props["source"] = self.source
                            props["source_url"] = self.source_url
                    yield base_id, syn, self.label, props