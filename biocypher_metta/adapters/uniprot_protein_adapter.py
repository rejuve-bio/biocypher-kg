from collections import defaultdict
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
        taxon_to_suffixes = defaultdict(lambda: None)
        taxon_to_suffixes[7227] ='DROME',
        taxon_to_suffixes[9606] = 'HUMAN',
        
        with gzip.open(self.filepath, 'rt') as input_file:
            records = SwissProt.parse(input_file)
            for record in records:
                if taxon_to_suffixes[self.taxon_id] == None or not record.entry_name.endswith(taxon_to_suffixes[self.taxon_id]):
                    continue
                # dbxrefs = self.get_dbxrefs(record.cross_references)
                
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
        taxon_to_suffixes = defaultdict(lambda: None)
        taxon_to_suffixes[7227] ='DROME',
        taxon_to_suffixes[9606] = 'HUMAN',
                
        with gzip.open(self.filepath, 'rt') as input_file:
            for record in SwissProt.parse(input_file):
                if taxon_to_suffixes[self.taxon_id] == None or not record.entry_name.endswith(taxon_to_suffixes[self.taxon_id]):
                    continue                
                dbxrefs = self.get_dbxrefs(record.cross_references)
                base_id = f"UniProtKB:{record.accessions[0].upper()}"

                if self.dbxref == "CHEBI":
                    if self.label == "protein_has_xref_catalytic_activity":
                        for comment in record.comments:
                            if "CATALYTIC ACTIVITY" in comment:
                                chebi_ids = re.findall(r"CHEBI:(\d+)", comment, re.IGNORECASE)
                                evidence = re.findall(r"ECO:(\d+)", comment, re.IGNORECASE)
                                evidence_codes = [f"ECO_{eco}" for eco in evidence]
                                for cid in chebi_ids:
                                    chebi_id = f"CHEBI:{cid}"
                                    props = {}
                                    if self.write_properties:
                                        if evidence_codes:
                                            props['evidence'] = evidence_codes
                                        if self.add_provenance:
                                            props['source'] = self.source
                                            props['source_url'] = self.source_url
                                    yield base_id, chebi_id, self.label, props

                    elif self.label == "protein_has_xref_cofactor":
                        for comment in record.comments:
                            if "COFACTOR" in comment:
                                chebi_ids = re.findall(r"CHEBI:(\d+)", comment, re.IGNORECASE)
                                evidence = re.findall(r"ECO:(\d+)", comment, re.IGNORECASE)
                                evidence_codes = [f"ECO_{eco}" for eco in evidence]
                                for cid in chebi_ids:
                                    chebi_id = f"CHEBI:{cid}"
                                    props = {}
                                    if self.write_properties:
                                        if evidence_codes:
                                            props['evidence'] = evidence_codes
                                        if self.add_provenance:
                                            props['source'] = self.source
                                            props['source_url'] = self.source_url
                                    yield base_id, chebi_id, self.label, props

                    elif self.label in ["protein_has_xref_binding_site_ligand", "chemical_substance_part_of_chemical_substance"]:
                        for feature in record.features:
                            if feature.type == "BINDING":
                                ligand_id = feature.qualifiers.get('ligand_id')
                                if ligand_id:
                                    if self.label == "protein_has_xref_binding_site_ligand":
                                        if isinstance(ligand_id, str):
                                            ligand_id = [ligand_id]
                                        
                                        for lid in ligand_id:
                                            cid_match = re.search(r"CHEBI:(\d+)", lid, re.IGNORECASE)
                                            if cid_match:
                                                cid = f"CHEBI:{cid_match.group(1)}"
                                                
                                                evidence = feature.qualifiers.get('evidence', [])
                                                if isinstance(evidence, str):
                                                    evidence = [evidence]
                                                evidence_codes = []
                                                for ev in evidence:
                                                    ecos = re.findall(r"ECO:(\d+)", ev)
                                                    for eco in ecos:
                                                        evidence_codes.append(f"ECO_{eco}")
                                                
                                                props = {}
                                                if self.write_properties:
                                                    if evidence_codes:
                                                        props['evidence'] = evidence_codes
                                                    if self.add_provenance:
                                                        props['source'] = self.source
                                                        props['source_url'] = self.source_url
                                                yield base_id, cid, self.label, props

                                    if self.label == "chemical_substance_part_of_chemical_substance":
                                        part_id = feature.qualifiers.get('ligand_part_id')
                                        if part_id:
                                            if isinstance(ligand_id, str):
                                                ligand_id = [ligand_id]
                                            if isinstance(part_id, str):
                                                part_id = [part_id]
                                            
                                            evidence = feature.qualifiers.get('evidence', [])
                                            if isinstance(evidence, str):
                                                evidence = [evidence]
                                            evidence_codes = []
                                            for ev in evidence:
                                                ecos = re.findall(r"ECO:(\d+)", ev)
                                                for eco in ecos:
                                                    evidence_codes.append(f"ECO_{eco}")
            
                                            for lid in ligand_id:
                                                l_match = re.search(r"CHEBI:(\d+)", lid, re.IGNORECASE)
                                                if l_match:
                                                    l_chebi = f"CHEBI:{l_match.group(1)}"
                                                    for pid in part_id:
                                                        p_match = re.search(r"CHEBI:(\d+)", pid, re.IGNORECASE)
                                                        if p_match:
                                                            p_chebi = f"CHEBI:{p_match.group(1)}"
                                                            
                                                            part_props = {}
                                                            if self.write_properties:
                                                                if evidence_codes:
                                                                    part_props['evidence'] = evidence_codes
                                                                if self.add_provenance:
                                                                    part_props['source'] = self.source
                                                                    part_props['source_url'] = self.source_url
                                                            yield p_chebi, l_chebi, self.label, part_props
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