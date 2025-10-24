'''
1. Protein-protein: use the "representative protein" from:
    https://wiki.flybase.org/wiki/FlyBase:Downloads_Overview#Unique_protein_isoforms_.28dmel_unique_protein_isoforms_fb_.2A.tsv.gz.29
    --> get its symbol nad get its FBpp from: 
    https://wiki.flybase.org/wiki/FlyBase:Downloads_Overview#FBgn_.3C.3D.3E_FBtr_.3C.3D.3E_FBpp_IDs_.28fbgn_fbtr_fbpp_.2A.tsv.29

2. Protein-RNA: same as above, but get FBtr from the same record as the representative protein ("representative transcript").
3. RNA-RNA: same as above

To speedup the processing build a dictionary mapping FBgn --> {('protein', FBpp), ('transcript', FBtr)}

ID(s) Interactor A	    0
ID(s) Interactor B	    1
Alt ID(s) Interactor A	Alt ID(s) Interactor B	Alias(es) Interactor A	Alias(es) Interactor B	
Interaction Detection Method(s)     6
Publication 1st Author(s)	
Publication ID(s)	8
Taxid Interactor A	9      # these could be of another organism (not  D. melanogaster)
Taxid Interactor B	10
Interaction Type(s) 11
Source Database(s)	Interaction Identifier(s)	Confidence Value(s)	Expansion Method(s)	Biological Role(s) Interactor A	Biological Role(s) Interactor B	Experimental Role(s) Interactor A	Experimental Role(s) Interactor B	

Type(s) Interactor A	20  # protein, ribonucleic acid
Type(s) Interactor B	21
Xref(s) Interactor A	Xref(s) Interactor B	Interaction Xref(s)	
Annotation(s) Interactor A	25
Annotation(s) Interactor B	26
Interaction Annotation(s)   27	
Host Organism(s)	Interaction Parameters	Creation Date	Update Date	Checksum Interactor A	Checksum Interactor B	Interaction Checksum	Negative	Feature(s) Interactor A	Feature(s) Interactor B	Stoichiometry Interactor A	Stoichiometry Interactor B	Identification Method(s) Participant A	Identification Method(s) Participant B			,0\\

Schemas

# Schema for RNA-protein and protein-protein interactions (there are 3349 RNA-prot and RNA-RNA interactions in the FB_2024_05 FB release) and protein-protein a
# Biocypher doesn't accept source AND targets as lists of types in the same schema
# # ptp stands for "protein or transcript to protein"
ptp physically interacts with:
  is_a: expression
  inherit_properties: true
  represented_as: edge
  input_label: ptp_physically_interacts_with
  source: [protein, transcript]
  target: protein
  properties:
    source_gene: gene       
    target_gene: gene       
    detection_method: mi    # Molecular Interaction Ontology
    pubmed_ids: str[]
    source_taxon_id: int
    target_taxon_id: int
    interaction_type: mi    # Molecular Interaction Ontology
    interaction_comments: str
    source_comments: str
    target_comments: str    


# Schema for protein-RNA and RNA-RNA interactions  (there are 3349 such interactions in the FB_2024_05 FB release)   physical interaction:
# ptt stands for "protein or transcript to transcript"
ptt physically interacts with:
  is_a: expression
  inherit_properties: true
  represented_as: edge
  input_label: ptt_physically_interacts_with
  source: [protein, transcript]
  target: transcript   
  properties:
    source_gene: gene       
    target_gene: gene       
    detection_method: mi    # Molecular Interaction Ontology
    pubmed_ids: str[]
    source_taxon_id: int
    target_taxon_id: int
    interaction_type: mi    # Molecular Interaction Ontology
    interaction_comments: str
    source_comments: str
    target_comments: str    

'''


from biocypher_metta.adapters.dmel.flybase_tsv_reader import FlybasePrecomputedTable
from biocypher_metta.adapters import Adapter
import re
import pandas
import pickle

class PhysicalInteractionAdapter(Adapter):

    def __init__(self, write_properties, add_provenance, label, filepaths: list[str] = None):
        self.filepaths = filepaths
        self.label = label
        self.source = 'FLYBASE'
        self.source_url = 'https://flybase.org/'

        self.fbgn_to_uniprot_dict = self.__build_fbgn_to_uniprot_dict()        
        self.fbgn_to_fbtr_uniprot_dict = self.__build_transcript_protein_dict()

        super(PhysicalInteractionAdapter, self).__init__(write_properties, add_provenance)


    # def get_nodes(self):
    #     """
    #     This is necessary to create nodes that were not created before the creation of edges among them.        
    #     """
    #     fb_psimi_table = FlybasePrecomputedTable(self.filepaths[0])
    #     self.version = fb_psimi_table.extract_date_string(self.filepaths[0])
    #     fb_psimi_table = self.pre_process(fb_psimi_table)
        
    #     #header:
    #     #ID(s) Interactor A	ID(s) Interactor B	Alt ID(s) Interactor A	Alt ID(s) Interactor B	Alias(es) Interactor A	Alias(es) Interactor B	Interaction Detection Method(s)	Publication 1st Author(s)	Publication ID(s)	Taxid Interactor A	Taxid Interactor B	Interaction Type(s)	Source Database(s)	Interaction Identifier(s)	Confidence Value(s)	Expansion Method(s)	Biological Role(s) Interactor A	Biological Role(s) Interactor B	Experimental Role(s) Interactor A	Experimental Role(s) Interactor B	Type(s) Interactor A	Type(s) Interactor B	Xref(s) Interactor A	Xref(s) Interactor B	Interaction Xref(s)	Annotation(s) Interactor A	Annotation(s) Interactor B	Interaction Annotation(s)	Host Organism(s)	Interaction Parameters	Creation Date	Update Date	Checksum Interactor A	Checksum Interactor B	Interaction Checksum	Negative	Feature(s) Interactor A	Feature(s) Interactor B	Stoichiometry Interactor A	Stoichiometry Interactor B	Identification Method(s) Participant A	Identification Method(s) Participant B			,0\\
    #     rows = fb_psimi_table.get_rows()

    #     for row in rows:


    def get_edges(self):
        fb_psimi_table = FlybasePrecomputedTable(self.filepaths[0])
        self.version = fb_psimi_table.extract_date_string(self.filepaths[0])
        fb_psimi_table = self.pre_process(fb_psimi_table)
        
        #header:
        #ID(s) Interactor A	ID(s) Interactor B	Alt ID(s) Interactor A	Alt ID(s) Interactor B	Alias(es) Interactor A	Alias(es) Interactor B	Interaction Detection Method(s)	Publication 1st Author(s)	Publication ID(s)	Taxid Interactor A	Taxid Interactor B	Interaction Type(s)	Source Database(s)	Interaction Identifier(s)	Confidence Value(s)	Expansion Method(s)	Biological Role(s) Interactor A	Biological Role(s) Interactor B	Experimental Role(s) Interactor A	Experimental Role(s) Interactor B	Type(s) Interactor A	Type(s) Interactor B	Xref(s) Interactor A	Xref(s) Interactor B	Interaction Xref(s)	Annotation(s) Interactor A	Annotation(s) Interactor B	Interaction Annotation(s)	Host Organism(s)	Interaction Parameters	Creation Date	Update Date	Checksum Interactor A	Checksum Interactor B	Interaction Checksum	Negative	Feature(s) Interactor A	Feature(s) Interactor B	Stoichiometry Interactor A	Stoichiometry Interactor B	Identification Method(s) Participant A	Identification Method(s) Participant B			,0\\
        rows = fb_psimi_table.get_rows()

        # for k in self.fbgn_to_uniprot_dict:
        #     print(f'{k}:\t {self.fbgn_to_uniprot_dict[k]}')
        # for k in self.fbgn_to_fbtr_uniprot_dict:
        #     print(f'{k}:\t {self.fbgn_to_fbtr_uniprot_dict[k]}')
        for row in rows:
            source_fbgn = row[0]
            target_fbgn = row[1]
            if source_fbgn not in self.fbgn_to_fbtr_uniprot_dict: # @todo get  from Flybase
                print(f'(Source) gene {source_fbgn} has no transcript/protein in Flybase. Skipping it in this Fly BioAS version...')
                # exit(9)
                continue
            if target_fbgn not in self.fbgn_to_fbtr_uniprot_dict: # @todo get  from Flybase
                print(f'(Target) gene {target_fbgn} has no transcript/protein in Flybase. Skipping it in this Fly BioAS version...')
                # exit(9)
                continue

            sources, targets = self.get_FB_ids(source_fbgn, target_fbgn, row)            
            props = self.get_properties(row)            
            # ptp
            # source: [protein, transcript]
            # target: protein
            if targets[0] == 'transcript' and self.label.startswith('ptp'):
                continue
            # ptt
            # source: [protein, transcript]
            # target: transcript            
            if targets[0] == 'protein' and self.label.startswith('ptt'):
                continue
            if sources[0] == 'transcript':
                source_ids = sources[1][0]
            else:
                source_ids = sources[1][1]
            if targets[0] == 'transcript':
                target_ids = targets[1][0]
            else:
                target_ids = targets[1][1]
            if source_ids is None:
                if sources[1][0] is None:
                    print(f'Source transcripts for {source_fbgn} not available yet...')
                if sources[1][1] is None:
                    print(f'Source uniprot ids for {source_fbgn} not available yet...')
                continue
            if target_ids is None:
                if targets[1][0] is None:
                    print(f'Target transcripts for {target_fbgn} not available yet...')
                if targets[1][1] is None:
                    print(f'Target uniprot ids for {target_fbgn} not available yet...')                
                continue

            # Flybase doesn't informs which transcript or protein is interacting. So, use all...
            for source in source_ids:
                for target in target_ids:                                        
                    yield (sources[0], source), target, self.label, props                   
                        

    def get_FB_ids(self, source_fbgn, target_fbgn, data_row):
        source_type = 'protein' if data_row[20] == 'MI_0326' else 'transcript'  # 'MI:0326' means 'protein'
        target_type = 'protein' if data_row[21] == 'MI_0326' else 'transcript'

        fb_source_list = self.fbgn_to_fbtr_uniprot_dict[source_fbgn]
        fb_target_list = self.fbgn_to_fbtr_uniprot_dict[target_fbgn]
        return (source_type, fb_source_list), (target_type, fb_target_list)        


    def get_gene_products_ids(self, fbgn: str, gene_product_type: str) -> list:
        if fbgn in self.fbgn_to_fbtr_uniprot_dict:
            filtered_values = [
                item[gene_product_type] 
                for item in self.fbgn_to_fbtr_uniprot_dict[fbgn] if gene_product_type in item
            ]
        else:
            filtered_values = []
        return filtered_values


    def get_properties(self, row):
        properties = {}        
        properties['source_gene'] = row[0].lower()
        properties['target_gene'] = row[1].lower()
        properties['detection_method'] = row[6]
        properties['pubmed_ids'] = row[8]
        properties['source_taxon_id'] = row[9]
        properties['target_taxon_id'] = row[10]
        properties['interaction_type'] = row[11].replace(':', '_')
        properties['interaction_comments'] = row[27]
        properties['source_comments'] = row[25]
        properties['target_comments'] = row[26]
        return properties


    def pre_process(self, fb_psimi_table: FlybasePrecomputedTable) -> FlybasePrecomputedTable:
        df = fb_psimi_table.to_pandas_dataframe()        
        # replace cells equal to "-" by ""
        #df = df.map(lambda x: '' if str(x) == '-' else x)
        df = df.applymap(lambda x: '' if str(x) == '-' else x)

        df["ID(s) Interactor A"] = df["ID(s) Interactor A"].apply(lambda x: self._remove_flybase_str(x) if x else x)
        df["ID(s) Interactor B"] = df["ID(s) Interactor B"].apply(lambda x: self._remove_flybase_str(x) if x else x)
        # df["Alt ID(s) Interactor A"] = df["Alt ID(s) Interactor A"].apply(lambda x: self._remove_flybase_str(x) if x else x)
        # df["Alt ID(s) Interactor B"] = df["Alt ID(s) Interactor B"].apply(lambda x: self._remove_flybase_str(x) if x else x)
        df["Publication ID(s)"] = df["Publication ID(s)"].apply(lambda x: self._get_pubmed_id(x) if x else x)
        # df["Interaction Xref(s)"] = df["Interaction Xref(s)"].apply(lambda x: self._extract_ids(x) if x else x)
        df["Annotation(s) Interactor A"] = df["Annotation(s) Interactor A"].apply(lambda x: self._strip_comments(x) if x else x)
        df["Annotation(s) Interactor B"] = df["Annotation(s) Interactor B"].apply(lambda x: self._strip_comments(x) if x else x)
        df["Interaction Annotation(s)"] = df["Interaction Annotation(s)"].apply(lambda x: self._strip_comments(x) if x else x)

        df["Taxid Interactor A"] = df["Taxid Interactor A"].apply(lambda x: self._get_taxon_id(x) if x else x)
        df["Taxid Interactor B"] = df["Taxid Interactor B"].apply(lambda x: self._get_taxon_id(x) if x else x)

        # These columns could be linked to MI Ontology
        mi_columns = [
            "Interaction Detection Method(s)",
            "Interaction Type(s)",
            "Source Database(s)",
            "Experimental Role(s) Interactor A",
            "Experimental Role(s) Interactor B",
            "Type(s) Interactor A",
            "Type(s) Interactor B"
        ]
        for mi_column in mi_columns:
            df[mi_column] = df[mi_column].apply(lambda x: self._get_MI_code(x) if x else x)

        # print(df)
        df = self._remove_empty_columns(df)
        # print(df)
        fb_psimi_table = fb_psimi_table.from_pandas_dataframe(df)
        # print(fb_psimi_table)
        return fb_psimi_table


# helper functions:

    def _remove_flybase_str(self, fbgn_string):
        return fbgn_string.split('|')[0].split(':')[-1]

    def _get_taxon_id(self, taxon_id: str):
        '''
        @param: taxon_id: like 'taxid:7227("Drosophila melanogaster")'
        '''
        return taxon_id.split('(')[0].split(':')[1]


    def _get_pubmed_id(self, db_reference_string):
        # return db_reference_string.split('|')[1].split(':')[-1]
        parts = db_reference_string.split('|')
        if len(parts) > 1:
            sub_parts = parts[1].split(':')
            return sub_parts[-1] if sub_parts else None
        return None


    def _get_MI_code(self, mi_string):
        try:
            parts = mi_string.split('"')    
            # print(parts[ len(parts) // 2 ].replace(':', '_'))    
            return parts[ len(parts) // 2 ].replace(':', '_')
        except Exception as e:
            print(f"mi_string:::::--> {mi_string}.  {e}")
            return ''

    # not  used
    def _extract_ids(self, input_string):
        ids = []
        parts = input_string.split("|")
        for part in parts:
            id_part = part.split(":")[-1]
            ids.append(id_part)
        return "|".join(ids)

    def _strip_comments(self, string):
        output_string = re.sub(r'comment:', '', string)
        output_string = re.sub(r'\|', ' AND ', output_string)
        return output_string.strip('"')
    
    def _remove_empty_columns(self, df):
        # Print the names of empty columns
        empty_columns = [column for column in df.columns if df[column].dropna().empty]
        #data_columns = [column for column in df.columns if not df[column].dropna().empty]
        df = df.drop(empty_columns, axis=1)

        return df

    def __build_fbgn_to_uniprot_dict(self):
        table_fbgn_to_uniprot = FlybasePrecomputedTable(self.filepaths[2])
        fbgn_to_uniprot_dict = {}
        for row in table_fbgn_to_uniprot.get_rows():
            fbgn_to_uniprot_dict.setdefault(row[0], []).append(row[3])
        return fbgn_to_uniprot_dict


    def __build_transcript_protein_dict(self):
        table_fbgn_to_fbtr_fbpp = FlybasePrecomputedTable(self.filepaths[1])
        fbgn_to_fbtr_dict = {}
        for row in table_fbgn_to_fbtr_fbpp.get_rows():
            fbgn_to_fbtr_dict.setdefault(row[2], []).append(row[7])  # FBtr

        # some genes aren't in the above table (probably because they don't have known transcripts or software imprecision, I guess), 
        # but they pursui an uniprot id

        fbgn_to_fbtr_uniprot_dict = {}
        # for fbgn in fbgn_to_fbtr_dict.keys():
        for fbgn in self.fbgn_to_uniprot_dict.keys():
            uniprot_list = self.fbgn_to_uniprot_dict[fbgn]          # if fbgn in self.fbgn_to_uniprot_dict else None
            fbtr_list = fbgn_to_fbtr_dict[fbgn] if fbgn in fbgn_to_fbtr_dict else None
            # fbgn_to_fbtr_uniprot_dict[fbgn] = (fbgn_to_fbtr_dict[fbgn], uniprot_list)
            fbgn_to_fbtr_uniprot_dict[fbgn] = (fbtr_list, uniprot_list)

        for fbgn in fbgn_to_fbtr_dict.keys():
        # for fbgn in self.fbgn_to_uniprot_dict.keys():
            uniprot_list = self.fbgn_to_uniprot_dict[fbgn] if fbgn in self.fbgn_to_uniprot_dict else None
            fbtr_list = fbgn_to_fbtr_dict[fbgn]
            # fbgn_to_fbtr_uniprot_dict[fbgn] = (fbgn_to_fbtr_dict[fbgn], uniprot_list)
            fbgn_to_fbtr_uniprot_dict[fbgn] = (fbtr_list, uniprot_list)
            
        return fbgn_to_fbtr_uniprot_dict
        