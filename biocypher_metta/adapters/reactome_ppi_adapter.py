from biocypher_metta.adapters import Adapter
import csv
import gzip


# Sample from the dataset
# Interactor 1 uniprot id	Interactor 1 Ensembl gene id	Interactor 1 Entrez Gene id	Interactor 2 uniprot id	Interactor 2 Ensembl gene id	Interactor 2 Entrez Gene id	Interaction type	Interaction context	Pubmed references
# uniprotkb:Q9Y287	ENST00000647800|ENST00000378549|ENSP00000497221|ENSP00000367811|ENSG00000136156	-	uniprotkb:Q9Y287	ENST00000647800|ENST00000378549|ENSP00000497221|ENSP00000367811|ENSG00000136156	-	physical association	reactome:R-HSA-976871	14690516|10391242
# uniprotkb:P37840	ENSG00000145335|ENST00000505199|ENST00000618500|ENST00000394986|ENST00000506244|ENST00000336904|ENST00000508895|ENST00000394989|ENST00000394991|ENST00000420646|ENST00000345009|ENST00000673718|ENSP00000378437|ENSP00000484044|ENSP00000421485|ENSP00000378442|ENSP00000343683|ENSP00000338345|ENSP00000500990|ENSP00000396241|ENSP00000426955|ENSP00000422238|ENSP00000378440	-	uniprotkb:P37840	ENSG00000145335|ENST00000505199|ENST00000618500|ENST00000394986|ENST00000506244|ENST00000336904|ENST00000508895|ENST00000394989|ENST00000394991|ENST00000420646|ENST00000345009|ENST00000673718|ENSP00000378437|ENSP00000484044|ENSP00000421485|ENSP00000378442|ENSP00000343683|ENSP00000338345|ENSP00000500990|ENSP00000396241|ENSP00000426955|ENSP00000422238|ENSP00000378440	-	physical association	reactome:R-HSA-1247852	24243840
# uniprotkb:P0DJI8	ENST00000532858|ENST00000672418|ENST00000672712|ENST00000356524|ENST00000405158|ENST00000672662|ENSP00000500281|ENSP00000384906|ENSP00000436866|ENSP00000500630|ENSP00000348918|ENSP00000500639|ENSG00000173432|ENSG00000288411	-	uniprotkb:P0DJI8	ENST00000532858|ENST00000672418|ENST00000672712|ENST00000356524|ENST00000405158|ENST00000672662|ENSP00000500281|ENSP00000384906|ENSP00000436866|ENSP00000500630|ENSP00000348918|ENSP00000500639|ENSG00000173432|ENSG00000288411	-	physical association	reactome:R-HSA-976898	19393650|103558
# uniprotkb:P06727	ENST00000357780|ENSP00000350425|ENSG00000110244	-	uniprotkb:P06727	ENST00000357780|ENSP00000350425|ENSG00000110244	-	physical association	reactome:R-HSA-976889	15146166

class ReactomePPIAdapter(Adapter):
    def __init__(self, filepath, write_properties, add_provenance, label, taxon_id,
                 include_self_interactions=True):
        
        self.filepath = filepath
        self.include_self_interactions = include_self_interactions
        self.taxon_id = taxon_id
        self.label = label
        self.source = "Reactome"
        self.source_url = "https://reactome.org/"
        
        self.seen_interactions = set()        
        super(ReactomePPIAdapter, self).__init__(write_properties, add_provenance)

    def _extract_reactome_pathway(self, context):
        if context and context.startswith('reactome:'):
            return context.replace('reactome:', '')
        return context

    def get_edges(self):
        try:
            if self.filepath.endswith('.gz'):
                file_handle = gzip.open(self.filepath, "rt")
            else:
                file_handle = open(self.filepath, "r")
            
            with file_handle as fp:
                for line in fp:
                    if not line.startswith('#'):
                        fp.seek(0)  
                        break
                
                reader = csv.reader(fp, delimiter='\t')
                
                header = next(reader, None)
                if header and header[0].startswith('#'):
                    pass  
                else:
                    if header:
                        row = header
                        if len(row) >= 9:
                            yield from self._process_row(row)
                
                for row in reader:
                    if len(row) >= 9: 
                        yield from self._process_row(row)
                        
        except Exception as e:
            print(f"Error processing file {self.filepath}: {e}")
            return

    def _process_row(self, row):
        try:
            if row[0].startswith('ChEBI:') or row[3].startswith('ChEBI:'):
                return
                
            protein1_uniprot = row[0].replace('uniprotkb:', '') if row[0].startswith('uniprotkb:') else row[0]
            protein2_uniprot = row[3].replace('uniprotkb:', '') if row[3].startswith('uniprotkb:') else row[3]
            
            interaction_type = row[6]
            interaction_context = row[7]
            
            if not self.include_self_interactions and protein1_uniprot == protein2_uniprot:
                return
            
            interaction_key = tuple(sorted([protein1_uniprot, protein2_uniprot]))
            
            if interaction_key in self.seen_interactions:
                return
            
            self.seen_interactions.add(interaction_key)
                
            _source = f"{protein1_uniprot}"
            _target = f"{protein2_uniprot}"
            
            _props = {}
            if self.write_properties:
                pathway_id = self._extract_reactome_pathway(interaction_context)
                
                _props = {
                    "interaction_type": interaction_type,
                    "reactome_pathway": pathway_id,
                    "taxon_id": self.taxon_id,
                }
                
                if self.add_provenance:
                    _props["source"] = self.source
                    _props["source_url"] = self.source_url
            
            yield _source, _target, self.label, _props
            
        except Exception as e:
            print(f"reactome_ppi_adapter: error processing row: {e}")
            return