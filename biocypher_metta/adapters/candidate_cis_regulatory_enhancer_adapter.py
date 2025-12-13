import gzip
from biocypher_metta.adapters import Adapter

# sample dataset from closest gene dataset
# chr1	10033	10250	EH38D4327497	EH38E2776516	pELS	chr1	11869	11869	ENST00000456328.2	.	+	ENSG00000223972.5
# chr1	10385	10713	EH38D4327498	EH38E2776517	pELS	chr1	11869	11869	ENST00000456328.2	.	+	ENSG00000223972.5
# chr1	16097	16381	EH38D6144701	EH38E3951272	CA-CTCF	chr1	17436	17436	ENST00000619216.1	.	-	ENSG00000278267.1
# chr1	17343	17642	EH38D6144702	EH38E3951273	CA-TF	chr1	17436	17436	ENST00000619216.1	.	-	ENSG00000278267.1

class EnhancercCREAdapter(Adapter):
    def __init__(self, filepath, taxon_id, label, write_properties=True, add_provenance=True, element_filter=None):
        self.filepath = filepath
        self.taxon_id = taxon_id
        self.label = label
        self.source = "ENCODE"
        self.source_url = "https://screen.wenglab.org/downloads"
        
        if element_filter == "proximal":
            self.element_types_to_include = {"pELS"}
        elif element_filter == "distal":
            self.element_types_to_include = {"dELS"}
        else:
            self.element_types_to_include = {"pELS", "dELS"}
        
        super(EnhancercCREAdapter, self).__init__(write_properties, add_provenance)

    def get_nodes(self):
        try:
            if self.filepath.endswith('.gz'):
                file = gzip.open(self.filepath, 'rt')
            else:
                file = open(self.filepath, 'rt')
                
            for line in file:
                if line.startswith('#'):
                    continue
                    
                fields = line.strip().split("\t")
                
                if len(fields) < 6:
                    continue
                    
                chrom = fields[0]
                start = int(fields[1]) + 1  
                end = int(fields[2]) + 1    
                element_type = fields[5]
                
                if element_type not in self.element_types_to_include:
                    continue
                
                accession = fields[4] if len(fields) > 4 else None
                
                if not accession:
                    continue

                props = {}
                if self.write_properties:
                    props.update({
                        'chr': chrom,
                        'start': start,
                        'end': end,
                        'accession': accession,
                    })

                    if self.add_provenance:
                        props['source'] = self.source
                        props['source_url'] = self.source_url

                #  SO:000165 for enhancer (Sequence Ontology term)
                element_id = f"SO:000165:{chrom}_{start}_{end}"
                yield element_id, self.label, props
            
            file.close()
                
        except Exception as e:
            print(f"Error loading enhancer data: {e}")
    
    def get_edges(self):
        try:
            if self.filepath.endswith('.gz'):
                file = gzip.open(self.filepath, 'rt')
            else:
                file = open(self.filepath, 'rt')
                
            for line in file:
                if line.startswith('#'):
                    continue
                    
                fields = line.strip().split("\t")
                
                if len(fields) < 13:  
                    continue
                    
                chrom = fields[0]
                start = int(fields[1]) + 1  
                end = int(fields[2]) + 1    
                element_type = fields[5]
                
                if element_type not in self.element_types_to_include:
                    continue
                
                gene_id = fields[12]  
                
                if not gene_id:
                    continue
                
                if gene_id and '.' in gene_id:
                    gene_id = gene_id.split('.')[0]
                
                # Add  prefix like in first code
                gene_id = f"ENSEMBL:{gene_id}"
                
                distance = "NA"  
                if len(fields) >= 9:
                    try:
                        gene_pos = int(fields[8])  
                        
                        if gene_pos < start:
                            distance = start - gene_pos
                        elif gene_pos > end:
                            distance = gene_pos - end
                        else:
                            distance = 0
                            
                    except (ValueError, IndexError):
                        distance = "NA"

                props = {
                    'distance': distance,
                }

                if self.add_provenance:
                    props['source'] = self.source
                    props['source_url'] = self.source_url

                # SO:000165 for enhancer (Sequence Ontology term)
                element_id = f"SO:000165:{chrom}_{start}_{end}"
                yield element_id, gene_id, self.label, props
            
            file.close()
                
        except Exception as e:
            print(f"Error processing enhancer edges: {e}")