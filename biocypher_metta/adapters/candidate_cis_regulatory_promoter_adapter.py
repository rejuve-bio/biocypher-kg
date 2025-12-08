import gzip
import pickle
from biocypher_metta.adapters import Adapter

# sample dataset from closest gene dataset
# chr1	10033	10250	EH38D4327497	EH38E2776516	pELS	chr1	11869	11869	ENST00000456328.2	.	+	ENSG00000223972.5
# chr1	10385	10713	EH38D4327498	EH38E2776517	pELS	chr1	11869	11869	ENST00000456328.2	.	+	ENSG00000223972.5
# chr1	16097	16381	EH38D6144701	EH38E3951272	CA-CTCF	chr1	17436	17436	ENST00000619216.1	.	-	ENSG00000278267.1
# chr1	17343	17642	EH38D6144702	EH38E3951273	CA-TF	chr1	17436	17436	ENST00000619216.1	.	-	ENSG00000278267.1

# sample dataset from eqtl dataset
# EH38E3951273	ENSG00000225972	MTND1P23 	unprocessed_pseudogene	chr1_17559_G_C_b38	GTEx:V8	Colon Transverse	-0.918039	0.00423421
# EH38E3951273	ENSG00000187608	ISG15 	protein_coding	chr1_17407_G_A_b38	GTEx:V8	Pituitary	1.24704	0.0313818
# EH38E3951273	ENSG00000238009	ENSG00000238009 	lncRNA	chr1_17556_C_T_b38	GTEx:V8	Spleen	1.13312	8.84078e-09
# EH38E3951273	ENSG00000269981	ENSG00000269981 	processed_pseudogene	chr1_17556_C_T_b38	GTEx:V8	Spleen	0.58546	4.53944e-05


class PromotercCREAdapter(Adapter):
    def __init__(
        self, filepath, eqtl_filepath, taxon_id, gtex_tissue_ontology_map=None, write_properties=True, add_provenance=True, label="promoter", edge_type=None):
        self.filepath = filepath
        self.eqtl_filepath = eqtl_filepath
        self.gtex_tissue_ontology_map = gtex_tissue_ontology_map
        self.label = label
        self.taxon_id = taxon_id
        self.source = "ENCODE"
        self.source_url = "https://screen.wenglab.org/downloads"
        
        self.edge_type = edge_type
        if self.edge_type is None:
            self.edge_type = ['nearest', 'eqtl']
        elif isinstance(self.edge_type, str):
            self.edge_type = [self.edge_type]
        
        self.accession_to_promoter = {}
        self.accession_to_eqtl = {}
        self.tissue_to_ontology = {}
        
        super(PromotercCREAdapter, self).__init__(write_properties, add_provenance)
        
        if self.gtex_tissue_ontology_map:
            self._load_tissue_ontology_map()
            
        self._preload_data()
    
    def _load_tissue_ontology_map(self):
        try:
            with open(self.gtex_tissue_ontology_map, 'rb') as f:
                self.tissue_to_ontology = pickle.load(f)
        except Exception as e:
            print(f"Error loading tissue ontology map: {e}")
    
    def _normalize_tissue_name(self, tissue_name):
        return tissue_name.replace(" ", "_")
    
    def _get_ontology_id(self, tissue_name):
        if not tissue_name or not self.tissue_to_ontology:
            return tissue_name
        
        normalized_name = self._normalize_tissue_name(tissue_name)
        
        ontology_id = self.tissue_to_ontology.get(normalized_name)
        
        return ontology_id if ontology_id else tissue_name
    
    def _preload_data(self):
        try:
            if self.filepath.endswith('.gz'):
                file = gzip.open(self.filepath, 'rt')
            else:
                file = open(self.filepath, 'rt')
            
            for line in file:
                if line.startswith('#'):
                    continue
                    
                fields = line.strip().split("\t")
                
                if len(fields) < 8:
                    continue
                
                element_type = fields[5]
                is_promoter = element_type.startswith("PLS") or "pls" in element_type.lower()
                
                if not is_promoter:
                    continue
                
                chrom = fields[0]
                start = int(fields[1]) + 1  
                end = int(fields[2]) + 1   
                
                if len(fields) <= 4 or not fields[4]:
                    continue
                    
                accession = fields[4]  
                
                gene_id = None
                if len(fields) >= 13:
                    gene_id = fields[12]  
                elif len(fields) >= 7:
                    gene_id = fields[6]  
                
                if not gene_id or not accession:
                    continue
                
                if gene_id and '.' in gene_id:
                    gene_id = gene_id.split('.')[0]
                
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
                
                # Use SO:0000167 for promoter (Sequence Ontology term)
                element_id = f"SO:0000167:{chrom}_{start}_{end}"
                self.accession_to_promoter[accession] = {
                    'chrom': chrom,
                    'start': start,
                    'end': end,
                    'element_id': element_id,
                    'nearest_gene': gene_id,
                    'distance': distance
                }
            
            file.close()
                
        except Exception as e:
            print(f"Error loading promoter data: {e}")
        
        if 'eqtl' in self.edge_type:
            try:
                if self.eqtl_filepath.endswith('.gz'):
                    file = gzip.open(self.eqtl_filepath, 'rt')
                else:
                    file = open(self.eqtl_filepath, 'rt')
                
                for line in file:
                    if line.startswith('#'):
                        continue
                    
                    fields = line.strip().split('\t')
                    
                    if len(fields) < 7:  
                        continue
                    
                    accession = fields[0]
                    gene_id = fields[1]
                    
                    if not accession or not gene_id:
                        continue
                    
                    if gene_id and '.' in gene_id:
                        gene_id = gene_id.split('.')[0]
                    
                    tissue = fields[6] if len(fields) > 6 else "unknown"
                    
                    if accession not in self.accession_to_eqtl:
                        self.accession_to_eqtl[accession] = []
                    
                    self.accession_to_eqtl[accession].append({
                        'gene_id': gene_id,
                        'tissue': tissue,
                    })
                
                file.close()
                
            except Exception as e:
                print(f"Error loading eQTL data: {e}")
    
    def get_nodes(self):
        for accession, promoter_data in self.accession_to_promoter.items():
            element_id = promoter_data['element_id']
            
            props = {}
            if self.write_properties:
                props.update({
                    'chr': promoter_data['chrom'],
                    'start': promoter_data['start'],
                    'end': promoter_data['end'],
                    'accession': accession,
                })

                if self.add_provenance:
                    props['source'] = self.source
                    props['source_url'] = self.source_url

            yield element_id, self.label, props
    
    def get_edges(self):
        if 'nearest' in self.edge_type:
            for accession, promoter_data in self.accession_to_promoter.items():
                element_id = promoter_data['element_id']
                gene_id = promoter_data['nearest_gene']
                
                if not gene_id:
                    continue
                
                # Add  prefix to gene_id
                gene_id = f"{gene_id}"
                
                props = {
                    'distance': promoter_data['distance'],
                }
                
                if self.add_provenance:
                    props['source'] = self.source
                    props['source_url'] = self.source_url
                
                yield element_id, gene_id, self.label, props
        
        if 'eqtl' in self.edge_type:
            common_accessions = set(self.accession_to_promoter.keys()) & set(self.accession_to_eqtl.keys())
            
            for accession in common_accessions:
                promoter_data = self.accession_to_promoter[accession]
                element_id = promoter_data['element_id']
                
                for eqtl_data in self.accession_to_eqtl[accession]:
                    gene_id = eqtl_data['gene_id']
                    
                    if not gene_id:
                        continue
                    
                    gene_id = f"{gene_id}"
                    
                    tissue = eqtl_data['tissue']
                    ontology_id = self._get_ontology_id(tissue)
                    
                    props = {
                        'biological_context': ontology_id
                    }
                    
                    if self.add_provenance:
                        props['source'] = self.source
                        props['source_url'] = self.source_url
                    
                    yield element_id, gene_id, self.label, props