import gzip
import pickle
from biocypher_metta.adapters import Adapter
from biocypher_metta.adapters.hgnc_processor import HGNCSymbolProcessor

class EncodecCREAdapter(Adapter):
    def __init__(self, filepath, write_properties, add_provenance, label, hgnc_pickle_path='hgnc_gene_data/hgnc_data.pkl'):
        self.filepath = filepath
        self.label = label
        self.source = "ENCODE cCRE"
        self.source_url = "https://screen.wenglab.org/downloads"
        self.hgnc_pickle_path = hgnc_pickle_path

        self.hgnc_processor = HGNCSymbolProcessor()
        self.hgnc_processor.update_hgnc_data()
        
        try:
            with open(self.hgnc_pickle_path, 'rb') as f:
                data = pickle.load(f)
            
            self.current_symbols = data['current_symbols']
            self.symbol_aliases = data['symbol_aliases']
            self.ensembl_to_symbol = data['ensembl_to_symbol']
            
            self.symbol_to_ensembl = {}
            for ensembl_id, symbol in self.ensembl_to_symbol.items():
                if ensembl_id.startswith('ENSG') and '.' not in ensembl_id:
                    self.symbol_to_ensembl[symbol] = ensembl_id
            
            print(f"HGNC data loaded from {self.hgnc_pickle_path}")
        except Exception as e:
            print(f"Error loading HGNC data: {e}")
            print("Proceeding without HGNC data")
            self.current_symbols = {}
            self.symbol_aliases = {}
            self.ensembl_to_symbol = {}
            self.symbol_to_ensembl = {}

        super(EncodecCREAdapter, self).__init__(write_properties, add_provenance)

    def _get_ensembl_id(self, gene_symbol):
        """Convert gene symbol to Ensembl ID using HGNC data"""
        if gene_symbol.startswith('ENSG'):
            return gene_symbol.split('.')[0]
        
        if gene_symbol in self.current_symbols:
            current_symbol = gene_symbol
        elif gene_symbol in self.symbol_aliases:
            current_symbol = self.symbol_aliases[gene_symbol]
        else:
            return gene_symbol
        
        return self.symbol_to_ensembl.get(current_symbol, current_symbol)

    def get_nodes(self):
        with gzip.open(self.filepath, 'rt') as file:
            for line in file:
                if line.startswith('#'):
                    continue
                    
                if line.startswith("chr"):
                    fields = line.strip().split("\t")
                    chrom = fields[0]
                    start = int(fields[1]) + 1
                    end = int(fields[2]) + 1
                    accession_d = fields[3]
                    accession_e = fields[4]

                    props = {}
                    if self.write_properties:
                        props.update({
                            'chr': chrom,
                            'start': start,
                            'end': end,
                            'accession_d': accession_d,
                            'accession_e': accession_e,
                        })

                        if self.add_provenance:
                            props['source'] = self.source
                            props['source_url'] = self.source_url

                    element_id = f"{chrom}_{start}_{end}"
                    yield element_id, self.label, props
    
    def get_edges(self):
        with gzip.open(self.filepath, 'rt') as file:
            for line in file:
                if line.startswith('#'):
                    continue
                    
                if line.startswith("chr"):
                    fields = line.strip().split("\t")
                    chrom = fields[0]
                    start = int(fields[1]) + 1
                    end = int(fields[2]) + 1
                    nearest_gene = fields[6]
                    distance = int(fields[7])

                    gene_id = self._get_ensembl_id(nearest_gene)

                    props = {
                        'distance': distance
                    }

                    if self.add_provenance:
                        props['source'] = self.source
                        props['source_url'] = self.source_url

                    element_id = f"{chrom}_{start}_{end}"
                    yield element_id, gene_id, self.label, props
    
    