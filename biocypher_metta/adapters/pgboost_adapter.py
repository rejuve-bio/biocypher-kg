import csv
import gzip
from biocypher_metta.adapters import Adapter
from biocypher._logger import logger
from biocypher_metta.adapters.hgnc_processor import HGNCSymbolProcessor

# sample from pgboost dataset
# rsID	Gene	pgBoost	pgBoost_percentile
# rs190009846	AGL	0.0489052012562752	0.6063455144964238
# rs190009846	CDC14A	0.0259311404079199	0.2963199630836861
# rs190009846	DBT	0.0400206223130226	0.517646544311636
# rs190009846	FRRS1	0.0359481237828732	0.4631402055678426

COL_DICT = {"rsid": 0, "gene": 1, "score": 2, "percentile": 3}

class PgBoostAdapter(Adapter):

    def __init__(self, filepath, write_properties, add_provenance,
                 percentile_threshold=0.95, hgnc_processor=None):
        self.filepath = filepath
        self.percentile_threshold = percentile_threshold
        self.label = 'predicted_variant_gene_association'
        self.source = 'pgBoost'
        self.source_url = 'https://zenodo.org/records/14957607'
        
        if hgnc_processor is None:
            self.hgnc_processor = HGNCSymbolProcessor()
            self.hgnc_processor.update_hgnc_data()
        else:
            self.hgnc_processor = hgnc_processor
        
        logger.info("Building symbol-to-Ensembl mapping...")
        self.symbol_to_ensembl = {}
        for ensembl_id, symbol in self.hgnc_processor.ensembl_to_symbol.items():
            if ensembl_id.startswith('ENSG') and '.' not in ensembl_id:
                self.symbol_to_ensembl[symbol] = ensembl_id
        logger.info(f"Mapped {len(self.symbol_to_ensembl)} symbols to Ensembl IDs")

        super(PgBoostAdapter, self).__init__(write_properties, add_provenance)

    def get_edges(self):
        skipped_count = 0
        processed_count = 0
        
        with gzip.open(self.filepath, 'rt') as f:
            reader = csv.reader(f, delimiter='\t')
            next(reader)  
            
            for row in reader:
                try:
                    rsid = row[COL_DICT["rsid"]]
                    gene_symbol = row[COL_DICT["gene"]]
                    score = float(row[COL_DICT["score"]])
                    percentile = float(row[COL_DICT["percentile"]])
                    
                    if percentile < self.percentile_threshold:
                        continue
                    
                    gene_info = self.hgnc_processor.process_identifier(gene_symbol)
                    current_symbol = gene_info['current']
                    
                    ensembl_id = self.symbol_to_ensembl.get(current_symbol)
                    
                    if not ensembl_id:
                        skipped_count += 1
                        continue
                    
                    _target = f"ENSEMBL:{ensembl_id}"
                    _source = f"DBSNP:{rsid}"
                    
                    _props = {}
                    if self.write_properties:
                        _props = {
                            'score': score,
                            'percentile': percentile,
                        }
                        if self.add_provenance:
                            _props['source'] = self.source
                            _props['source_url'] = self.source_url
                    
                    processed_count += 1
                    yield _source, _target, self.label, _props
                    
                except Exception as e:
                    logger.warning(f"Error processing row: {row}, Error: {e}")
                    continue
        
        logger.info(f"Processed {processed_count} edges, skipped {skipped_count} genes without Ensembl IDs")