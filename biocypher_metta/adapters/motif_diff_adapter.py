import csv
from biocypher_metta.adapters import Adapter
from biocypher._logger import logger
from biocypher_metta.adapters.helpers import to_float
from biocypher_metta.processors import HGNCProcessor

class MotifDiffAdapter(Adapter):
    def __init__(self, filepath, hgnc_to_ensembl=None, label=None, write_properties=None,
                 add_provenance=None, threshold=1e-3, hgnc_processor=None):
        self.filepath = filepath
        self.label = label
        self.threshold = threshold

        # Use provided processor or create new one
        if hgnc_processor is None:
            self.hgnc_processor = HGNCProcessor()
            self.hgnc_processor.load_or_update()
        else:
            self.hgnc_processor = hgnc_processor

        self.source = 'MotifDiff'
        self.source_url = 'https://github.com/rezwanhosseini/MotifDiff'
        super(MotifDiffAdapter, self).__init__(write_properties, add_provenance)
    
    def parse_tf_id(self, tf_id):
        if '_HUMAN' in tf_id:
            return tf_id.split('_HUMAN')[0]
        return tf_id
    
    def get_edges(self):
        with open(self.filepath, 'r') as f:
            reader = csv.reader(f, delimiter='\t')
        
            header = next(reader)
        
            for row in reader:
                if not row: 
                    continue
                
                variant_rsid = row[0]
            
                best_motifs = {}
            
                for i, score_str in enumerate(row[1:], 1):
                    tf_id = header[i]
                    hgnc_symbol = self.parse_tf_id(tf_id)
                
                    try:
                        score = float(score_str)
                    except ValueError:
                        continue
                
                    if abs(score) < self.threshold:
                        continue
                
                    parts = tf_id.split('.')
                    confidence = parts[-1] if parts else 'Z'  
                
                    if hgnc_symbol not in best_motifs:
                        best_motifs[hgnc_symbol] = (confidence, score, tf_id)
                    else:
                        current_confidence, current_score, current_id = best_motifs[hgnc_symbol]
                    
                        # Higher confidence (A > B > C), or same confidence with higher absolute score
                        if confidence < current_confidence:
                            best_motifs[hgnc_symbol] = (confidence, score, tf_id)
                        elif confidence == current_confidence and abs(score) > abs(current_score):
                            best_motifs[hgnc_symbol] = (confidence, score, tf_id)
            
                for hgnc_symbol, (confidence, score, tf_id) in best_motifs.items():
                    tf_ensembl = self.hgnc_processor.get_ensembl_id(hgnc_symbol)

                    if tf_ensembl is None:
                        logger.warning(f"Couldn't find Ensembl ID for TF {hgnc_symbol}")
                        continue
                
                    effect = "gain" if score > 0 else "loss"
                
                    props = {}
                    if self.write_properties:
                        props['effect'] = effect
                        props['score'] = to_float(score)
                        props['motif_id'] = tf_id 
                        if self.add_provenance:
                            props['source'] = self.source
                            props['source_url'] = self.source_url

                    yield tf_ensembl, variant_rsid, self.label, props