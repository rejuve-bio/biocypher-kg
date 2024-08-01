# Author Abdulrahman S. Omar <xabush@singularitynet.io>
from biocypher_metta.adapters import Adapter
import pickle
import csv
import gzip

# Transcription factor - target gene relationships from TFLink

# Human
# UniprotID.TF	UniprotID.Target	NCBI.GeneID.TF	NCBI.GeneID.Target	Name.TF	Name.Target	Detection.method    PubmedID	Organism	Source.database	Small-scale.evidence	TF.TFLink.ortho	TF.nonTFLink.ortho	Target.TFLink.ortho	Target.nonTFLink.ortho
# Q9H9S0	O94907	79923	22943	NANOG	DKK1	chromatin immunoprecipitation assay;inferred by curator	19148141;29087512;29126285;27924024	Homo sapiens	GTRD;ReMap;TRRUST	Yes	-	-	Dr:Q9PWH3;Dr:F1RBK0;Mm:O54908	Rn:D3Z9J1
# P37231	P10826	5468	5915	PPARG	RARB	chromatin immunoprecipitation assay;inferred by curator	17202159;12839938;29087512;27924024	Homo sapiens	GTRD;TRED;TRRUST	Yes	-	-	Mm:P22605;Rn:D3ZFD9	-
# P10242	P08047	4602	6667	MYB	SP1	chromatin immunoprecipitation assay;inferred by curator	29126285;27924024;17202159	Homo sapiens	GTRD;ReMap;TRED	Yes	Dr:F1QP24;Rn:A0A0G2K2A4	Mm:A0A087WPA7	Dr:F1QW97;Rn:Q01714	Mm:G3X8Q0

# Fly
# UniprotID.TF	UniprotID.Target	NCBI.GeneID.TF	NCBI.GeneID.Target	Name.TF	Name.Target	Detection.method	PubmedID	Organism    Source.database	Small-scale.evidence	TF.TFLink.ortho	TF.nonTFLink.ortho	Target.TFLink.ortho	Target.nonTFLink.ortho
# P02836	P02836	36240	36240	en	en	DNase I footprinting;chromatin immunoprecipitation assay;inferred by curator	2895896;26578589;20965965;27924024;2573829	Drosophila melanogaster	GTRD;ORegAnno;REDfly	Yes	-	-	-	-
# O61735	P49021	38872	33571	Clk	tim	chromatin immunoprecipitation assay;experimental interaction detection	9616122;20965965;27924024	Drosophila melanogaster	GTRD;REDfly	Yes	Dr:Q5RIV1;Rn:F1LRL5	-	-	-
# Q9VN10	P39770	40549	34569	hkb	salm	DNase I footprinting;inferred by curator	20965965;9376314;26578589	Drosophila melanogaster	ORegAnno;REDfly	Yes	-	-	Hs:Q9BXA9	Dr:A0A1D5NSE4;Mm:A0A5F8MPC9

class TFLinkAdapter(Adapter):
    INDEX = {'NCBI.GeneID.TF': 2, 'NCBI.GeneID.Target': 3, 'Detection.method': 6, 'PubmedID': 7, 'Source.database': 9, 'Small-scale.evidence': 10}
    def __init__(self, dmel_filepath, dmel_entrez_to_ensemble_map, hsa_filepath, hsa_entrez_to_ensemble_map,
                 write_properties, add_provenance):
        """
        Constructs TFLink adapter that returns edges between TFs and their target gene
        :param filepath: Path to the TSV file downloaded from tflink
        :param entrez_to_ensemble_map: file containing pickled dictionary mapping NCBI Entrez IDs to Ensemble IDs -
        this b/c we use Ensemble IDs to identify genes where TFLink uses Entrez Ids
        """
        self.dmel_filepath = dmel_filepath
        with open(dmel_entrez_to_ensemble_map, "rb") as f:
            self.dmel_entrez2ensemble = pickle.load(f)

        self.hsa_filepath = hsa_filepath
        with open(hsa_entrez_to_ensemble_map, "rb") as f:
            self.hsa_entrez2ensemble = pickle.load(f)

        self.label = "tf_gene"
        self.source = "TFLink"
        self.source_url = "tflink.net"

        super(TFLinkAdapter, self).__init__(write_properties, add_provenance)

    def get_edges(self):
        with gzip.open(self.dmel_filepath, 'rt') as fp:
            table = csv.reader(fp, delimiter="\t", quotechar='"')
            for row in table:
                tf_entrez_id = row[TFLinkAdapter.INDEX['NCBI.GeneID.TF']]
                target_entrez_id = row[TFLinkAdapter.INDEX['NCBI.GeneID.Target']]
                if tf_entrez_id in self.dmel_entrez2ensemble and target_entrez_id in self.dmel_entrez2ensemble:
                    tf_ensemble_id = self.dmel_entrez2ensemble[tf_entrez_id]
                    target_ensemble_id = self.dmel_entrez2ensemble[target_entrez_id]
                    _source = tf_ensemble_id
                    _target = target_ensemble_id
                    pubmed_ids_str = row[TFLinkAdapter.INDEX['PubmedID']]
                    pubmed_ids = [f"pubmed:{i}" for i in pubmed_ids_str.split(";")]
                    sources = row[TFLinkAdapter.INDEX['Source.database']].split(";")
                    small_scale_evidence = row[TFLinkAdapter.INDEX['Small-scale.evidence']]
                    if small_scale_evidence == "Yes":
                        evidence_type = "small_scale_evidence"
                    else:
                        evidence_type = "large_scale_evidence"
                    _props = {}
                    if self.write_properties:
                        _props = {
                            "evidence": pubmed_ids,
                            "databases": sources,
                            "evidence_type": evidence_type,
                            "detection_method": row[TFLinkAdapter.INDEX['Detection.method']]
                        }
                        if self.add_provenance:
                            _props['source'] = self.source
                            _props['source_url'] = self.source_url
                    _props['taxon_id'] = 7227

                    yield _source, _target, self.label, _props

        with gzip.open(self.hsa_filepath, 'rt') as fp:
            table = csv.reader(fp, delimiter="\t", quotechar='"')
            for row in table:
                tf_entrez_id = row[TFLinkAdapter.INDEX['NCBI.GeneID.TF']]
                target_entrez_id = row[TFLinkAdapter.INDEX['NCBI.GeneID.Target']]
                if tf_entrez_id in self.hsa_entrez2ensemble and target_entrez_id in self.hsa_entrez2ensemble:
                    tf_ensemble_id = self.hsa_entrez2ensemble[tf_entrez_id]
                    target_ensemble_id = self.hsa_entrez2ensemble[target_entrez_id]
                    _source = tf_ensemble_id
                    _target = target_ensemble_id
                    pubmed_ids_str = row[TFLinkAdapter.INDEX['PubmedID']]
                    pubmed_ids = [f"pubmed:{i}" for i in pubmed_ids_str.split(";")]
                    sources = row[TFLinkAdapter.INDEX['Source.database']].split(";")
                    small_scale_evidence = row[TFLinkAdapter.INDEX['Small-scale.evidence']]
                    if small_scale_evidence == "Yes":
                        evidence_type = "small_scale_evidence"
                    else:
                        evidence_type = "large_scale_evidence"
                    _props = {}
                    if self.write_properties:
                        _props = {
                            "evidence": pubmed_ids,
                            "databases": sources,
                            "evidence_type": evidence_type,
                            "detection_method": row[TFLinkAdapter.INDEX['Detection.method']]
                        }
                        if self.add_provenance:
                            _props['source'] = self.source
                            _props['source_url'] = self.source_url
                    _props['taxon_id'] = 9606

                    yield _source, _target, self.label, _props
