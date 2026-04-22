import gzip
from biocypher_metta.adapters import Adapter
from biocypher_metta.adapters.helpers import build_regulatory_region_id, check_genomic_location

# Human data:
# https://www.encodeproject.org/
#
# BED-like TSV columns (0-based start):
#   0: chr  1: start  2: end  3: name  4: class  5: TargetGene  6: TargetGeneEnsemblID
#   7: TargetGeneTSS  8: isSelfPromoter  9: CellType  ...  -1: Score


class ENCODERe2GAdapter(Adapter):
    def __init__(self, filepath, taxon_id, write_properties, add_provenance, label=None,
                 chr=None, start=None, end=None, cell_ontology_id=None):
        self.filepath = filepath
        self.chr = chr
        self.start = start
        self.end = end
        self.label = label  # optional override; if None, derived from isSelfPromoter
        self.taxon_id = str(taxon_id) if taxon_id is not None else None
        self.cell_ontology_id = cell_ontology_id  # CL ontology ID for biological_context
        self.source = "ENCODE-rE2G"
        self.source_url = "https://www.encodeproject.org/"

        super().__init__(write_properties, add_provenance)

    @staticmethod
    def _node_label(is_self_promoter):
        return "promoter" if is_self_promoter else "enhancer"

    @staticmethod
    def _edge_label(is_self_promoter):
        return "promoter_gene" if is_self_promoter else "enhancer_activity_by_contact"

    def get_nodes(self):
        with gzip.open(self.filepath, "rt") as f:
            for line in f:
                if line.startswith("#"):
                    continue

                fields = line.strip().split("\t")
                chr = fields[0]
                start = int(fields[1]) + 1  # BED 0-based → 1-based closed
                end = int(fields[2])
                
                if not check_genomic_location(self.chr, self.start, self.end, chr, start, end):
                    continue

                is_self_promoter = fields[8].strip().upper() == "TRUE"
                region_id = f"ENCODE_RE2G:{build_regulatory_region_id(chr, start, end)}"

                node_label = self.label if self.label else self._node_label(is_self_promoter)

                props = {}
                if self.write_properties:
                    props = {
                        "chr": chr,
                        "start": start,
                        "end": end,
                        "taxon_id": self.taxon_id,
                    }

                    if self.add_provenance:
                        props["source"] = self.source
                        props["source_url"] = self.source_url

                yield region_id, node_label, props

    def get_edges(self):
        with gzip.open(self.filepath, "rt") as f:
            for line in f:
                if line.startswith("#"):
                    continue

                fields = line.strip().split("\t")
                chr = fields[0]
                start = int(fields[1]) + 1  # BED 0-based → 1-based closed
                end = int(fields[2])
                is_self_promoter = fields[8].strip().upper() == "TRUE"
                cell_type = fields[9].strip() if len(fields) > 9 else None
                
                if not check_genomic_location(self.chr, self.start, self.end, chr, start, end):
                    continue

                gene_id = f"ENSEMBL:{fields[6].strip()}"
                score = float(fields[-1])
                edge_label = self.label if self.label else self._edge_label(is_self_promoter)
                biological_context = self.cell_ontology_id if self.cell_ontology_id else cell_type
                region_id = f"ENCODE_RE2G:{build_regulatory_region_id(chr, start, end)}"

                props = {}
                if self.write_properties:
                    props = {
                        "score": score,
                        "biological_context": biological_context,
                        "taxon_id": self.taxon_id,
                    }

                    if self.add_provenance:
                        props["source"] = self.source
                        props["source_url"] = self.source_url

                yield region_id, gene_id, edge_label, props
