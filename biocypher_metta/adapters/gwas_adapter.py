import csv
import os
import pickle
from biocypher_metta.adapters import Adapter
from biocypher_metta.adapters.helpers import to_float, check_genomic_location
from biocypher._logger import logger
import gzip


class GWASAdapter(Adapter):

    index = {
        "rsid": 21,
        "in_gene": 17,
        "upstream_gene": 15,
        "downstream_gene": 16,
        "p_value": 27,
        "chr": 11,
        "pos": 12,
    }

    def __init__(
        self,
        filepath,
        write_properties,
        add_provenance,
        label,
        chr=None,
        start=None,
        end=None,
    ):
        self.filepath = filepath
        self.chr = chr
        self.start = start
        self.end = end
        self.label = label
        self.source = "GWAS"
        self.source_url = "https://ftp.ebi.ac.uk/pub/databases/gwas/releases/2024/07/29/gwas-catalog-associations_ontology-annotated.tsv"

        super(GWASAdapter, self).__init__(write_properties, add_provenance)

    def get_edges(self):
        with gzip.open(self.filepath, "rt") as gwas:
            next(gwas)  # skip header
            gwas_row = csv.reader(gwas)
            for row in gwas_row:
                try:
                    chr, pos = row[self.index["chr"]], row[self.index["pos"]]
                    if pos is not None:
                        pos = int(pos)
                    else:
                        continue
                    variant_id = row[self.index["rsid"]]
                    if not row[self.index[self.label]]:
                        continue
                    gene_id = row[self.index[self.label]]
                    if check_genomic_location(
                        self.chr, self.start, self.end, chr, pos, pos
                    ):
                        _source = variant_id
                        _target = gene_id
                        _props = {}
                        if self.write_properties:
                            _props = {
                                "p_value": to_float(row[self.index["p_value"]]),
                            }
                            if self.add_provenance:
                                _props["source"] = self.source
                                _props["source_url"] = self.source_url

                        yield _source, _target, self.label, _props
                except Exception as e:
                    print(row)
                    print(e)
