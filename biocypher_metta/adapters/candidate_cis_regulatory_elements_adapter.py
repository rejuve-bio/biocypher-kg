import gzip
from biocypher_metta.adapters import Adapter

class EncodecCREAdapter(Adapter):
    def __init__(self, filepath, write_properties=True, add_provenance=True, label='regulatory_element'):
        self.filepath = filepath
        self.label = label
        self.source = "ENCODE cCRE"
        self.source_url = "https://screen.wenglab.org/downloads"

        super(EncodecCREAdapter, self).__init__(write_properties, add_provenance)

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

                    props = {
                        'distance': distance
                    }

                    if self.add_provenance:
                        props['source'] = self.source
                        props['source_url'] = self.source_url

                    element_id = f"{chrom}_{start}_{end}"
                    yield element_id, nearest_gene, self.label, props