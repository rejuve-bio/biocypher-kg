# Author Abdulrahman S. Omar <xabush@singularitynet.io>

from biocypher_metta.adapters import Adapter
from biocypher_metta.adapters.helpers import check_genomic_location, build_regulatory_region_id

# Human data:
# https://cb.csail.mit.edu/cb/tadmap/
# https://cb.csail.mit.edu/tadmap/TADMap_scaffold_hs.bed

## Example data:
# 1|chr1|800000|1350000,SAMD11|ENSG00000187634|HGNC:SAMD11;NOC2L|ENSG00000188976|HGNC:NOC2L;KLHL17|ENSG00000187961|HGNC:KLHL17;PLEKHN1|ENSG00000187583|HGNC:PLEKHN1;PERM1|ENSG00000187642|HGNC:PERM1;HES4|ENSG00000188290|HGNC:HES4;ISG15|ENSG00000187608|HGNC:ISG15;AGRN|ENSG00000188157|HGNC:AGRN;RNF223|ENSG00000237330|HGNC:RNF223;C1orf159|ENSG00000131591|HGNC:C1orf159;TTLL10|ENSG00000162571|HGNC:TTLL10;TNFRSF18|ENSG00000186891|HGNC:TNFRSF18;TNFRSF4|ENSG00000186827|HGNC:TNFRSF4;SDF4|ENSG00000078808|HGNC:SDF4;B3GALT6|ENSG00000176022|HGNC:B3GALT6;C1QTNF12|ENSG00000184163|HGNC:C1QTNF12;UBE2J2|ENSG00000160087|HGNC:UBE2J2;SCNN1D|ENSG00000162572|HGNC:SCNN1D;ACAP3|ENSG00000131584|HGNC:ACAP3;PUSL1|ENSG00000169972|HGNC:PUSL1;INTS11|ENSG00000127054|HGNC:INTS11;CPTP|ENSG00000224051|HGNC:CPTP;TAS1R3|ENSG00000169962|HGNC:TAS1R3;DVL1|ENSG00000107404|HGNC:DVL1

# Mouse data:
# https://cb.csail.mit.edu/cb/tadmap/
# https://cb.csail.mit.edu/tadmap/TADMap_scaffold_mm.bed


class TADMapAdapter(Adapter):
    """
    Adapter for Topologically Associated Domain (TAD) data.
    TAD are contiguous segments of the genome where the genomic elements are in frequent contact with each other.
    Source : TADMap https://cb.csail.mit.edu/cb/tadmap/
    """
    INDEX = {'loc_info': 0, 'genes': 1, 'chr': 1, 'start': 2, 'end': 3}

    def __init__(self, filepath, write_properties, add_provenance, taxon_id, label="tad",
                 chr=None, start=None, end=None):
        """
        :type filepath: str
        :type chr: str
        :type start: int
        :type end: int
        :param filepath: path to the TAD file
        :param chr: chromosome name
        :param start: start position
        :param end: end position
        """
        self.filepath = filepath
        self.dataset = 'tad'
        self.source = 'TADMap'
        self.source_url = 'https://cb.csail.mit.edu/cb/tadmap/'

        self.chr = chr
        self.start = start
        self.end = end

        self.label = label

        super(TADMapAdapter, self).__init__(write_properties, add_provenance)


    def get_nodes(self):
        """
        :return: generator of TAD nodes
        """
        with open(self.filepath, 'r') as tad_file:
            next(tad_file) # skip header
            for row in tad_file:
                row = row.strip().split(',')
                loc_info = row[TADMapAdapter.INDEX['loc_info']].split('|')
                genes_info = row[TADMapAdapter.INDEX['genes']].split(';')
                chr = loc_info[TADMapAdapter.INDEX['chr']]
                start = loc_info[TADMapAdapter.INDEX['start']]
                end = loc_info[TADMapAdapter.INDEX['end']]
                genes = []
                for gene in genes_info:
                    try:
                        gene = gene.split('|')
                        gene = gene[1].split(':')[1]
                        genes.append(gene)
                    except IndexError:
                        continue

                if check_genomic_location(self.chr, self.start, self.end, chr, start, end):
                    _id = f"SO:{build_regulatory_region_id(chr, start, end)}"
                    _props = {}
                    if self.write_properties:
                        _props = {
                            'chr': chr,
                            'start': int(start),
                            'end': int(end),
                            'genes': genes
                        }
                        if self.add_provenance:
                            _props['source'] = self.source
                            _props['source_url'] = self.source_url

                    yield _id, self.label, _props

    def get_edges(self):
        with open(self.filepath, 'r') as tad_file:
            next(tad_file) # skip header
            for row in tad_file:
                row = row.strip().split(',')
                loc_info = row[TADMapAdapter.INDEX['loc_info']].split('|')
                genes_info = row[TADMapAdapter.INDEX['genes']].split(';')
                chr = loc_info[TADMapAdapter.INDEX['chr']]
                start = loc_info[TADMapAdapter.INDEX['start']]
                end = loc_info[TADMapAdapter.INDEX['end']]
                genes = []
                for gene in genes_info:
                    try:
                        gene = gene.split('|')
                        gene = f"{gene[1].split(':')[1].upper()}"  # Add prefix + uppercase
                        genes.append(gene)
                    except IndexError:
                        continue

                if check_genomic_location(self.chr, self.start, self.end, chr, start, end):
                    _id = f"SO:{build_regulatory_region_id(chr, start, end)}"  # Use SO: prefix
                    _props = {}
                    for gene in genes:
                        if self.write_properties and self.add_provenance:
                            _props['source'] = self.source
                            _props['source_url'] = self.source_url
                        yield gene, _id, self.label, _props