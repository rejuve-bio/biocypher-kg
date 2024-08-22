import csv
import os
import pickle
from biocypher_metta.adapters import Adapter
from biocypher_metta.adapters.helpers import to_float, check_genomic_location
from biocypher._logger import logger
import gzip

# DATE ADDED TO CATALOG	PUBMEDID	FIRST AUTHOR	DATE	JOURNAL	LINK	STUDY	DISEASE/TRAIT	INITIAL SAMPLE SIZE	REPLICATION SAMPLE SIZE	REGION	CHR_ID	CHR_POS	REPORTED GENE(S)	MAPPED_GENE	UPSTREAM_GENE_ID	DOWNSTREAM_GENE_ID	SNP_GENE_IDS	UPSTREAM_GENE_DISTANCE	DOWNSTREAM_GENE_DISTANCE	STRONGEST SNP-RISK ALLELE	SNPS	MERGED	SNP_ID_CURRENT	CONTEXT	INTERGENIC	RISK ALLELE FREQUENCY	P-VALUE	PVALUE_MLOG	P-VALUE (TEXT)	OR or BETA	95% CI (TEXT)	PLATFORM [SNPS PASSING QC]	CNV	MAPPED_TRAIT	MAPPED_TRAIT_URI	STUDY ACCESSION	GENOTYPING TECHNOLOGY
# 8/20/2020	32372009	de Las Fuentes L	5/5/2020	Mol Psychiatry	www.ncbi.nlm.nih.gov/pubmed/32372009	Gene-educational attainment interactions in a multi-ancestry genome-wide meta-analysis identify novel blood pressure loci.	Systolic blood pressure x educational attainment (some college) interaction (2df)	27,617 European ancestry individuals with some college education, 20,253 European ancestry individuals without college education, 8,128 African ancestry individuals with some college education, 8,537 African ancestry individuals without college education, 1,138 Asian ancestry individuals with some college education, 10,214 Asian ancestry individuals without college education, 1,869 Hispanic/Latin American individuals with some college education, 3,276 Hispanic/Latin American individuals without college education	131,584 European ancestry individuals with some college education, 110,940 European ancestry individuals without college education, 1,494 African ancestry individuals with some college education, 5,704 African ancestry individuals without college education, 2,339 Asian ancestry individuals with some college education, 8,567 Asian ancestry individuals without college education, 4,133 Hispanic individuals with some college education, 7,498 Hispanic individuals without college education	1p21.2	1	100364129	CDC14A	CDC14A			ENSG00000079335			rs114558965-A	rs114558965	0	114558965	intron_variant	0	0.97	1.00E-09	9	(African)	4.123	[1.46-6.78] unit increase	Affymetrix, Illumina [~ 18800000] (imputed)	N	systolic blood pressure, self reported educational attainment	http://www.ebi.ac.uk/efo/EFO_0006335, http://www.ebi.ac.uk/efo/EFO_0004784	GCST010426	Genome-wide genotyping array
# 8/20/2020	32372009	de Las Fuentes L	5/5/2020	Mol Psychiatry	www.ncbi.nlm.nih.gov/pubmed/32372009	Gene-educational attainment interactions in a multi-ancestry genome-wide meta-analysis identify novel blood pressure loci.	Systolic blood pressure x educational attainment (some college) interaction (2df)	27,617 European ancestry individuals with some college education, 20,253 European ancestry individuals without college education, 8,128 African ancestry individuals with some college education, 8,537 African ancestry individuals without college education, 1,138 Asian ancestry individuals with some college education, 10,214 Asian ancestry individuals without college education, 1,869 Hispanic/Latin American individuals with some college education, 3,276 Hispanic/Latin American individuals without college education	131,584 European ancestry individuals with some college education, 110,940 European ancestry individuals without college education, 1,494 African ancestry individuals with some college education, 5,704 African ancestry individuals without college education, 2,339 Asian ancestry individuals with some college education, 8,567 Asian ancestry individuals without college education, 4,133 Hispanic individuals with some college education, 7,498 Hispanic individuals without college education	2q37.1	2	234696002	ARL4C	LINC01173			ENSG00000280744			rs145586115-T	rs145586115	0	145586115	intron_variant	0	0.97	2.00E-08	7.698970004	(African)	2.57	[0.042-5.098] unit decrease	Affymetrix, Illumina [~ 18800000] (imputed)	N	systolic blood pressure, self reported educational attainment	http://www.ebi.ac.uk/efo/EFO_0006335, http://www.ebi.ac.uk/efo/EFO_0004784	GCST010426	Genome-wide genotyping array
# 8/20/2020	32372009	de Las Fuentes L	5/5/2020	Mol Psychiatry	www.ncbi.nlm.nih.gov/pubmed/32372009	Gene-educational attainment interactions in a multi-ancestry genome-wide meta-analysis identify novel blood pressure loci.	Systolic blood pressure x educational attainment (some college) interaction (2df)	27,617 European ancestry individuals with some college education, 20,253 European ancestry individuals without college education, 8,128 African ancestry individuals with some college education, 8,537 African ancestry individuals without college education, 1,138 Asian ancestry individuals with some college education, 10,214 Asian ancestry individuals without college education, 1,869 Hispanic/Latin American individuals with some college education, 3,276 Hispanic/Latin American individuals without college education	131,584 European ancestry individuals with some college education, 110,940 European ancestry individuals without college education, 1,494 African ancestry individuals with some college education, 5,704 African ancestry individuals without college education, 2,339 Asian ancestry individuals with some college education, 8,567 Asian ancestry individuals without college education, 4,133 Hispanic individuals with some college education, 7,498 Hispanic individuals without college education	5q14.1	5	80567637	ANKRD34B	ANKRD34B			ENSG00000189127			rs66907226-?	rs66907226	0	66907226	intron_variant	0	0.58	4.00E-08	7.397940009	(EA)	0.04	[-0.3128-0.3928] unit increase	Affymetrix, Illumina [~ 18800000] (imputed)	N	systolic blood pressure, self reported educational attainment	http://www.ebi.ac.uk/efo/EFO_0006335, http://www.ebi.ac.uk/efo/EFO_0004784	GCST010426	Genome-wide genotyping array
# 8/20/2020	32372009	de Las Fuentes L	5/5/2020	Mol Psychiatry	www.ncbi.nlm.nih.gov/pubmed/32372009	Gene-educational attainment interactions in a multi-ancestry genome-wide meta-analysis identify novel blood pressure loci.	Systolic blood pressure x educational attainment (some college) interaction (2df)	27,617 European ancestry individuals with some college education, 20,253 European ancestry individuals without college education, 8,128 African ancestry individuals with some college education, 8,537 African ancestry individuals without college education, 1,138 Asian ancestry individuals with some college education, 10,214 Asian ancestry individuals without college education, 1,869 Hispanic/Latin American individuals with some college education, 3,276 Hispanic/Latin American individuals without college education	131,584 European ancestry individuals with some college education, 110,940 European ancestry individuals without college education, 1,494 African ancestry individuals with some college education, 5,704 African ancestry individuals without college education, 2,339 Asian ancestry individuals with some college education, 8,567 Asian ancestry individuals without college education, 4,133 Hispanic individuals with some college education, 7,498 Hispanic individuals without college education	11q24.2	11	125971183	CDON	CDON			ENSG00000064309			rs12295584-A	rs12295584	0	12295584	intron_variant	0	NR	5.00E-08	7.301029996		1.07	[-0.5764-2.7164] unit decrease	Affymetrix, Illumina [~ 18800000] (imputed)	N	systolic blood pressure, self reported educational attainment	http://www.ebi.ac.uk/efo/EFO_0006335, http://www.ebi.ac.uk/efo/EFO_0004784	GCST010426	Genome-wide genotyping array
# 8/20/2020	32372009	de Las Fuentes L	5/5/2020	Mol Psychiatry	www.ncbi.nlm.nih.gov/pubmed/32372009	Gene-educational attainment interactions in a multi-ancestry genome-wide meta-analysis identify novel blood pressure loci.	Systolic blood pressure x educational attainment (some college) interaction (2df)	27,617 European ancestry individuals with some college education, 20,253 European ancestry individuals without college education, 8,128 African ancestry individuals with some college education, 8,537 African ancestry individuals without college education, 1,138 Asian ancestry individuals with some college education, 10,214 Asian ancestry individuals without college education, 1,869 Hispanic/Latin American individuals with some college education, 3,276 Hispanic/Latin American individuals without college education	131,584 European ancestry individuals with some college education, 110,940 European ancestry individuals without college education, 1,494 African ancestry individuals with some college education, 5,704 African ancestry individuals without college education, 2,339 Asian ancestry individuals with some college education, 8,567 Asian ancestry individuals without college education, 4,133 Hispanic individuals with some college education, 7,498 Hispanic individuals without college education	11q24.2	11	125971183	CDON	CDON			ENSG00000064309			rs12295584-A	rs12295584	0	12295584	intron_variant	0	0.95	1.00E-07	7	(African)	1.512	[-0.40547-3.42947] unit decrease	Affymetrix, Illumina [~ 18800000] (imputed)	N	systolic blood pressure, self reported educational attainment	http://www.ebi.ac.uk/efo/EFO_0006335, http://www.ebi.ac.uk/efo/EFO_0004784	GCST010426	Genome-wide genotyping array


class GWASAdapter(Adapter):

    index = {
        "rsid": 21,
        "snp_in_gene": 17,
        "snp_upstream_gene": 15,
        "snp_downstream_gene": 16,
        "upstream_distance": 18,
        "downstream_distance": 19,
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
                            if self.label == "snp_upstream_gene":
                                _props["distance"] = int(row[self.index["upstream_distance"]])
                            if self.label == "snp_downstream_gene":
                                _props["distance"] = int(row[self.index["downstream_distance"]])
                            if self.add_provenance:
                                _props["source"] = self.source
                                _props["source_url"] = self.source_url

                        yield _source, _target, self.label, _props
                except Exception as e:
                    print(row)
                    print(e)
