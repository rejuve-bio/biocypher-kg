---

Title: "BioCypher graph schema configuration file"
author: saulo
output:
  github_document:
    toc: TRUE
    toc_depth: 2
    preserve_yaml: TRUE
    html_preview: TRUE
    keep_html: TRUE
  
---


- [1.1) position entity:](#11-position-entity)
- [1.2) coding element:](#12-coding-element)
- [1.3) non coding element:](#13-non-coding-element)
- [1.4) genomic variant:](#14-genomic-variant)
- [1.5) epigenomic port:](#15-epigenomic-port)
  - [should also include cell type in hierarchy between epigenetic
    feature and
    tissue](#should-also-include-cell-type-in-hierarchy-between-epigenetic-feature-and-tissue)
- [1.6) 3d genome structure:](#16-3d-genome-structure)
- [1.7) ontology term:](#17-ontology-term)
- [2.1) chromosome chain:](#21-chromosome-chain)
- [coding elements](#coding-elements)
- [Human:](#human)
- [Maybe the “latest_release” could be
  better:](#maybe-the-latest_release-could-be-better)
- [Fly:](#fly)
- [Flybase (FB) gtf:
  <u>https://ftp.flybase.org/genomes/dmel/current/gtf/</u> ← it doesn’t
  list gene/transcript
  types](#flybase-fb-gtf-httpsftpflybaseorggenomesdmelcurrentgtf--it-doesnt-list-genetranscript-types)
- [this table holds fly genes with the transcript types
  data:](#this-table-holds-fly-genes-with-the-transcript-types-data)
- [From Ensembl:](#from-ensembl)
- [Adapter: gencode_gene_adapter](#adapter-gencode_gene_adapter)
- [2.2) gene:](#22-gene)
- [Human:](#human-1)
- [Fly:](#fly-1)
- [Flybase:](#flybase)
- [2.3) protein:](#23-protein)
- [2.4) transcript:](#24-transcript)
- [2.5) exon:](#25-exon)
- [---](#---)
- [genomic variants](#genomic-variants)
- [3.1) snp:](#31-snp)
- [3.2) structural variant:](#32-structural-variant)
- [3.3) sequence variant:](#33-sequence-variant)
- [Human: to be defined…](#human-to-be-defined)
- [Fly:](#fly-2)
- [Human:](#human-2)
- [Fly:](#fly-3)
- [FB’s ncRNA_genes_fb_XXXX_XX.json holds most of data, including genes
  for each
  transcript](#fbs-ncrna_genes_fb_xxxx_xxjson-holds-most-of-data-including-genes-for-each-transcript)
- [4.4) non coding rna:](#44-non-coding-rna)
- [Ontologies](#ontologies)
- [5.1) go:](#51-go)
- [<u>https://bioportal.bioontology.org/ontologies/ECO</u>](#httpsbioportalbioontologyorgontologieseco)
- [<u>https://bioportal.bioontology.org/ontologies/MI</u>](#httpsbioportalbioontologyorgontologiesmi)
- [<u>https://bioportal.bioontology.org/ontologies/SO</u>](#httpsbioportalbioontologyorgontologiesso)
- [6.4)](#64)
- [FB:
  <u>https://wiki.flybase.org/wiki/FlyBase:ModENCODE_data_at_FlyBase</u>
  ← I need to check
  this](#fb-httpswikiflybaseorgwikiflybasemodencode_data_at_flybase--i-need-to-check-this)
- [Where is the human data?](#where-is-the-human-data)
- [—-------------------------------------------------------------------------------------------------------------------------------------](#-------------------------------------------------------------------------------------------------------------------------------------)
- [Associations](#associations)
- [—-------------------------------------------------------------------------------------------------------------------------------------](#--------------------------------------------------------------------------------------------------------------------------------------1)
- [7.1)](#71)
- [7.2)](#72)
- [7.3)](#73)
- [7.4) expression](#74-expression)
- [7.5) transcribed from:](#75-transcribed-from)
- [7.6) translates to:](#76-translates-to)
- [7.7) translation of:](#77-translation-of)
- [FB:
  <u>https://wiki.flybase.org/wiki/FlyBase:Downloads_Overview#FBgn\_.3C.3D.3E_FBtr\_.3C.3D.3E_FBpp_IDs\_.28expanded.29\_.28fbgn_fbtr_fbpp_expanded\_.2A.tsv.29</u>](#fb-httpswikiflybaseorgwikiflybasedownloads_overviewfbgn_3c3d3e_fbtr_3c3d3e_fbpp_ids_28expanded29_28fbgn_fbtr_fbpp_expanded_2atsv29)
- [7.9) post translational
  interaction:](#79-post-translational-interaction)
- [7.23) transcription factor to gene
  association:](#723-transcription-factor-to-gene-association)
- [Dmel to hsa data:](#dmel-to-hsa-data)
- [Human:](#human-3)
- [](#section)
- [Fly::](#fly-4)
- [](#section-1)
- [From FB:
  <u>https://wiki.flybase.org/wiki/FlyBase:Downloads_Overview#Drosophila_Paralogs\_.28dmel_paralogs_fb\_.2A.tsv.gz.29</u>](#from-fb-httpswikiflybaseorgwikiflybasedownloads_overviewdrosophila_paralogs_28dmel_paralogs_fb_2atsvgz29)
- [non-coding elements](#non-coding-elements)
- [4.1) enhancer:](#41-enhancer)
- [4.2) promoter:](#42-promoter)
- [4.3) super enhancer:](#43-super-enhancer)
- [4.5) pathway:](#45-pathway)
- [4.6) regulatory region:](#46-regulatory-region)
- [5.2) motif:](#52-motif)
- [3D genome structures](#3d-genome-structures)
- [6.1) tad:](#61-tad)
- [6.2) chromatin state:](#62-chromatin-state)
- [6.3) dnase hypersensitivity site:](#63-dnase-hypersensitivity-site)
- [Associations](#associations-1)
- [7.8) gene to gene coexpression
  association:](#78-gene-to-gene-coexpression-association)
- [annotation](#annotation)
- [7.10) gene to pathway association:](#710-gene-to-pathway-association)
- [7.11) parent pathway of:](#711-parent-pathway-of)
- [7.12) child pathway of:](#712-child-pathway-of)
- [7.13) go subtype of:](#713-go-subtype-of)
- [7.14) go gene product:](#714-go-gene-product)
- [7.15) go gene:](#715-go-gene)
- [7.16) go rna:](#716-go-rna)
- [7.17) ontology has part:](#717-ontology-has-part)
- [7.18) ontology part of:](#718-ontology-part-of)
- [7.19) ontology subclass of:](#719-ontology-subclass-of)
- [regulatory association](#regulatory-association)
- [7.20) enhancer to gene
  association:](#720-enhancer-to-gene-association)
- [7.21) promoter to gene
  association:](#721-promoter-to-gene-association)
- [7.22) super enhancer to gene
  association:](#722-super-enhancer-to-gene-association)
- [7.24) regulatory region to gene
  association:](#724-regulatory-region-to-gene-association)
- [7.25) gtex variant to gene expression
  association:](#725-gtex-variant-to-gene-expression-association)
- [7.26) closest gene to variant
  association:](#726-closest-gene-to-variant-association)
- [7.27) upstream gene to variant
  association:](#727-upstream-gene-to-variant-association)
- [7.28) downstream gene to variant
  association:](#728-downstream-gene-to-variant-association)
- [7.29) in gene to variant
  association:](#729-in-gene-to-variant-association)
- [7.30) top ld in linkage disequilibrium
  with:](#730-top-ld-in-linkage-disequilibrium-with)
- [7.31) lower resolution structure:](#731-lower-resolution-structure)
- [7.32) located on chain:](#732-located-on-chain)
- [](#section-2)
- [FB data:](#fb-data)
- [<u>https://wiki.flybase.org/wiki/FlyBase:Downloads_Overview#Phenotypic_data\_.28genotype_phenotype_data\_.2A.tsv.29</u>](#httpswikiflybaseorgwikiflybasedownloads_overviewphenotypic_data_28genotype_phenotype_data_2atsv29)
- [Associations](#associations-2)
- [<u>https://wiki.flybase.org/wiki/FlyBase:Downloads_Overview#Genetic_interactions\_.28allele_genetic_interactions\_.2A.tsv.29</u>](#httpswikiflybaseorgwikiflybasedownloads_overviewgenetic_interactions_28allele_genetic_interactions_2atsv29)
- [Ontologies](#ontologies-1)
- [<u>https://bioportal.bioontology.org/ontologies/FB-DV</u>](#httpsbioportalbioontologyorgontologiesfb-dv)
- [<u>https://bioportal.bioontology.org/ontologies/FB-CV</u>](#httpsbioportalbioontologyorgontologiesfb-cv)
- [<u>https://www.enzyme-database.org/downloads.php</u>](#httpswwwenzyme-databaseorgdownloadsphp)
  - [](#section-3)

------------------------------------------------------------------------

Title: “BioCypher graph schema configuration file” author: saulo output:
github_document: toc: TRUE toc_depth: 2 preserve_yaml: TRUE
html_preview: TRUE keep_html: TRUE

------------------------------------------------------------------------

################################## hsa_dmel

------------------------------------------------------------------------

Named Things

------------------------------------------------------------------------

\#parent types

# 1.1) position entity:

represented_as: node

is_a: biological entity

input_label: position_entity

description: A biological entity that is defined by its position in the
genome

properties:

chr: str

start: int

end: int

# 1.2) coding element:

represented_as: node

is_a: position entity

inherit_properties: true

input_label: coding_element

description: A region of a gene that codes for a protein or peptide

properties:

source: str

source_url: str

# 1.3) non coding element:

represented_as: node

is_a: position entity

inherit_properties: true

input_label: non_coding_element

description: A region of a gene that does not code for a protein or
peptide

properties:

biological_context: str

source: str

source_url: str

# 1.4) genomic variant:

represented_as: node

is_a: position entity

inherit_properties: true

input_label: genomic_variant

description: A genomic variant is a change in one or more sequence of a
genome

properties:

source: str

source_url: str

# 1.5) epigenomic port:

represented_as: node

is_a: position entity

inherit_properties: true

input_label: epigenomic_port

description: A region of the genome that is associated with epigenetic
modifications

properties:

biological_context: str

source: str

source_url: str

## should also include cell type in hierarchy between epigenetic feature and tissue

# 1.6) 3d genome structure:

represented_as: node

is_a: position entity

input_label: 3d_genome_structure

description: A region of the genome that is associated with 3D genome
structure

properties:

source: str

source_url: str

# 1.7) ontology term:

is_a: ontology class

represented_as: node

input_label: ontology term

properties:

source: str

source_url: str

\#child types

# 2.1) chromosome chain:

represented_as: node

is_a: position entity

inherit_properties: true

input_label: chromosome_chain

properties:

chain_id: str

next_start: int

resolution: int

# coding elements

# Human:

[<u>https://ftp.ebi.ac.uk/pub/databases/gencode/Gencode_human/release_46/gencode.v46.annotation.gtf.gz</u>](https://ftp.ebi.ac.uk/pub/databases/gencode/Gencode_human/release_46/gencode.v46.annotation.gtf.gz)

# Maybe the “latest_release” could be better:

[<u>https://ftp.ebi.ac.uk/pub/databases/gencode/Gencode_human/latest_release/gencode.v46.annotation.gtf.gz</u>](https://ftp.ebi.ac.uk/pub/databases/gencode/Gencode_human/latest_release/gencode.v46.annotation.gtf.gz)

# Fly:

# Flybase (FB) gtf: [<u>https://ftp.flybase.org/genomes/dmel/current/gtf/</u>](https://ftp.flybase.org/genomes/dmel/current/gtf/) ← it doesn’t list gene/transcript types

# this table holds fly genes with the transcript types data:

[<u>https://wiki.flybase.org/wiki/FlyBase:Downloads_Overview#FBgn\_.3C.3D.3E_FBtr\_.3C.3D.3E_FBpp_IDs\_.28expanded.29\_.28fbgn_fbtr_fbpp_expanded\_.2A.tsv.29</u>](https://wiki.flybase.org/wiki/FlyBase:Downloads_Overview#FBgn_.3C.3D.3E_FBtr_.3C.3D.3E_FBpp_IDs_.28expanded.29_.28fbgn_fbtr_fbpp_expanded_.2A.tsv.29)

# From Ensembl:

[<u>https://ftp.ensembl.org/pub/current_gtf/drosophila_melanogaster/Drosophila_melanogaster.BDGP6.46.112.gtf.gz</u>](https://ftp.ensembl.org/pub/current_gtf/drosophila_melanogaster/Drosophila_melanogaster.BDGP6.46.112.gtf.gz)

[<u>https://ftp.ensembl.org/pub/current_gtf/drosophila_melanogaster/README</u>](https://ftp.ensembl.org/pub/current_gtf/drosophila_melanogaster/README)

or:
[<u>https://ftp.ebi.ac.uk/ensemblgenomes/pub/metazoa/current/gtf/drosophila_melanogaster/</u>](https://ftp.ebi.ac.uk/ensemblgenomes/pub/metazoa/current/gtf/drosophila_melanogaster/)

# Adapter: gencode_gene_adapter

# 2.2) gene:

represented_as: node

preferred_id: ensemble \# ensembl ids are FB FBgn# ids

input_label: gene

is_a: coding element

inherit_properties: true

properties:

gene_name: str \# In FB this is the gene symbol (eg “Clk”) whereas the
gene name is “Clock”.

gene_type: str

synonyms: str\[\]

# Human:

[<u>https://ftp.uniprot.org/pub/databases/uniprot/current_release/knowledgebase/taxonomic_divisions/uniprot_sprot_human.dat.gz</u>](https://ftp.uniprot.org/pub/databases/uniprot/current_release/knowledgebase/taxonomic_divisions/uniprot_sprot_human.dat.gz)

[<u>https://ftp.uniprot.org/pub/databases/uniprot/current_release/knowledgebase/taxonomic_divisions/uniprot_trembl_human.dat.gz</u>](https://ftp.uniprot.org/pub/databases/uniprot/current_release/knowledgebase/taxonomic_divisions/uniprot_trembl_human.dat.gz)

# Fly:

[<u>https://ftp.uniprot.org/pub/databases/uniprot/current_release/knowledgebase/taxonomic_divisions/uniprot_sprot_invertebrates.dat.gz</u>](https://ftp.uniprot.org/pub/databases/uniprot/current_release/knowledgebase/taxonomic_divisions/uniprot_sprot_invertebrates.dat.gz)

FB gene symbols are suffixed with "\_DROME"

# Flybase:

[<u>https://wiki.flybase.org/wiki/FlyBase:Downloads_Overview#FBgn\_.3C.3D.3E_DB_Accession_IDs\_.28fbgn_NAseq_Uniprot\_.2A.tsv.29</u>](https://wiki.flybase.org/wiki/FlyBase:Downloads_Overview#FBgn_.3C.3D.3E_DB_Accession_IDs_.28fbgn_NAseq_Uniprot_.2A.tsv.29)

[<u>https://ftp.flybase.org/releases/current/precomputed_files/collaborators/</u>](https://ftp.flybase.org/releases/current/precomputed_files/collaborators/)

# 2.3) protein:

represented_as: node

preferred_id: uniprot

input_label: protein

is_a: coding element \# it should not be a “coding element” neither a
“non coding element” considering their descriptions here

properties:

accessions: str

protein_name: str

synonyms: str

source: str

source_url: str

# 2.4) transcript:

represented_as: node

input_label: transcript

is_a: coding element

inherit_properties: true

properties:

gene_name: str

transcript_name: str

transcript_id: str

transcript_type: str

description: An RNA synthesized on a DNA or RNA template by an RNA
polymerase.

exact_mappings:

\- SO:0000673

\- SIO:010450

\- WIKIDATA:Q7243183

\- dcid:RNATranscript

in_subset:

\- model_organism_database

# 2.5) exon:

represented_as: node

preferred_id: ensemble

input_label: exon

is_a: coding element

inherit_properties: true

properties:

gene_id: str

transcript_id: str

exon_number: int

exon_id: str

# ---

# genomic variants

# 3.1) snp:

represented_as: node

input_label: snp

is_a: genomic variant

inherit_properties: true

properties:

ref: str

alt: str

caf_ref: str

caf_alt: str

description: A single nucleotide polymorphism (SNP) is a variation in a
single nucleotide that occurs at a specific position in the genome

# 3.2) structural variant:

represented_as: node

input_label: structural_variant

is_a: genomic variant

inherit_properties: true

properties:

variant_type: str

evidence: str

# 3.3) sequence variant:

represented_as: node

input_label: sequence_variant

is_a: genomic variant

inherit_properties: true

description: A change in the nucleotide sequence of a genome compared to
a reference sequence.

properties:

rsid: str

ref: str

alt: str

raw_cadd_score: float

phred_score: float

# Human: to be defined…

# Fly:

FB
[<u>https://wiki.flybase.org/wiki/FlyBase:Downloads_Overview#Alleles\_.3C.3D.3E_Genes\_.28fbal_to_fbgn_fb\_.2A.tsv.29</u>](https://wiki.flybase.org/wiki/FlyBase:Downloads_Overview#Alleles_.3C.3D.3E_Genes_.28fbal_to_fbgn_fb_.2A.tsv.29)

allele:

represented_as: node

input_label: allele

is_a: genomic variant

properties:

gene_id: str

description: Different versions of the same variant (in a specific
locus) are called alleles$$1, 2$$. Most commonly used referring to
genes.

# Human:

The same source for ’gene” schema

# Fly:

The same source for ’gene” schema

# FB’s ncRNA_genes_fb_XXXX_XX.json holds most of data, including genes for each transcript

[<u>https://wiki.flybase.org/wiki/FlyBase:Downloads_Overview#Non-coding_RNAs\_.28JSON.29\_.28ncRNA_genes_fb\_.2A.json.gz.29</u>](https://wiki.flybase.org/wiki/FlyBase:Downloads_Overview#Non-coding_RNAs_.28JSON.29_.28ncRNA_genes_fb_.2A.json.gz.29)

# 4.4) non coding rna:

represented_as: node

input_label: non_coding_rna

is_a: non coding element

inherit_properties: true

properties:

rna_type: str

gene_id: str \# to link things together

------------------------------------------------------------------------

# Ontologies

------------------------------------------------------------------------

[<u>http://purl.obolibrary.org/obo/go.owl</u>](http://purl.obolibrary.org/obo/go.owl)

# 5.1) go:

is_a: ontology term

represented_as: node

input_label: go

properties:

term_name: str

description: str \# definition

synonyms: str

subontology: str \# GO “namespaces” (BP, MF, CC)

[<u>https://bioportal.bioontology.org/ontologies/DOID</u>](https://bioportal.bioontology.org/ontologies/DOID)

disease ontology:

is_a: ontology term

represented_as: node

input_label: disease_ontology

properties:

term_name: str

description: str \# definition

synonyms: str\[\]

subsetdef: str\[\]

xref: str \# like dbxref: ids for other databases

# [<u>https://bioportal.bioontology.org/ontologies/ECO</u>](https://bioportal.bioontology.org/ontologies/ECO)

Evidence & Conclusion Ontology

# [<u>https://bioportal.bioontology.org/ontologies/MI</u>](https://bioportal.bioontology.org/ontologies/MI)

PSI-MI Molecular Interaction Ontology

# [<u>https://bioportal.bioontology.org/ontologies/SO</u>](https://bioportal.bioontology.org/ontologies/SO)

The Sequence Ontology

# 6.4)

# FB: [<u>https://wiki.flybase.org/wiki/FlyBase:ModENCODE_data_at_FlyBase</u>](https://wiki.flybase.org/wiki/FlyBase:ModENCODE_data_at_FlyBase) ← I need to check this

# Where is the human data?

histone modification:

represented_as: node

is_a: epigenomic port

inherit_properties: true

input_label: histone_modification

description: \>

A post-translational modification of histone proteins e.g methylation,
acetylation, phosphorylation

properties:

modification: str

modification_type: str

# —-------------------------------------------------------------------------------------------------------------------------------------

# Associations

# —-------------------------------------------------------------------------------------------------------------------------------------

\#parent types

# 7.1)

expression:

is_a: related to at instance level

represented_as: edge

input_label: expression

description: \>-

An association between a gene and its expression

properties:

source: str

source_url: str

# 7.2)

annotation:

is_a: related to at concept level

represented_as: edge

input_label: annotation

description: \>-

An association between a gene/ontology term and another entity

properties:

source: str

source_url: str

# 7.3)

regulatory association:

is_a: related to at instance level

represented_as: edge

input_label: regulatory_association

Source: gene

properties:

source: str

source_url: str

------------------------------------------------------------------------

# 7.4) expression

FB:
[<u>https://wiki.flybase.org/wiki/FlyBase:Downloads_Overview#FBgn\_.3C.3D.3E_FBtr\_.3C.3D.3E_FBpp_IDs\_.28expanded.29\_.28fbgn_fbtr_fbpp_expanded\_.2A.tsv.29</u>](https://wiki.flybase.org/wiki/FlyBase:Downloads_Overview#FBgn_.3C.3D.3E_FBtr_.3C.3D.3E_FBpp_IDs_.28expanded.29_.28fbgn_fbtr_fbpp_expanded_.2A.tsv.29)

transcribed to:

represented_as: edge

is_a: expression

inherit_properties: true

input_label: transcribed_to

source: gene

target: transcript

description: \>-

inverse of transcribed from

exact_mappings:

\- RO:0002511

\- SIO:010080

# 7.5) transcribed from:

FB:
[<u>https://wiki.flybase.org/wiki/FlyBase:Downloads_Overview#FBgn\_.3C.3D.3E_FBtr\_.3C.3D.3E_FBpp_IDs\_.28expanded.29\_.28fbgn_fbtr_fbpp_expanded\_.2A.tsv.29</u>](https://wiki.flybase.org/wiki/FlyBase:Downloads_Overview#FBgn_.3C.3D.3E_FBtr_.3C.3D.3E_FBpp_IDs_.28expanded.29_.28fbgn_fbtr_fbpp_expanded_.2A.tsv.29)

is_a: expression

inherit_properties: true

represented_as: edge

input_label: transcribed_from

source: transcript

target: gene

description: \>-

x is transcribed from y if and only if x is synthesized from template y

exact_mappings:

\- RO:0002510

\- SIO:010081

# 7.6) translates to:

FB:
[<u>https://wiki.flybase.org/wiki/FlyBase:Downloads_Overview#FBgn\_.3C.3D.3E_FBtr\_.3C.3D.3E_FBpp_IDs\_.28expanded.29\_.28fbgn_fbtr_fbpp_expanded\_.2A.tsv.29</u>](https://wiki.flybase.org/wiki/FlyBase:Downloads_Overview#FBgn_.3C.3D.3E_FBtr_.3C.3D.3E_FBpp_IDs_.28expanded.29_.28fbgn_fbtr_fbpp_expanded_.2A.tsv.29)

is_a: expression

inherit_properties: true

represented_as: edge

input_label: translates_to

source: transcript

target: protein

inverse: translation of

description: \>-

x (amino acid chain/polypeptide) is the ribosomal translation of y
(transcript) if and only if a ribosome

reads y (transcript) through a series of triplet codon-amino acid
adaptor activities (<GO:0030533>)

and produces x (amino acid chain/polypeptide)

close_mappings:

\- RO:0002513

\- SIO:010082

# 7.7) translation of:

# FB: [<u>https://wiki.flybase.org/wiki/FlyBase:Downloads_Overview#FBgn\_.3C.3D.3E_FBtr\_.3C.3D.3E_FBpp_IDs\_.28expanded.29\_.28fbgn_fbtr_fbpp_expanded\_.2A.tsv.29</u>](https://wiki.flybase.org/wiki/FlyBase:Downloads_Overview#FBgn_.3C.3D.3E_FBtr_.3C.3D.3E_FBpp_IDs_.28expanded.29_.28fbgn_fbtr_fbpp_expanded_.2A.tsv.29)

is_a: expression

inherit_properties: true

represented_as: edge

input_label: translation_of

source: protein

target: transcript

description: \>-

inverse of translates to

inverse: translates to

close_mappings:

\- RO:0002512

\- SIO:010083

# 7.9) post translational interaction:

These files list protein1_id protein2_id combined_score

dmel:
[<u>https://stringdb-downloads.org/download/protein.links.v12.0/7227.protein.links.v12.0.txt.gz</u>](https://stringdb-downloads.org/download/protein.links.v12.0/7227.protein.links.v12.0.txt.gz)

hsa:
[<u>https://stringdb-downloads.org/download/protein.links.v12.0/9606.protein.links.v12.0.txt.gz</u>](https://stringdb-downloads.org/download/protein.links.v12.0/9606.protein.links.v12.0.txt.gz)

is_a: expression

inherit_properties: true

represented_as: edge

input_label: interacts_with

source: protein

target: protein

properties:

score: float

# 7.23) transcription factor to gene association:

<span class="mark">This holds data from TFLink to fly:
[<u>https://cdn.netbiol.org/tflink/download_files/TFLink_Drosophila_melanogaster_interactions_All_simpleFormat_v1.0.tsv</u>](https://cdn.netbiol.org/tflink/download_files/TFLink_Drosophila_melanogaster_interactions_All_simpleFormat_v1.0.tsv)</span>

description: A regulatory association between a transcription factor and
its target gene

is_a: regulatory association

inherit_properties: true

represented_as: edge

input_label: tf_gene

output_label: regulates

source: gene

target: gene

properties:

evidence: str

detection_method: str

databases: str

evidence_type: str

# Dmel to hsa data:

From FB:
[<u>https://wiki.flybase.org/wiki/FlyBase:Downloads_Overview#Human_Orthologs\_.28dmel_human_orthologs_disease_fb\_.2A.tsv.gz.29</u>](https://wiki.flybase.org/wiki/FlyBase:Downloads_Overview#Human_Orthologs_.28dmel_human_orthologs_disease_fb_.2A.tsv.gz.29)

orthology association:

description: Non-directional association between two genes indicating
that there is an orthology relation among them: “Historical homology
that involves genes that diverged after a speciation event”:
[<u>http://purl.obolibrary.org/obo/RO_HOM0000017</u>](http://purl.obolibrary.org/obo/RO_HOM0000017)

is_a: related to at instance level

inherit_properties: true

represented_as: edge

input_label: orthologs_genes

source: gene

target: gene

properties:

source_organism: str

target_organism: str

# Human:

# 

# Fly::

# 

# From FB: [<u>https://wiki.flybase.org/wiki/FlyBase:Downloads_Overview#Drosophila_Paralogs\_.28dmel_paralogs_fb\_.2A.tsv.gz.29</u>](https://wiki.flybase.org/wiki/FlyBase:Downloads_Overview#Drosophila_Paralogs_.28dmel_paralogs_fb_.2A.tsv.gz.29)

paralogy association:

description: Non-directional association between two genes indicating
that there is an paralogy relation among them: “Historical homology that
involves genes that diverged after a duplication event”
[<u>http://purl.obolibrary.org/obo/RO_HOM0000011</u>](http://purl.obolibrary.org/obo/RO_HOM0000011)

is_a: related to at instance level

inherit_properties: true

represented_as: edge

input_label: paralogs_genes

source: gene

target: gene

properties:

organism: str

#################################### hsa

------------------------------------------------------------------------

# non-coding elements

# 4.1) enhancer:

represented_as: node

input_label: enhancer

is_a: non coding element

inherit_properties: true

properties:

data_source: str

# 4.2) promoter:

represented_as: node

input_label: promoter

is_a: non coding element

inherit_properties: true

# 4.3) super enhancer:

represented_as: node

input_label: super_enhancer

is_a: non coding element

inherit_properties: true

# 4.5) pathway:

is_a: biological process

exact_mappings:

\- PW:0000001

\- WIKIDATA:Q4915012

narrow_mappings:

\- SIO:010526

\- <GO:0007165>

represented_as: node

input_label: pathway

properties:

pathway_name: str

evidence: str

# 4.6) regulatory region:

represented_as: node

input_label: regulatory_region

is_a: non coding element

inherit_properties: true

description: A region of the genome that is involved in gene regulation.

properties:

cell: str

biochemical_activity: str

biological_context: str

# 5.2) motif:

represented_as: node

is_a: epigenomic port

input_label: motif

accessible_via:

name: motifs

description: TF binding motifs

fuzzy_text_search: tf_name

return: \_id, tf_name, source, source_url, pwm, length

properties:

tf_name: str

pwm_A: float\[\]

pwm_C: float\[\]

pwm_G: float\[\]

pwm_T: float\[\]

length: str

------------------------------------------------------------------------

# 3D genome structures

# 6.1) tad:

represented_as: node

input_label: tad

is_a: 3d genome structure

inherit_properties: true

properties:

genes: str

# 6.2) chromatin state:

represented_as: node

is_a: epigenomic port

inherit_properties: true

input_label: chromatin_state

properties:

state: str

# 6.3) dnase hypersensitivity site:

represented_as: node

is_a: epigenomic port

inherit_properties: true

input_label: dnase_hypersensitivity_site

description: A region of chromatin that is sensitive to cleavage by the
enzyme DNase I

------------------------------------------------------------------------

# Associations

------------------------------------------------------------------------

# 7.8) gene to gene coexpression association:

description: Indicates that two genes are co-expressed, generally under
the same conditions.

is_a: expression

inherit_properties: true

represented_as: edge

input_label: coexpressed_with

source: gene

target: gene

properties:

score: float

–

# annotation

# 7.10) gene to pathway association:

description: \>-

An interaction between a gene or gene product and a biological process
or pathway.

is_a: annotation

inherit_properties: true

represented_as: edge

input_label: genes_pathways

source: gene

target: pathway

# 7.11) parent pathway of:

is_a: annotation

inherit_properties: true

description: \>-

holds between two pathways where the domain class is a parent pathway of
the range class

represented_as: edge

input_label: parent_pathway_of

source: pathway

target: pathway

# 7.12) child pathway of:

is_a: annotation

inherit_properties: true

description: \>-

holds between two pathways where the domain class is a child pathway of
the range class

represented_as: edge

input_label: child_pathway_of

source: pathway

target: pathway

# 7.13) go subtype of:

is_a: annotation

inherit_properties: true

represented_as: edge

input_label: subtype_of

source: go

target: go

# 7.14) go gene product:

is_a: annotation

inherit_properties: true

represented_as: edge

input_label: go_gene_product

source: gene ontology

target: protein

properties:

qualifier: obj

db_reference: obj

evidence: str

# 7.15) go gene:

is_a: annotation

inherit_properties: true

represented_as: edge

input_label: go_gene

output_label: belongs_to

source: gene

target: go

properties:

qualifier: obj

db_reference: obj

evidence: str

# 7.16) go rna:

is_a: annotation

inherit_properties: true

represented_as: edge

input_label: go_rna

output_label: belongs_to

source: non coding rna

target: go

# 7.17) ontology has part:

is_a: annotation

inherit_properties: true

represented_as: edge

input_label: ontology_has_part

output_label: has_part

source: ontology term

target: ontology term

# 7.18) ontology part of:

is_a: annotation

inherit_properties: true

represented_as: edge

input_label: ontology_part_of

output_label: part_of

source: ontology term

target: ontology term

# 7.19) ontology subclass of:

is_a: annotation

inherit_properties: true

represented_as: edge

input_label: ontology_subclass_of

output_label: subclass_of

source: ontology term

target: ontology term

properties:

rel_type: str

------------------------------------------------------------------------

# regulatory association

# 7.20) enhancer to gene association:

description: An association between an enhancer and a gene

is_a: regulatory association

inherit_properties: true

represented_as: edge

input_label: enhancer_gene

output_label: regulates

source: enhancer

target: gene

properties:

score: float

biological_context: str

# 7.21) promoter to gene association:

description: An association between a promoter and a gene

is_a: regulatory association

inherit_properties: true

represented_as: edge

input_label: promoter_gene

output_label: regulates

source: promoter

target: gene

properties:

score: float

biological_context: str

# 7.22) super enhancer to gene association:

description: An association between a super enhancer and a gene

is_a: regulatory association

inherit_properties: true

represented_as: edge

input_label: super_enhancer_gene

output_label: regulates

source: super enhancer

target: gene

properties:

score: float

biological_context: str

# 7.24) regulatory region to gene association:

description: An association between a regulatory region and a gene it
regulates

is_a: regulatory association

inherit_properties: true

represented_as: edge

input_label: regulatory_region_gene

output_label: regulates

source: regulatory_region

target: gene

properties:

abc_score: float

biological_context: str

# 7.25) gtex variant to gene expression association:

aliases: eQTL, e-QTL

description: An association between a variant and expression of a gene
(i.e. e-QTL)

is_a: related to at instance level

represented_as: edge

input_label: gtex_variant_gene

output_label: correlates_with

source: snp

target: gene

properties:

slope: float

maf: float

p_value: float

q_value: float

biological_context: str

source: str

source_url: str

# 7.26) closest gene to variant association:

is_a: related to at instance level

description: holds between a sequence variant and a gene that is closest
to the variant

represented_as: edge

input_label: closest_gene

source: snp

target: gene

properties:

chr: str

pos: int

distance: int

source: str

source_url: str

# 7.27) upstream gene to variant association:

is_a: closest gene to variant association

inherit_properties: true

description: holds between a sequence variant and a gene that is
upstream to the variant

represented_as: edge

input_label: upstream_gene

source: snp

target: gene

# 7.28) downstream gene to variant association:

is_a: closest gene to variant association

inherit_properties: true

description: holds between a sequence variant and a gene that is
downstream to the variant

represented_as: edge

input_label: downstream_gene

source: snp

target: gene

# 7.29) in gene to variant association:

is_a: closest gene to variant association

description: holds between a sequence variant and a gene that is within
the gene body

represented_as: edge

input_label: in_gene

source: snp

target: gene

# 7.30) top ld in linkage disequilibrium with:

is_a: related to at instance level

description: holds between two sequence variants, the presence of which
are correlated in a population

represented_as: edge

input_label: in_ld_with

source: snp

target: snp

properties:

ancestry: str

r2: float

d_prime: float

source: str

source_url: str

# 7.31) lower resolution structure:

is_a: related to at instance level

description: holds between two chromosome chains where one is a lower
resolution version of the other

represented_as: edge

input_label: lower_resolution

source: chromosome_chain

target: chromosome_chain

# 7.32) located on chain:

is_a: related to at instance level

description: holds between a position entity and a chromosome chain

represented_as: edge

input_label: located_on_chain

source: position_entity

target: chromosome_chain

#################################### dmel

[<u>https://wiki.flybase.org/wiki/FlyBase:Downloads_Overview#Gene_group_data\_.28gene_group_data_fb\_.2A.tsv.29</u>](https://wiki.flybase.org/wiki/FlyBase:Downloads_Overview#Gene_group_data_.28gene_group_data_fb_.2A.tsv.29)

# 

gene group:

represented_as: node

preferred_id: group_id

input_label: disease_model

is_a: biological entity

inherit_properties: true

properties:

genes: gene\[\]

group_id: str \# FBgg#

group_symbol: str

group_name: str

parent_group_id: str \# FBgg#

parent_group_symbol: str

HGNC_family_ID: str
[<u>https://wiki.flybase.org/wiki/FlyBase:Downloads_Overview#Gene_groups_with_HGNC_IDs\_.28gene_groups_HGNC_fb\_.2A.tsv.29</u>](https://wiki.flybase.org/wiki/FlyBase:Downloads_Overview#Gene_groups_with_HGNC_IDs_.28gene_groups_HGNC_fb_.2A.tsv.29)

# FB data:

[<u>https://wiki.flybase.org/wiki/FlyBase:Downloads_Overview#Human_disease_model_data\_.28disease_model_annotations_fb\_.2A.tsv.gz.29</u>](https://wiki.flybase.org/wiki/FlyBase:Downloads_Overview#Human_disease_model_data_.28disease_model_annotations_fb_.2A.tsv.gz.29)

disease model:

represented_as: node

preferred_id: do_id

input_label: disease_model

is_a: biological entity

inherit_properties: true

properties:

gene_id: str

do_qualifier: str

do_term_id: str \# only this should be enough

do_term_name: str \# this is in the DO

allele_id: str

ortholog_hgnc_id: str

ortholog_hgnc_symbol: str

evidence_code: str

reference_FBrf_id: str

[<u>https://wiki.flybase.org/wiki/FlyBase:Downloads_Overview#Phenotypic_data\_.28genotype_phenotype_data\_.2A.tsv.29</u>](https://wiki.flybase.org/wiki/FlyBase:Downloads_Overview#Phenotypic_data_.28genotype_phenotype_data_.2A.tsv.29)

genotype:

represented_as: node

input_label: genotype

is_a:

inherit_properties:

properties:

id: str \# string composed of one or more allele id(s) (FBal#)

symbol: str \# string composed of one or more allele symbol(s)

# [<u>https://wiki.flybase.org/wiki/FlyBase:Downloads_Overview#Phenotypic_data\_.28genotype_phenotype_data\_.2A.tsv.29</u>](https://wiki.flybase.org/wiki/FlyBase:Downloads_Overview#Phenotypic_data_.28genotype_phenotype_data_.2A.tsv.29)

phenotype:

represented_as: node

input_label: phenotype

is_a:

inherit_properties:

properties:

id: str \# The Flybase Anatomy id (FBbt#) or Flybase Controlled
Vocabulary id (FBcv#)

name: str

qualifier_ids: str\[\] \# zero or more FBcv# and/or FBdv# to add
information to the phenotype

qualifier_names: str\[\]

reference: str \# FBrf#

------------------------------------------------------------------------

# Associations

------------------------------------------------------------------------

[<u>https://wiki.flybase.org/wiki/FlyBase:Downloads_Overview#Genetic_interaction_table\_.28gene_genetic_interactions\_.2A.tsv.29</u>](https://wiki.flybase.org/wiki/FlyBase:Downloads_Overview#Genetic_interaction_table_.28gene_genetic_interactions_.2A.tsv.29)

gene genetic association:

description: An association between a gene and another gene, i.e., a
<span class="mark">gene-level genetic interactions in FlyBase. This data
is computed from the allele-level genetic interaction data captured by
FlyBase curators.</span>

is_a: regulatory association

inherit_properties: true

represented_as: edge

input_label: gene_genetic

source: gene

target: gene

properties:

type: str \# suppresses (“suppressible”) / enhances (“enhanceable”)

# [<u>https://wiki.flybase.org/wiki/FlyBase:Downloads_Overview#Genetic_interactions\_.28allele_genetic_interactions\_.2A.tsv.29</u>](https://wiki.flybase.org/wiki/FlyBase:Downloads_Overview#Genetic_interactions_.28allele_genetic_interactions_.2A.tsv.29)

allelic interaction annotation:

Description: \>- <span class="mark">An allelic interaction is a
controlled vocabulary (i.e. not free text) genetic interaction data
associated with alleles.</span>

<span class="mark">Is_a: annotation</span>

represented_as: edge

input_label: allelic_interaction

source: allele

target: annotation \# FB Controlled Vocabulary (FBcv) ontology

properties:

source: str

source_url: str

------------------------------------------------------------------------

# Ontologies

------------------------------------------------------------------------

[<u>https://bioportal.bioontology.org/ontologies/FB-BT</u>](https://bioportal.bioontology.org/ontologies/FB-BT)

The Flybase Anatomy

# [<u>https://bioportal.bioontology.org/ontologies/FB-DV</u>](https://bioportal.bioontology.org/ontologies/FB-DV)

The Flybase Development

# [<u>https://bioportal.bioontology.org/ontologies/FB-CV</u>](https://bioportal.bioontology.org/ontologies/FB-CV)

The Flybase Controlled Vocabulary

# [<u>https://www.enzyme-database.org/downloads.php</u>](https://www.enzyme-database.org/downloads.php)

ExplorEnz - The Enzyme Database

## 
