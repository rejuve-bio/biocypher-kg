# From reactome download page ("Glossary") at https://download.reactome.org/documentation/DataModelGlossary_V90.pdf
# :
# A PhysicalEntity is a physical substance that can interact with
# other substances. PhysicalEntities include all kinds of small molecules, proteins, nucleic acids,
# chemical compounds, complexes, larger macromolecular assemblies, atoms (including ionized
# atoms), electrons, and photons.

# Physical Entity Identifiers Mapping
# The mapping files consist of a tab-separated table that indicates which external protein, gene or small molecule identifiers in the source database were mapped to Reactome physical entities and reaction annotations. Our goal with distributing three sets of files for each different identifier type is to provide these mapping files link the source database identifier to:

# the lowest level pathway diagram or subset of the pathway
# all level pathway diagrams (filename appended with 'All_Levels’),
# all reaction events.
# The columns within the mappings files follow a similar format:

# Source database identifier, e.g. UniProt, ENSEMBL, NCBI Gene or ChEBI identifier
# Reactome Physical Entity Stable Identifier
# Reactome Physical Entity Name
# Reactome Pathway Stable identifier
# URL
# Event (Pathway or Reaction) Name
# Evidence Code
# Species


#####################################
# In  the Rejuve's BioatomSpace context, an physical entity is only a "linking
# node": proteins, genes, and small molecules (ChEBI) are (also) linked to a
# physical entity.
#####################################