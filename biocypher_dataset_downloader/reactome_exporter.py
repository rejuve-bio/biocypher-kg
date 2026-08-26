import csv
import logging
from pathlib import Path
from neo4j import GraphDatabase

logger = logging.getLogger(__name__)

QUERY_IDS = """
MATCH (rle:ReactionLikeEvent)
OPTIONAL MATCH (rle)-[:input|hasComponent|hasMember|hasCandidate|proteinMarker|RNAMarker*]->(pe:PhysicalEntity),
               (pe)-[:referenceEntity]->(re:ReferenceEntity)-[:referenceDatabase]->(rd:ReferenceDatabase{displayName:$refDb})
WITH rle, COLLECT(DISTINCT CASE pe WHEN NULL THEN NULL ELSE {uniprot: re.identifier, type:'input'} END) AS ps_input
OPTIONAL MATCH (rle)-[:output|hasComponent|hasMember|hasCandidate|proteinMarker|RNAMarker*]->(pe:PhysicalEntity),
               (pe)-[:referenceEntity]->(re:ReferenceEntity)-[:referenceDatabase]->(rd:ReferenceDatabase{displayName:$refDb})
WITH rle, ps_input, COLLECT(DISTINCT CASE pe WHEN NULL THEN NULL ELSE {uniprot: re.identifier, type:'output'} END) AS ps_output
OPTIONAL MATCH (rle)-[:catalystActivity|physicalEntity|hasComponent|hasMember|hasCandidate|proteinMarker|RNAMarker*]->(pe:PhysicalEntity),
               (pe)-[:referenceEntity]->(re:ReferenceEntity)-[:referenceDatabase]->(rd:ReferenceDatabase{displayName:$refDb})
WITH rle, ps_input, ps_output, COLLECT(DISTINCT CASE pe WHEN NULL THEN NULL ELSE {uniprot: re.identifier, type:'catalyst'} END) AS ps_catalyst
OPTIONAL MATCH (rle)-[:regulatedBy]->(:NegativeRegulation)-[:regulator|hasComponent|hasMember|hasCandidate|proteinMarker|RNAMarker*]->(pe:PhysicalEntity),
               (pe)-[:referenceEntity]->(re:ReferenceEntity)-[:referenceDatabase]->(rd:ReferenceDatabase{displayName:$refDb})
WITH rle, ps_input, ps_output, ps_catalyst, COLLECT(DISTINCT CASE pe WHEN NULL THEN NULL ELSE {uniprot: re.identifier, type:'negative'} END) AS ps_negative
OPTIONAL MATCH (rle)-[:regulatedBy]->(:PositiveRegulation)-[:regulator|hasComponent|hasMember|hasCandidate|proteinMarker|RNAMarker*]->(pe:PhysicalEntity),
               (pe)-[:referenceEntity]->(re:ReferenceEntity)-[:referenceDatabase]->(rd:ReferenceDatabase{displayName:$refDb})
WITH rle, ps_input, ps_output, ps_catalyst, ps_negative, COLLECT(DISTINCT CASE pe WHEN NULL THEN NULL ELSE {uniprot: re.identifier, type:'positive'} END) AS ps_positive
WITH rle, ps_input + ps_output + ps_catalyst + ps_negative + ps_positive AS ps
MATCH path=(p:Pathway)-[:hasEvent]->(rle)
UNWIND ps AS part
WITH p, rle, part
WHERE part IS NOT NULL
RETURN p.stId AS pathway_id, rle.stId AS reaction_id, rle.displayName as reaction_name, part.uniprot as uniprot_acc, collect(part.type) as role_in_reaction
ORDER BY pathway_id, reaction_id, uniprot_acc
"""

def export_reactome_reactions(output_file: Path, neo4j_uri: str, neo4j_user: str, neo4j_password: str, ref_db: str = "UniProt"):
    """
    Connects to a Reactome Neo4j graph database and exports the reaction data
    to a TSV file for all species.
    """
    logger.info(f"Connecting to Neo4j at {neo4j_uri} to generate Reactome export...")
    try:
        driver = GraphDatabase.driver(neo4j_uri, auth=(neo4j_user, neo4j_password))
    except Exception as e:
        logger.error(f"Failed to connect to Neo4j: {e}")
        raise

    output_file.parent.mkdir(parents=True, exist_ok=True)

    try:
        with driver.session() as session:
            logger.info("Executing Cypher query...")
            result = session.run(QUERY_IDS, refDb=ref_db)
            
            with open(output_file, 'w', newline='', encoding='utf-8') as f:
                writer = csv.writer(f, delimiter='\t')
                writer.writerow(["pathway_id", "reaction_id", "reaction_name", "uniprot_acc", "role_in_reaction"])
                
                count = 0
                for record in result:
                    pathway_id = record["pathway_id"]
                    reaction_id = record["reaction_id"]
                    reaction_name = record["reaction_name"]
                    uniprot_acc = record["uniprot_acc"]
                    role_in_reaction = ", ".join(record["role_in_reaction"]) if record["role_in_reaction"] else ""
                    
                    if uniprot_acc: 
                        writer.writerow([pathway_id, reaction_id, reaction_name, uniprot_acc, f"[{role_in_reaction}]"])
                        count += 1
                        
            logger.info(f"Export completed. {count} rows written to {output_file}")
    finally:
        driver.close()
