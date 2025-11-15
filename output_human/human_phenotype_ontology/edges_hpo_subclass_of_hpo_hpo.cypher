
CALL apoc.periodic.iterate(
    "LOAD CSV WITH HEADERS FROM 'file:////app/output_human/human_phenotype_ontology/edges_hpo_subclass_of_hpo_hpo.csv' AS row FIELDTERMINATOR '|' RETURN row",
    "MATCH (source:hpo {id: row.source_id})
    MATCH (target:hpo {id: row.target_id})
    MERGE (source)-[r:subclass_of]->(target)
    SET r += apoc.map.removeKeys(row, ['source_id', 'target_id', 'label', 'source_type', 'target_type'])",
    {batchSize:1000}
)
YIELD batches, total
RETURN batches, total;
