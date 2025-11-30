
CALL apoc.periodic.iterate(
    "LOAD CSV WITH HEADERS FROM 'file:////app/output_human/brenda_tissue_ontology/edges_bto_subclass_of_bto_bto.csv' AS row FIELDTERMINATOR '|' RETURN row",
    "MATCH (source:bto {id: row.source_id})
    MATCH (target:bto {id: row.target_id})
    MERGE (source)-[r:is_a]->(target)
    SET r += apoc.map.removeKeys(row, ['source_id', 'target_id', 'label', 'source_type', 'target_type'])",
    {batchSize:1000}
)
YIELD batches, total
RETURN batches, total;
