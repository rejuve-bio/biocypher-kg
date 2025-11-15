
CALL apoc.periodic.iterate(
    "LOAD CSV WITH HEADERS FROM 'file:////app/output_human/uberon/edges_uberon_subclass_of_uberon_uberon.csv' AS row FIELDTERMINATOR '|' RETURN row",
    "MATCH (source:uberon {id: row.source_id})
    MATCH (target:uberon {id: row.target_id})
    MERGE (source)-[r:subclass_of]->(target)
    SET r += apoc.map.removeKeys(row, ['source_id', 'target_id', 'label', 'source_type', 'target_type'])",
    {batchSize:1000}
)
YIELD batches, total
RETURN batches, total;
