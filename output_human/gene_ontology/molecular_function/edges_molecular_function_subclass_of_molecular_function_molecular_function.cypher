
CALL apoc.periodic.iterate(
    "LOAD CSV WITH HEADERS FROM 'file:////app/output_human/gene_ontology/molecular_function/edges_molecular_function_subclass_of_molecular_function_molecular_function.csv' AS row FIELDTERMINATOR '|' RETURN row",
    "MATCH (source:molecular_function {id: row.source_id})
    MATCH (target:molecular_function {id: row.target_id})
    MERGE (source)-[r:is_a]->(target)
    SET r += apoc.map.removeKeys(row, ['source_id', 'target_id', 'label', 'source_type', 'target_type'])",
    {batchSize:1000}
)
YIELD batches, total
RETURN batches, total;
