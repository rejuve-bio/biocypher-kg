
CALL apoc.periodic.iterate(
    "LOAD CSV WITH HEADERS FROM 'file:////app/output_human/cell_ontology/edges_cl_capable_of_cl_biological_process.csv' AS row FIELDTERMINATOR '|' RETURN row",
    "MATCH (source:cl {id: row.source_id})
    MATCH (target:biological_process {id: row.target_id})
    MERGE (source)-[r:capable_of]->(target)
    SET r += apoc.map.removeKeys(row, ['source_id', 'target_id', 'label', 'source_type', 'target_type'])",
    {batchSize:1000}
)
YIELD batches, total
RETURN batches, total;
