
CALL apoc.periodic.iterate(
    "LOAD CSV WITH HEADERS FROM 'file:////app/output_human/reactome/biological_process/edges_pathway_to_biological_process_pathway_biological_process.csv' AS row FIELDTERMINATOR '|' RETURN row",
    "MATCH (source:pathway {id: row.source_id})
    MATCH (target:biological_process {id: row.target_id})
    MERGE (source)-[r:pathway_to_biological_process]->(target)
    SET r += apoc.map.removeKeys(row, ['source_id', 'target_id', 'label', 'source_type', 'target_type'])",
    {batchSize:1000}
)
YIELD batches, total
RETURN batches, total;
