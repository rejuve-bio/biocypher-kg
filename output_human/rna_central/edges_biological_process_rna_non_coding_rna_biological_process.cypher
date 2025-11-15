
CALL apoc.periodic.iterate(
    "LOAD CSV WITH HEADERS FROM 'file:////app/output_human/rna_central/edges_biological_process_rna_non_coding_rna_biological_process.csv' AS row FIELDTERMINATOR '|' RETURN row",
    "MATCH (source:non_coding_rna {id: row.source_id})
    MATCH (target:biological_process {id: row.target_id})
    MERGE (source)-[r:participates_in]->(target)
    SET r += apoc.map.removeKeys(row, ['source_id', 'target_id', 'label', 'source_type', 'target_type'])",
    {batchSize:1000}
)
YIELD batches, total
RETURN batches, total;
