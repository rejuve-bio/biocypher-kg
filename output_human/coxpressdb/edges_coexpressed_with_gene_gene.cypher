
CALL apoc.periodic.iterate(
    "LOAD CSV WITH HEADERS FROM 'file:////app/output_human/coxpressdb/edges_coexpressed_with_gene_gene.csv' AS row FIELDTERMINATOR '|' RETURN row",
    "MATCH (source:gene {id: row.source_id})
    MATCH (target:gene {id: row.target_id})
    MERGE (source)-[r:coexpressed_with]->(target)
    SET r += apoc.map.removeKeys(row, ['source_id', 'target_id', 'label', 'source_type', 'target_type'])",
    {batchSize:1000}
)
YIELD batches, total
RETURN batches, total;
