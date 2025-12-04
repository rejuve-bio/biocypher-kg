
CALL apoc.periodic.iterate(
    "LOAD CSV WITH HEADERS FROM 'file:////app/output_human/MotifDiff/edges_tf_snp_gene_snp.csv' AS row FIELDTERMINATOR '|' RETURN row",
    "MATCH (source:gene {id: row.source_id})
    MATCH (target:snp {id: row.target_id})
    MERGE (source)-[r:tf_snp]->(target)
    SET r += apoc.map.removeKeys(row, ['source_id', 'target_id', 'label', 'source_type', 'target_type'])",
    {batchSize:1000}
)
YIELD batches, total
RETURN batches, total;
