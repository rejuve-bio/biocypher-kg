
CALL apoc.periodic.iterate(
    "LOAD CSV WITH HEADERS FROM 'file:////app/output_human/roadmap/h3_mark/edges_histone_modification_snp_uberon.csv' AS row FIELDTERMINATOR '|' RETURN row",
    "MATCH (source:snp {id: row.source_id})
    MATCH (target:uberon {id: row.target_id})
    MERGE (source)-[r:histone_modification]->(target)
    SET r += apoc.map.removeKeys(row, ['source_id', 'target_id', 'label', 'source_type', 'target_type'])",
    {batchSize:1000}
)
YIELD batches, total
RETURN batches, total;
