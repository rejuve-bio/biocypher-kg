
CALL apoc.periodic.iterate(
    "LOAD CSV WITH HEADERS FROM 'file:////app/output_human/gencode/exon/edges_includes_transcript_exon.csv' AS row FIELDTERMINATOR '|' RETURN row",
    "MATCH (source:transcript {id: row.source_id})
    MATCH (target:exon {id: row.target_id})
    MERGE (source)-[r:includes]->(target)
    SET r += apoc.map.removeKeys(row, ['source_id', 'target_id', 'label', 'source_type', 'target_type'])",
    {batchSize:1000}
)
YIELD batches, total
RETURN batches, total;
