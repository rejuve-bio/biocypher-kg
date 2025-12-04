
CALL apoc.periodic.iterate(
    "LOAD CSV WITH HEADERS FROM 'file:////app/output_human/roadmap/dhs/edges_in_dnase_i_hotspot_snp_uberon.csv' AS row FIELDTERMINATOR '|' RETURN row",
    "MATCH (source:snp {id: row.source_id})
    MATCH (target:uberon {id: row.target_id})
    MERGE (source)-[r:in_dnase_i_hotspot]->(target)
    SET r += apoc.map.removeKeys(row, ['source_id', 'target_id', 'label', 'source_type', 'target_type'])",
    {batchSize:1000}
)
YIELD batches, total
RETURN batches, total;
