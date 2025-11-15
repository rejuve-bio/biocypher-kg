
CALL apoc.periodic.iterate(
    "LOAD CSV WITH HEADERS FROM 'file:////app/output_human/rna_central/edges_cellular_component_rna_non_coding_rna_cellular_component.csv' AS row FIELDTERMINATOR '|' RETURN row",
    "MATCH (source:non_coding_rna {id: row.source_id})
    MATCH (target:cellular_component {id: row.target_id})
    MERGE (source)-[r:located_in]->(target)
    SET r += apoc.map.removeKeys(row, ['source_id', 'target_id', 'label', 'source_type', 'target_type'])",
    {batchSize:1000}
)
YIELD batches, total
RETURN batches, total;
