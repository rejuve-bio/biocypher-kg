
CALL apoc.periodic.iterate(
    "LOAD CSV WITH HEADERS FROM 'file:///reactome/ppi/edges_protein_interacts_with_protein.csv' AS row FIELDTERMINATOR '|' RETURN row",
    "MATCH (source:protein {id: row.source_id})
    MATCH (target:protein {id: row.target_id})
    CREATE (source)-[r:interacts_with]->(target)
    SET r += apoc.map.removeKeys(row, ['source_id', 'target_id', 'label', 'source_type', 'target_type'])",
    {batchSize:1000}
)
YIELD batches, total
RETURN batches, total;
