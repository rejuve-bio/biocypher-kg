
CALL apoc.periodic.iterate(
    "LOAD CSV WITH HEADERS FROM 'file:////app/output_human/gaf/edges_molecular_function_gene_product_protein_molecular_function.csv' AS row FIELDTERMINATOR '|' RETURN row",
    "MATCH (source:protein {id: row.source_id})
    MATCH (target:molecular_function {id: row.target_id})
    MERGE (source)-[r:enables]->(target)
    SET r += apoc.map.removeKeys(row, ['source_id', 'target_id', 'label', 'source_type', 'target_type'])",
    {batchSize:1000}
)
YIELD batches, total
RETURN batches, total;
