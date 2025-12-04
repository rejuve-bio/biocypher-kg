
CALL apoc.periodic.iterate(
    "LOAD CSV WITH HEADERS FROM 'file:////app/output_human/gwas/upstream/edges_snp_upstream_gene_snp_gene.csv' AS row FIELDTERMINATOR '|' RETURN row",
    "MATCH (source:snp {id: row.source_id})
    MATCH (target:gene {id: row.target_id})
    MERGE (source)-[r:upstream_of]->(target)
    SET r += apoc.map.removeKeys(row, ['source_id', 'target_id', 'label', 'source_type', 'target_type'])",
    {batchSize:1000}
)
YIELD batches, total
RETURN batches, total;
