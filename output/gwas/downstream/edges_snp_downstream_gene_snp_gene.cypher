
    CALL apoc.periodic.iterate(
        "LOAD CSV WITH HEADERS FROM 'file:////Users/ermiyasggod/Desktop/BioCypher/biocypher-kg/output/gwas/downstream/edges_snp_downstream_gene_snp_gene.csv' AS row FIELDTERMINATOR '|' RETURN row",
        "MATCH (source:snp {id: row.source_id})
        MATCH (target:gene {id: row.target_id})
        MERGE (source)-[r:snp_downstream_gene]->(target)
        SET r += apoc.map.removeKeys(row, ['source_id', 'target_id', 'label', 'source_type', 'target_type'])",
        {batchSize:1000}
    )
    YIELD batches, total
    RETURN batches, total;
            