
    CALL apoc.periodic.iterate(
        "LOAD CSV WITH HEADERS FROM 'file:////Users/ermiyasggod/Desktop/BioCypher/biocypher-kg/output/tfbs/edges_gene_tfbs_gene_tfbs.csv' AS row FIELDTERMINATOR '|' RETURN row",
        "MATCH (source:gene {id: row.source_id})
        MATCH (target:tfbs {id: row.target_id})
        MERGE (source)-[r:gene_tfbs]->(target)
        SET r += apoc.map.removeKeys(row, ['source_id', 'target_id', 'label', 'source_type', 'target_type'])",
        {batchSize:1000}
    )
    YIELD batches, total
    RETURN batches, total;
            