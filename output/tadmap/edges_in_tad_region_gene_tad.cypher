
    CALL apoc.periodic.iterate(
        "LOAD CSV WITH HEADERS FROM 'file:////Users/ermiyasggod/Desktop/BioCypher/biocypher-kg/output/tadmap/edges_in_tad_region_gene_tad.csv' AS row FIELDTERMINATOR '|' RETURN row",
        "MATCH (source:gene {id: row.source_id})
        MATCH (target:tad {id: row.target_id})
        MERGE (source)-[r:in_tad_region]->(target)
        SET r += apoc.map.removeKeys(row, ['source_id', 'target_id', 'label', 'source_type', 'target_type'])",
        {batchSize:1000}
    )
    YIELD batches, total
    RETURN batches, total;
            