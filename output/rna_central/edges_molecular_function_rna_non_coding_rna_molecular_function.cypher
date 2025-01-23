
    CALL apoc.periodic.iterate(
        "LOAD CSV WITH HEADERS FROM 'file:////Users/ermiyasggod/Desktop/BioCypher/biocypher-kg/output/rna_central/edges_molecular_function_rna_non_coding_rna_molecular_function.csv' AS row FIELDTERMINATOR '|' RETURN row",
        "MATCH (source:non_coding_rna {id: row.source_id})
        MATCH (target:molecular_function {id: row.target_id})
        MERGE (source)-[r:molecular_function_rna]->(target)
        SET r += apoc.map.removeKeys(row, ['source_id', 'target_id', 'label', 'source_type', 'target_type'])",
        {batchSize:1000}
    )
    YIELD batches, total
    RETURN batches, total;
            