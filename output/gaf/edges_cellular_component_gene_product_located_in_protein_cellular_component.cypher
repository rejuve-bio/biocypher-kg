
    CALL apoc.periodic.iterate(
        "LOAD CSV WITH HEADERS FROM 'file:////Users/ermiyasggod/Desktop/BioCypher/biocypher-kg/output/gaf/edges_cellular_component_gene_product_located_in_protein_cellular_component.csv' AS row FIELDTERMINATOR '|' RETURN row",
        "MATCH (source:protein {id: row.source_id})
        MATCH (target:cellular_component {id: row.target_id})
        MERGE (source)-[r:cellular_component_gene_product_located_in]->(target)
        SET r += apoc.map.removeKeys(row, ['source_id', 'target_id', 'label', 'source_type', 'target_type'])",
        {batchSize:1000}
    )
    YIELD batches, total
    RETURN batches, total;
            