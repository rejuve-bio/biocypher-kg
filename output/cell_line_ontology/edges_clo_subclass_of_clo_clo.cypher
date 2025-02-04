
    CALL apoc.periodic.iterate(
        "LOAD CSV WITH HEADERS FROM 'file:////Users/ermiyasggod/Desktop/BioCypher/biocypher-kg/output/cell_line_ontology/edges_clo_subclass_of_clo_clo.csv' AS row FIELDTERMINATOR '|' RETURN row",
        "MATCH (source:clo {id: row.source_id})
        MATCH (target:clo {id: row.target_id})
        MERGE (source)-[r:clo_subclass_of]->(target)
        SET r += apoc.map.removeKeys(row, ['source_id', 'target_id', 'label', 'source_type', 'target_type'])",
        {batchSize:1000}
    )
    YIELD batches, total
    RETURN batches, total;
            