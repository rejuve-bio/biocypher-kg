
    CALL apoc.periodic.iterate(
        "LOAD CSV WITH HEADERS FROM 'file:////Users/ermiyasggod/Desktop/BioCypher/biocypher-kg/output/cell_ontology/edges_cl_subclass_of_cl_cl.csv' AS row FIELDTERMINATOR '|' RETURN row",
        "MATCH (source:cl {id: row.source_id})
        MATCH (target:cl {id: row.target_id})
        MERGE (source)-[r:cl_subclass_of]->(target)
        SET r += apoc.map.removeKeys(row, ['source_id', 'target_id', 'label', 'source_type', 'target_type'])",
        {batchSize:1000}
    )
    YIELD batches, total
    RETURN batches, total;
            