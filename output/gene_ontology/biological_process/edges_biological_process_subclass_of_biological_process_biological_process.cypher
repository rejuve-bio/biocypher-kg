
    CALL apoc.periodic.iterate(
        "LOAD CSV WITH HEADERS FROM 'file:////Users/ermiyasggod/Desktop/BioCypher/biocypher-kg/output/gene_ontology/biological_process/edges_biological_process_subclass_of_biological_process_biological_process.csv' AS row FIELDTERMINATOR '|' RETURN row",
        "MATCH (source:biological_process {id: row.source_id})
        MATCH (target:biological_process {id: row.target_id})
        MERGE (source)-[r:biological_process_subclass_of]->(target)
        SET r += apoc.map.removeKeys(row, ['source_id', 'target_id', 'label', 'source_type', 'target_type'])",
        {batchSize:1000}
    )
    YIELD batches, total
    RETURN batches, total;
            