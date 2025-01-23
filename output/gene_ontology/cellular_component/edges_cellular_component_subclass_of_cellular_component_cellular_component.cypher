
    CALL apoc.periodic.iterate(
        "LOAD CSV WITH HEADERS FROM 'file:////Users/ermiyasggod/Desktop/BioCypher/biocypher-kg/output/gene_ontology/cellular_component/edges_cellular_component_subclass_of_cellular_component_cellular_component.csv' AS row FIELDTERMINATOR '|' RETURN row",
        "MATCH (source:cellular_component {id: row.source_id})
        MATCH (target:cellular_component {id: row.target_id})
        MERGE (source)-[r:cellular_component_subclass_of]->(target)
        SET r += apoc.map.removeKeys(row, ['source_id', 'target_id', 'label', 'source_type', 'target_type'])",
        {batchSize:1000}
    )
    YIELD batches, total
    RETURN batches, total;
            