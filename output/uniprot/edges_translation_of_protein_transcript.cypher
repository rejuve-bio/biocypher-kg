
    CALL apoc.periodic.iterate(
        "LOAD CSV WITH HEADERS FROM 'file:///C:/Users/Abdu/Desktop/Rejuve/biocypher-kg/output/uniprot/edges_translation_of_protein_transcript.csv' AS row FIELDTERMINATOR '|' RETURN row",
        "MATCH (source:protein {id: row.source_id})
        MATCH (target:transcript {id: row.target_id})
        MERGE (source)-[r:None]->(target)
        SET r += apoc.map.removeKeys(row, ['source_id', 'target_id', 'label', 'source_type', 'target_type'])",
        {batchSize:1000}
    )
    YIELD batches, total
    RETURN batches, total;
    