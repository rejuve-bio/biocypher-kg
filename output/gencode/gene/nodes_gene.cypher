
    CREATE CONSTRAINT IF NOT EXISTS FOR (n:gene) REQUIRE n.id IS UNIQUE;

    CALL apoc.periodic.iterate(
        "LOAD CSV WITH HEADERS FROM 'file:///C:/Users/Abdu/Desktop/Rejuve/biocypher-kg/output/gencode/gene/nodes_gene.csv' AS row FIELDTERMINATOR '|' RETURN row",
        "MERGE (n:gene {id: row.id})
        SET n += apoc.map.removeKeys(row, ['id'])",
        {batchSize:1000, parallel:true, concurrency:4}
    )
    YIELD batches, total
    RETURN batches, total;
    