
CREATE CONSTRAINT IF NOT EXISTS FOR (n:protein) REQUIRE n.id IS UNIQUE;

CALL apoc.periodic.iterate(
    "LOAD CSV WITH HEADERS FROM 'file:////app/output_human/uniprot/nodes_protein.csv' AS row FIELDTERMINATOR '|' RETURN row",
    "MERGE (n:protein {id: row.id})
    SET n += apoc.map.removeKeys(row, ['id'])",
    {batchSize:1000, parallel:true, concurrency:4}
)
YIELD batches, total
RETURN batches, total;
