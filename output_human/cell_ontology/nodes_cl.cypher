
CREATE CONSTRAINT IF NOT EXISTS FOR (n:cl) REQUIRE n.id IS UNIQUE;

CALL apoc.periodic.iterate(
    "LOAD CSV WITH HEADERS FROM 'file:////app/output_human/cell_ontology/nodes_cl.csv' AS row FIELDTERMINATOR '|' RETURN row",
    "MERGE (n:cl {id: row.id})
    SET n += apoc.map.removeKeys(row, ['id'])",
    {batchSize:1000, parallel:true, concurrency:4}
)
YIELD batches, total
RETURN batches, total;
