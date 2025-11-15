
CREATE CONSTRAINT IF NOT EXISTS FOR (n:enhancer) REQUIRE n.id IS UNIQUE;

CALL apoc.periodic.iterate(
    "LOAD CSV WITH HEADERS FROM 'file:////app/output_human/enhancer_atlas/nodes_enhancer.csv' AS row FIELDTERMINATOR '|' RETURN row",
    "MERGE (n:enhancer {id: row.id})
    SET n += apoc.map.removeKeys(row, ['id'])",
    {batchSize:1000, parallel:true, concurrency:4}
)
YIELD batches, total
RETURN batches, total;
