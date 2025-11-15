
CREATE CONSTRAINT IF NOT EXISTS FOR (n:bto) REQUIRE n.id IS UNIQUE;

CALL apoc.periodic.iterate(
    "LOAD CSV WITH HEADERS FROM 'file:////app/output_human/brenda_tissue_ontology/nodes_bto.csv' AS row FIELDTERMINATOR '|' RETURN row",
    "MERGE (n:bto {id: row.id})
    SET n += apoc.map.removeKeys(row, ['id'])",
    {batchSize:1000, parallel:true, concurrency:4}
)
YIELD batches, total
RETURN batches, total;
