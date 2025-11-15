
CREATE CONSTRAINT IF NOT EXISTS FOR (n:efo) REQUIRE n.id IS UNIQUE;

CALL apoc.periodic.iterate(
    "LOAD CSV WITH HEADERS FROM 'file:////app/output_human/experimental_factor_ontology/nodes_efo.csv' AS row FIELDTERMINATOR '|' RETURN row",
    "MERGE (n:efo {id: row.id})
    SET n += apoc.map.removeKeys(row, ['id'])",
    {batchSize:1000, parallel:true, concurrency:4}
)
YIELD batches, total
RETURN batches, total;
