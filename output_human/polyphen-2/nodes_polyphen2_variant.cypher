
CREATE CONSTRAINT IF NOT EXISTS FOR (n:polyphen2_variant) REQUIRE n.id IS UNIQUE;

CALL apoc.periodic.iterate(
    "LOAD CSV WITH HEADERS FROM 'file:////app/output_human/polyphen-2/nodes_polyphen2_variant.csv' AS row FIELDTERMINATOR '|' RETURN row",
    "MERGE (n:polyphen2_variant {id: row.id})
    SET n += apoc.map.removeKeys(row, ['id'])",
    {batchSize:1000, parallel:true, concurrency:4}
)
YIELD batches, total
RETURN batches, total;
