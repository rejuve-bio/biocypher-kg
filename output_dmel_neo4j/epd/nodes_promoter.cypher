
CREATE CONSTRAINT IF NOT EXISTS FOR (n:promoter) REQUIRE n.id IS UNIQUE;

CALL apoc.periodic.iterate(
    "LOAD CSV WITH HEADERS FROM 'file:////home/eyorica/Downloads/biocypherKG/biocypher-kg/output_dmel_neo4j/epd/nodes_promoter.csv' AS row FIELDTERMINATOR '|' RETURN row",
    "MERGE (n:promoter {id: row.id})
    SET n += apoc.map.removeKeys(row, ['id'])",
    {batchSize:1000, parallel:true, concurrency:4}
)
YIELD batches, total
RETURN batches, total;
