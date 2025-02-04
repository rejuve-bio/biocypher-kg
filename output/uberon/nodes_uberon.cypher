
CREATE CONSTRAINT IF NOT EXISTS FOR (n:uberon) REQUIRE n.id IS UNIQUE;

CALL apoc.periodic.iterate(
    "LOAD CSV WITH HEADERS FROM 'file:////Users/ermiyasggod/Desktop/BioCypher/biocypher-kg/output/uberon/nodes_uberon.csv' AS row FIELDTERMINATOR '|' RETURN row",
    "MERGE (n:uberon:ontology_term {id: row.id})
    SET n += apoc.map.removeKeys(row, ['id'])",
    {batchSize:1000, parallel:true, concurrency:4}
)
YIELD batches, total
RETURN batches, total;
                