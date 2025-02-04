
CREATE CONSTRAINT IF NOT EXISTS FOR (n:bto) REQUIRE n.id IS UNIQUE;

CALL apoc.periodic.iterate(
    "LOAD CSV WITH HEADERS FROM 'file:////Users/ermiyasggod/Desktop/BioCypher/biocypher-kg/output/brenda_tissue_ontology/nodes_bto.csv' AS row FIELDTERMINATOR '|' RETURN row",
    "MERGE (n:bto:ontology_term {id: row.id})
    SET n += apoc.map.removeKeys(row, ['id'])",
    {batchSize:1000, parallel:true, concurrency:4}
)
YIELD batches, total
RETURN batches, total;
                