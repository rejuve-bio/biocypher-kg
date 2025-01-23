
CREATE CONSTRAINT IF NOT EXISTS FOR (n:clo) REQUIRE n.id IS UNIQUE;

CALL apoc.periodic.iterate(
    "LOAD CSV WITH HEADERS FROM 'file:////Users/ermiyasggod/Desktop/BioCypher/biocypher-kg/output/cell_line_ontology/nodes_clo.csv' AS row FIELDTERMINATOR '|' RETURN row",
    "MERGE (n:clo:ontology_term {id: row.id})
    SET n += apoc.map.removeKeys(row, ['id'])",
    {batchSize:1000, parallel:true, concurrency:4}
)
YIELD batches, total
RETURN batches, total;
                