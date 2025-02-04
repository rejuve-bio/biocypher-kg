
CREATE CONSTRAINT IF NOT EXISTS FOR (n:molecular_function) REQUIRE n.id IS UNIQUE;

CALL apoc.periodic.iterate(
    "LOAD CSV WITH HEADERS FROM 'file:////Users/ermiyasggod/Desktop/BioCypher/biocypher-kg/output/gene_ontology/molecular_function/nodes_molecular_function.csv' AS row FIELDTERMINATOR '|' RETURN row",
    "MERGE (n:molecular_function {id: row.id})
    SET n += apoc.map.removeKeys(row, ['id'])",
    {batchSize:1000, parallel:true, concurrency:4}
)
YIELD batches, total
RETURN batches, total;
                