
CREATE CONSTRAINT IF NOT EXISTS FOR (n:biological_process) REQUIRE n.id IS UNIQUE;

CALL apoc.periodic.iterate(
    "LOAD CSV WITH HEADERS FROM 'file:////Users/ermiyasggod/Desktop/BioCypher/biocypher-kg/output/gene_ontology/biological_process/nodes_biological_process.csv' AS row FIELDTERMINATOR '|' RETURN row",
    "MERGE (n:biological_process {id: row.id})
    SET n += apoc.map.removeKeys(row, ['id'])",
    {batchSize:1000, parallel:true, concurrency:4}
)
YIELD batches, total
RETURN batches, total;
                