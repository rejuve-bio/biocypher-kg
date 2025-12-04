
CREATE CONSTRAINT IF NOT EXISTS FOR (n:hpo) REQUIRE n.id IS UNIQUE;

CALL apoc.periodic.iterate(
    "LOAD CSV WITH HEADERS FROM 'file:////app/output_human/human_phenotype_ontology/nodes_hpo.csv' AS row FIELDTERMINATOR '|' RETURN row",
    "MERGE (n:hpo {id: row.id})
    SET n += apoc.map.removeKeys(row, ['id'])",
    {batchSize:1000, parallel:true, concurrency:4}
)
YIELD batches, total
RETURN batches, total;
