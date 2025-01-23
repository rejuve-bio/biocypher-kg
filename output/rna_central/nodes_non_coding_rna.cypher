
CREATE CONSTRAINT IF NOT EXISTS FOR (n:non_coding_rna) REQUIRE n.id IS UNIQUE;

CALL apoc.periodic.iterate(
    "LOAD CSV WITH HEADERS FROM 'file:////Users/ermiyasggod/Desktop/BioCypher/biocypher-kg/output/rna_central/nodes_non_coding_rna.csv' AS row FIELDTERMINATOR '|' RETURN row",
    "MERGE (n:non_coding_rna {id: row.id})
    SET n += apoc.map.removeKeys(row, ['id'])",
    {batchSize:1000, parallel:true, concurrency:4}
)
YIELD batches, total
RETURN batches, total;
                