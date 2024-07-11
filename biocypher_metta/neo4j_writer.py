import pathlib
import os
from biocypher import BioCypher
from biocypher._logger import logger
import networkx as nx


class Neo4jWriter:

    def __init__(self, schema_config, biocypher_config, output_dir):
        self.schema_config = schema_config
        self.biocypher_config = biocypher_config
        self.output_path = pathlib.Path(output_dir)

        if not os.path.exists(output_dir):
            self.output_path.mkdir()

        self.bcy = BioCypher(
            schema_config_path=schema_config, biocypher_config_path=biocypher_config
        )

        self.onotology = self.bcy._get_ontology()

        self.excluded_properties = []
