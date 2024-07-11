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
        self.create_edge_types()

        self.excluded_properties = []
        
    def create_edge_types(self):
            schema = self.bcy._get_ontology_mapping()._extend_schema()
            self.edge_node_types = {}

            for k, v in schema.items():
                if (
                    v["represented_as"] == "edge"
                ):  # (: (label $x $y) (-> source_type target_type
                    edge_type = self.convert_input_labels(k)
                    source_type = v.get("source", None)
                    target_type = v.get("target", None)

                    if source_type is not None and target_type is not None:
                        # ## TODO fix this in the scheme config
                        if isinstance(v["input_label"], list):
                            label = self.convert_input_labels(v["input_label"][0])
                            source_type = self.convert_input_labels(source_type[0])
                            target_type = self.convert_input_labels(target_type[0])
                        else:
                            label = self.convert_input_labels(v["input_label"])
                            source_type = self.convert_input_labels(source_type)
                            target_type = self.convert_input_labels(target_type)
                        output_label = v.get("output_label", None)

                        self.edge_node_types[label.lower()] = {
                            "source": source_type.lower(),
                            "target": target_type.lower(),
                            "output_label": (
                                output_label.lower() if output_label is not None else None
                            ),
                        }
