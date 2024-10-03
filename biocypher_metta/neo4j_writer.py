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

    def write_nodes(self, nodes, path_prefix=None, create_dir=True):
        if path_prefix is not None:
            file_path = f"{self.output_path}/{path_prefix}/nodes.cypher"
            if create_dir:
                if not os.path.exists(f"{self.output_path}/{path_prefix}"):
                    pathlib.Path(f"{self.output_path}/{path_prefix}").mkdir(
                        parents=True, exist_ok=True
                    )
        else:
            file_path = f"{self.output_path}/nodes.cypher"

        with open(file_path, "a") as f:
            for node in nodes:
                query = self.write_node(node)
                f.write(query + "\n")

        logger.info("Finished writing out nodes")

    def write_edges(self, edges, path_prefix=None, create_dir=True):
        if path_prefix is not None:
            file_path = f"{self.output_path}/{path_prefix}/edges.cypher"
            if create_dir:
                if not os.path.exists(f"{self.output_path}/{path_prefix}"):
                    pathlib.Path(f"{self.output_path}/{path_prefix}").mkdir(
                        parents=True, exist_ok=True
                    )
        else:
            file_path = f"{self.output_path}/edges.cypher"

        with open(file_path, "a") as f:
            for edge in edges:
                query = self.write_edge(edge)
                f.write(query + "\n")

        logger.info("Finished writing out edges")

    def write_node(self, node):
        id, label, properties = node
        if "." in label:
            label = label.split(".")[1]
        label = label.lower()
        id = id.lower()
        properties_str = self._format_properties(properties)
        return f'MERGE (:{label} {{id: "{id}", {properties_str}}})'

    def write_edge(self, edge):
        source_id, target_id, label, properties = edge
        label = label.lower()
        source_id = source_id.lower()
        target_id = target_id.lower()
        source_type = self.edge_node_types[label]["source"]
        target_type = self.edge_node_types[label]["target"]
        output_label = self.edge_node_types[label]["output_label"]
        if output_label is not None:
            label = output_label
        properties_str = self._format_properties(properties)
        return (
            f"MATCH (a:{source_type}) "
            f"WITH a "
            f"MATCH (b:{target_type}) "
            f'WHERE a.id = "{source_id}" AND b.id = "{target_id}" '
            f"MERGE (a)-[:{label} {{ {properties_str} }}]->(b);"
        )

    def _format_properties(self, properties):
        """
        Format properties into a Cypher string.
        :param properties: Dictionary of properties
        :return: Cypher formatted properties string
        """
        out_str = []
        for k, v in properties.items():
            if k in self.excluded_properties or v is None or v == "":
                continue
            
            if isinstance(v, list):
                formatted_list = []
                for e in v:
                    if isinstance(e, str):
                        cleaned_str = e.replace('"', '').replace("'", "")
                        formatted_list.append(f'"{cleaned_str}"')
                    else:
                        formatted_list.append(str(e))
                prop = "[" + ", ".join(formatted_list) + "]"
            elif isinstance(v, dict):
                prop = "{" + self._format_properties(v) + "}"
            elif isinstance(v, str):
                cleaned_str = v.replace('"', '').replace("'", "")
                prop = f'"{cleaned_str}"'
            elif isinstance(v, int) or isinstance(v, float):
                prop = v
            else:
                prop = v
        
            out_str.append(f"{k}: {prop}")
        
        return ", ".join(out_str)

    def convert_input_labels(self, label, replace_char="_"):
        """
        A method that removes spaces in input labels and replaces them with replace_char
        :param label: Input label of a node or edge
        :param replace_char: the character to replace spaces with
        :return:
        """
        return label.replace(" ", replace_char)

    def get_parent(self, G, node):
        """
        Get the immediate parent of a node in the ontology.
        """
        return nx.dfs_preorder_nodes(G, node, depth_limit=2)

    def show_ontology_structure(self):
        self.bcy.show_ontology_structure()

    def summary(self):
        self.bcy.summary()
