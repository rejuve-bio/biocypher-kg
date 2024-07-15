# Author Abdulrahman S. Omar <xabush@singularitynet.io>
from biocypher import BioCypher
import pathlib
import os
from biocypher._logger import logger
import networkx as nx

class PrologWriter:

    def __init__(self, schema_config, biocypher_config,
                 output_dir):
        self.schema_config = schema_config
        self.biocypher_config = biocypher_config
        self.output_path = pathlib.Path(output_dir)

        if not os.path.exists(output_dir):
            self.output_path.mkdir()

        self.bcy = BioCypher(schema_config_path=schema_config,
                             biocypher_config_path=biocypher_config)

        self.ontology = self.bcy._get_ontology()
        self.create_edge_types()
        #self.excluded_properties = ["license", "version", "source"]
        self.excluded_properties = []


    def create_edge_types(self):
        schema = self.bcy._get_ontology_mapping()._extend_schema()
        self.edge_node_types = {}

        for k, v in schema.items():
            if v["represented_as"] == "edge":
                source_type = v.get("source", None)
                target_type = v.get("target", None)
                # ## TODO fix this in the scheme config
                if source_type is not None and target_type is not None:
                    if isinstance(v["input_label"], list):
                        label = self.sanitize_text(v["input_label"][0])
                        source_type = self.sanitize_text(v["source"][0])
                        target_type = self.sanitize_text(v["target"][0])
                    else:
                        label = self.sanitize_text(v["input_label"])
                        source_type = self.sanitize_text(v["source"])
                        target_type = self.sanitize_text(v["target"])
                    output_label = v.get("output_label", None)
                    self.edge_node_types[label.lower()] = {"source": source_type.lower(), "target": target_type.lower(),
                                                           "output_label": output_label.lower() if output_label is not None else None}

    def write_nodes(self, nodes, path_prefix=None, create_dir=True):
        if path_prefix is not None:
            file_path = f"{self.output_path}/{path_prefix}/nodes.pl"
            if create_dir:
                if not os.path.exists(f"{self.output_path}/{path_prefix}"):
                    pathlib.Path(f"{self.output_path}/{path_prefix}").mkdir(parents=True, exist_ok=True)
        else:
            file_path = f"{self.output_path}/nodes.pl"
        with open(file_path, "a") as f:
            for node in nodes:
                out_str = self.write_node(node)
                for s in out_str:
                    f.write(s + "\n")

            f.write("\n")

        logger.info("Finished writing out nodes")

    def write_edges(self, edges, path_prefix=None, create_dir=True):
        if path_prefix is not None:
            file_path = f"{self.output_path}/{path_prefix}/edges.pl"
            if create_dir:
                if not os.path.exists(f"{self.output_path}/{path_prefix}"):
                    pathlib.Path(f"{self.output_path}/{path_prefix}").mkdir(parents=True, exist_ok=True)
        else:
            file_path = f"{self.output_path}/edges.pl"

        with open(file_path, "a") as f:
            for edge in edges:
                out_str = self.write_edge(edge)
                for s in out_str:
                    f.write(s + "\n")

            f.write("\n")

    def write_node(self, node):
        id, label, properties = node
        if "." in label:
            label = label.split(".")[1]
        label = label.lower()
        id = self.sanitize_text(id.lower())
        def_out = f"{self.sanitize_text(label)}({id})"
        return self.write_property(def_out, properties)

    def write_edge(self, edge):
        source_id, target_id, label, properties = edge
        label = label.lower()
        source_id = source_id.lower()
        target_id = target_id.lower()
        source_type = self.edge_node_types[label]["source"]
        target_type = self.edge_node_types[label]["target"]
        output_label = self.edge_node_types[label]["output_label"]
        if output_label is not None:
            label = output_label.lower()
        source_id = self.sanitize_text(source_id)
        target_id = self.sanitize_text(target_id)
        label = self.sanitize_text(label)
        def_out = f"{label}({source_type}({source_id}), {target_type}({target_id}))"
        return self.write_property(def_out, properties)


    def write_property(self, def_out, property):
        out_str = [f"{def_out}."]
        for k, v in property.items():
            if k in self.excluded_properties or v is None or v == "": continue
            if isinstance(v, list):
                prop = "["
                for i, e in enumerate(v):
                    prop += f'{self.sanitize_text(e)}'
                    if i != len(v) - 1: prop += ","
                prop += "]"
                out_str.append(f'{k}({def_out}, {prop}).')
            elif isinstance(v, dict):
                prop = f"{k}({def_out})."
                out_str.extend(self.write_property(prop, v))
            else:
                prop = self.sanitize_text(v)
                if prop is not None:
                    out_str.append(f'{k}({def_out}, {prop}).')
        return out_str

    def sanitize_text(self, prop):
        replace_chars = [" ", "-", ":"]
        omit_chars = ["(", ")", "+", "."]
        if isinstance(prop, str):        
            for c in replace_chars:
                prop = prop.replace(c, "_").lower()
            for c in omit_chars:
                prop = prop.replace(c, "").lower()         
            prop = prop.strip("_")
            if prop == "":
                return None

            # strips '_' each string separated by comma
            # 'abc,__fg,___,_x' ===> 'abc,fg,x'
            prop = ",".join([p.strip('_') for p in prop.split(',') if p.strip('_') != ""])
        
            try:
                float(prop)
                return prop # It's a numeric string, return as is
            except ValueError:
                # Check if the first character is a digit
                if prop[0].isdigit():
                    return f"'{prop}'"
        elif isinstance(prop, list):
            for i in range(len(prop)):
                prop[i] = self.sanitize_text(prop[i])
            prop = [p for p in prop if p != None]
        return prop

    def get_parent(self, G, node):
        """
        Get the immediate parent of a node in the ontology.
        """
        return nx.dfs_preorder_nodes(G, node, depth_limit=2)

    def show_ontology_structure(self):
        self.bcy.show_ontology_structure()

    def summary(self):
        self.bcy.summary()