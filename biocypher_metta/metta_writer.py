# Author Abdulrahman S. Omar <xabush@singularitynet.io>
import pathlib
import os
from biocypher._logger import logger
import networkx as nx

from biocypher_metta import BaseWriter

class MeTTaWriter(BaseWriter):

    def __init__(self, schema_config, biocypher_config,
                 output_dir):
        super().__init__(schema_config, biocypher_config, output_dir)
        self.create_type_hierarchy()

        #self.excluded_properties = ["license", "version", "source"]
        self.excluded_properties = []


    def create_type_hierarchy(self):
        G = self.ontology._nx_graph
        file_path = f"{self.output_path}/type_defs.metta"
        with open(file_path, "w") as f:
            for node in G.nodes:
                if "mixin" in node: continue
                ancestor = list(self.get_parent(G, node))[-1]
                node = self.convert_input_labels(node)
                ancestor = self.convert_input_labels(ancestor)
                if ancestor == node:
                    f.write(f"(: {node.upper()} Type)\n")

                else:
                    f.write(f"(<: {node.upper()} {ancestor.upper()})\n")

            self.create_data_constructors(f)

        logger.info("Type hierarchy created successfully.")

    def create_data_constructors(self, file):
        schema = self.bcy._get_ontology_mapping()._extend_schema()
        self.edge_node_types = {}
        def edge_data_constructor(edge_type, source_type, target_type, label):
            return f"(: {label.lower()} (-> {source_type.upper()} {target_type.upper()} {edge_type.upper()}))"

        def node_data_constructor(node_type, node_label):
            return f"(: {node_label.lower()} (-> $x {node_type.upper()}))"

        for k, v in schema.items():
            if v["represented_as"] == "edge": #(: (label $x $y) (-> source_type target_type
                edge_type = self.convert_input_labels(k)
                #Check if there are source and target fields
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
                    # saulo
                    # out_str = edge_data_constructor(edge_type, source_type, target_type, label)
                    # file.write(out_str + "\n")
                    # self.edge_node_types[label.lower()] = {"source": source_type.lower(), "target":
                    #     target_type.lower(), "output_label": output_label.lower() if output_label is not None else None}

                    # to  handle lists in source and/or target types (2024/09/17) <-- first time I changed this! :()  :D
                    # the first case is the "general case" commented above
                    if isinstance(source_type, str) and isinstance(target_type, str): # most frequent case: source_type, target_type are strings
                        print(f"key: => {k}")# \n{v}")
                        if '.' not in k:                            
                            self.edge_node_types[label.lower()] = {
                                "source": source_type.lower(), 
                                "target":target_type.lower(),
                                "output_label": (
                                    output_label.lower() if output_label is not None else None
                                ),
                            }
                            out_str = edge_data_constructor(edge_type, source_type, target_type, label)
                            file.write(out_str + "\n")
                    elif isinstance(source_type, list) and isinstance(target_type, str):  # gene to pathway, expression_value edge schemas, physically interacts with...
                        for i in range(len(source_type)):
                            source_type[i] = source_type[i].lower()                        
                        # print(f"list-str:::key: => {k}")# \n{v}")
                        self.edge_node_types[label.lower()] = {
                            "target": target_type.lower(),
                            "output_label": (
                                output_label.lower() if output_label is not None else None
                            )
                        }                        
                        self.edge_node_types[label.lower()]["source"] = []                        
                        for t in source_type:
                            self.edge_node_types[label.lower()]["source"].append(t)
                            out_str = edge_data_constructor(edge_type, t, target_type, label)
                            file.write(out_str + "\n")
                    elif isinstance(source_type, str) and isinstance(target_type, list):  # expression edge schema
                        print(f"str-list:::key: => {k}")# \n{v}")
                        for i in range(len(target_type)):
                            target_type[i] = target_type[i].lower()                        
                        self.edge_node_types[label.lower()] = {
                            "source": source_type.lower(), 
                            "output_label": (
                                output_label.lower() if output_label is not None else None
                                )
                        } 
                        self.edge_node_types[label.lower()]["target"] = []
                        for t in target_type:
                            self.edge_node_types[label.lower()]["target"].append(t)
                            out_str = edge_data_constructor(edge_type, source_type, t, label)
                            file.write(out_str + "\n")

            elif v["represented_as"] == "node":
                label = v["input_label"]
                if not isinstance(label, list):
                    label = [label]

                label = [self.convert_input_labels(l) for l in label]
                node_type = self.convert_input_labels(k)
                for l in label:
                    out_str = node_data_constructor(node_type, l)
                    file.write(out_str + "\n")


    def write_nodes(self, nodes, path_prefix=None, create_dir=True):
        if path_prefix is not None:
            file_path = f"{self.output_path}/{path_prefix}/nodes.metta"
            if create_dir:
                if not os.path.exists(f"{self.output_path}/{path_prefix}"):
                    pathlib.Path(f"{self.output_path}/{path_prefix}").mkdir(parents=True, exist_ok=True)
        else:
            file_path = f"{self.output_path}/nodes.metta"
        
        with open(file_path, "a") as f:
            for node in nodes:
                self.extract_node_info(node) # Count nodes and extract node properties
                out_str = self.write_node(node)
                for s in out_str:
                    f.write(s + "\n")

            f.write("\n")

        logger.info("Finished writing out nodes")
        return self.node_freq, self.node_props



    def write_edges(self, edges, path_prefix=None, create_dir=True):
        if path_prefix is not None:
            file_path = f"{self.output_path}/{path_prefix}/edges.metta"
            if create_dir:
                if not os.path.exists(f"{self.output_path}/{path_prefix}"):
                    pathlib.Path(f"{self.output_path}/{path_prefix}").mkdir(parents=True, exist_ok=True)
        else:
            file_path = f"{self.output_path}/edges.metta"

        with open(file_path, "a") as f:
            for edge in edges:
                self.extract_edge_info(edge) # Count edges
                out_str = self.write_edge(edge)
                for s in out_str:
                    f.write(s + "\n")
            f.write("\n")
        return self.edge_freq


    def write_node(self, node):
        id, label, properties = node
        if "." in label:
            label = label.split(".")[1]
        def_out = f"({self.convert_input_labels(label)} {id})"
        return self.write_property(def_out, properties)


    def write_edge(self, edge):
        source_id, target_id, label, properties = edge
        label = label.lower()
        # added by saulo to handle list of types in the schema of edge's source
        if isinstance(source_id, tuple):
            source_type = source_id[0]
            if source_type not in self.edge_node_types[label]["source"]:
                raise TypeError(f"Type '{source_type}' must be one of {self.edge_node_types[label]['source']}")
            source_id = source_id[1]
        else:
            source_type = self.edge_node_types[label]["source"]
        # added by saulo to handle list of types in the schema of edge's target
        if isinstance(target_id, tuple):
            target_type = target_id[0]
            if target_type not in self.edge_node_types[label]["target"]:
                raise TypeError(f"Type {target_type} must be one of {self.edge_node_types[label]['target']}")
            target_id = target_id[1]
        else:
            target_type = self.edge_node_types[label]["target"]

        # source_type = self.edge_node_types[label]["source"]
        # target_type = self.edge_node_types[label]["target"]
        
        output_label = self.edge_node_types[label]["output_label"]
        if output_label is not None:
            label = output_label
        # added by saulo to handle lists of values with (probably) different types
        if isinstance(source_type, list):
            def_out = ""
            for a_source_type in source_type:
                def_out += f"({label} ({a_source_type} {source_id}) ({target_type} {target_id}))" + "\n"            
            def_out = def_out.rstrip('\n')            
        else:
            def_out = f"({label} ({source_type} {source_id}) ({target_type} {target_id}))"

        if source_type == "ontology_term":
            source_type = source_id.replace(':', '_').split('_')[0].lower()
        if target_type == "ontology_term":
            target_type = target_id.replace(':', '_').split('_')[0].lower()
        def_out = f"({label} ({source_type} {source_id}) ({target_type} {target_id}))"

        return self.write_property(def_out, properties)


    def write_property(self, def_out, property):
        out_str = [def_out]
        for k, v in property.items():
            if k in self.excluded_properties or v is None or v == "": continue
            if k == 'biological_context':
                try:
                    ontology_id = self.check_property(v).upper().replace('_', ':')
                    ontology_name = ontology_id.split(':')[0].lower()
                    out_str.append(f'({k} {def_out} ({ontology_name} {ontology_id}))')
                except Exception as e:
                    print(f"An error occurred while processing the biological context '{v}': {e}.")
                    continue
            elif isinstance(v, list):
                prop = "("
                for i, e in enumerate(v):
                    prop += f'{self.check_property(e)}'
                    if i != len(v) - 1: prop += " "
                prop += ")"
                out_str.append(f'({k} {def_out} {prop})')
            elif isinstance(v, dict):
                prop = f"({k} {def_out})"
                out_str.extend(self.write_property(prop, v))
            else:
                out_str.append(f'({k} {def_out} {self.check_property(v)})')
        return out_str

    def check_property(self, prop):
        if isinstance(prop, str):
            if " " in prop:
                prop = prop.replace(" ", "_").strip("_")

            special_chars = ["(", ")"]
            escape_char = "\\"
            return "".join(escape_char + c if c in special_chars or c == escape_char else c for c in prop)

        return prop

    def convert_input_labels(self, label, replace_char="_"):
        """
        A method that removes spaces in input labels and replaces them with replace_char
        :param label: Input label of a node or edge
        :param replace_char: the character to replace spaces with
        :return:
        """
        if isinstance(label, list):
            labels = []
            for aLabel in label:
                labels.append(aLabel.replace(" ", "_"))
            return labels        
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


        