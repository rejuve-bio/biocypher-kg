# Author Abdulrahman S. Omar <xabush@singularitynet.io>
import pathlib
import os
from biocypher._logger import logger
import networkx as nx
from collections import Counter, defaultdict

from biocypher_metta import BaseWriter

class MeTTaWriter(BaseWriter):

    def __init__(self, schema_config, biocypher_config,
                 output_dir):
        super().__init__(schema_config, biocypher_config, output_dir)
        
        # Initialize edge node types for tuple handling
        self.edge_node_types = {}
        
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
                node = self.normalize_text(node)
                ancestor = self.normalize_text(ancestor)
                if ancestor == node:
                    f.write(f"(: {node.upper()} Type)\n")
                else:
                    f.write(f"(<: {node.upper()} {ancestor.upper()})\n")

            self.create_data_constructors(f)

        logger.info("Type hierarchy created successfully.")

    def create_data_constructors(self, file):
        schema = self.bcy._get_ontology_mapping()._extend_schema()
        
        def edge_data_constructor(edge_type, source_types, target_types, label):
            # Handle both single types and lists of types
            if isinstance(source_types, list):
                source_str = ' '.join([st.upper() for st in source_types])
            else:
                source_str = source_types.upper()
                
            if isinstance(target_types, list):
                target_str = ' '.join([tt.upper() for tt in target_types])
            else:
                target_str = target_types.upper()
                
            return f"(: {label.lower()} (-> {source_str} {target_str} {edge_type.upper()}))"

        def node_data_constructor(node_type, node_label):
            return f"(: {node_label.lower()} (-> $x {node_type.upper()}))"

        for k, v in schema.items():
            if v["represented_as"] == "edge":
                edge_type = self.normalize_text(k)
                source_type = v.get("source", None)
                target_type = v.get("target", None)
        
                if source_type is not None and target_type is not None:
                    label = self.normalize_text(v["input_label"])
                    source_type_normalized = self.normalize_text(source_type)
                    target_type_normalized = self.normalize_text(target_type)
            
                    output_label = v.get("output_label", None)

                    if '.' not in k:
                        out_str = edge_data_constructor(edge_type, source_type_normalized, target_type_normalized, label)
                        file.write(out_str + "\n")
                
                        self.edge_node_types[label] = {
                            "source": source_type_normalized, 
                            "target": target_type_normalized,
                            "output_label": output_label
                        }

            elif v["represented_as"] == "node":
                label = self.normalize_text(v["input_label"])
                node_type = self.normalize_text(k)
        
                # Handle both single labels and lists
                if isinstance(label, list):
                    labels_to_process = label
                else:
                    labels_to_process = [label]
            
                for l in labels_to_process:
                    out_str = node_data_constructor(node_type, l)
                    file.write(out_str + "\n")

    def preprocess_id(self, prev_id):
        """Ensure ID remains in CURIE format while cleaning special characters"""
        prev_id = str(prev_id)
        
        if ':' in prev_id:
            prefix, local_id = prev_id.split(':', 1)
            prefix = prefix.upper()
            clean_local = local_id.lower().replace(f"{prefix.lower()}_", "")
            clean_local = clean_local.strip().translate(str.maketrans({' ': '_'}))
            return f"{prefix}:{clean_local}"
        return prev_id.lower().strip().translate(str.maketrans({' ': '_', ':': '_'}))

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
                self.extract_node_info(node)  # Count nodes and extract node properties
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
                self.extract_edge_info(edge)  # Count edges
                out_str = self.write_edge(edge)
                for s in out_str:
                    f.write(s + "\n")
            f.write("\n")
        return self.edge_freq

    def write_node(self, node):
        id, label, properties = node
        id = self.preprocess_id(str(id))  
        if "." in label:
            label = label.split(".")[1]
        def_out = f"({self.normalize_text(label)} {id})"
        return self.write_property(def_out, properties)

    def write_edge(self, edge):
        source_id, target_id, label, properties = edge
        source_id_processed = source_id
        target_id_processed = target_id
        label = label.lower()
        
        if isinstance(source_id, tuple):
            source_type = source_id[0]
            source_id_processed = self.preprocess_id(str(source_id[1]))  
            if label in self.edge_node_types:
                valid_source_types = self.edge_node_types[label]["source"]
                if isinstance(valid_source_types, list):
                    if source_type not in valid_source_types:
                        raise TypeError(f"Type '{source_type}' must be one of {valid_source_types}")
                else:
                    if source_type != valid_source_types:
                        raise TypeError(f"Type '{source_type}' must be '{valid_source_types}'")
        else:
            source_id_processed = self.preprocess_id(str(source_id)) 
            if label in self.edge_node_types:
                source_type_info = self.edge_node_types[label]["source"]
                if isinstance(source_type_info, list):
                    source_type = source_type_info[0] 
                else:
                    source_type = source_type_info
            else:
                source_type = "unknown"

        if isinstance(target_id, tuple):
            target_type = target_id[0]
            target_id_processed = self.preprocess_id(str(target_id[1])) 
            if label in self.edge_node_types:
                valid_target_types = self.edge_node_types[label]["target"]
                if isinstance(valid_target_types, list):
                    if target_type not in valid_target_types:
                        raise TypeError(f"Type '{target_type}' must be one of {valid_target_types}")
                else:
                    if target_type != valid_target_types:
                        raise TypeError(f"Type '{target_type}' must be '{valid_target_types}'")
        else:
            target_id_processed = self.preprocess_id(str(target_id))  
            if label in self.edge_node_types:
                target_type_info = self.edge_node_types[label]["target"]
                if isinstance(target_type_info, list):
                    target_type = target_type_info[0]  
                else:
                    target_type = target_type_info
            else:
                target_type = "unknown"

        output_label = None
        if label in self.edge_node_types and self.edge_node_types[label]["output_label"] is not None:
            output_label = self.edge_node_types[label]["output_label"]
            label_to_use = output_label
        else:
            label_to_use = label

        if source_type == "ontology_term":
            source_type = source_id_processed.replace(':', '_').split('_')[0].lower()
        if target_type == "ontology_term":
            target_type = target_id_processed.replace(':', '_').split('_')[0].lower()

        if isinstance(source_type, list):
            def_out = ""
            for a_source_type in source_type:
                def_out += f"({label_to_use} ({a_source_type} {source_id_processed}) ({target_type} {target_id_processed}))" + "\n"
            def_out = def_out.rstrip('\n')
        else:
            def_out = f"({label_to_use} ({source_type} {source_id_processed}) ({target_type} {target_id_processed}))"

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
                    if isinstance(e, tuple):
                        tuple_prop = '('
                        for el in e:
                            tuple_prop += f'{self.check_property(el)} '
                        tuple_prop = tuple_prop.rstrip()
                        prop += tuple_prop + ')'
                    else:
                        prop += f'{self.check_property(e)}'
                    if i != len(v) - 1: 
                        prop += " "
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
            if "" in prop:
                prop = prop.replace(" ", "_").strip("_")
            if '->' in prop:
                prop = prop.replace('->', '-\\>')

            special_chars = ["(", ")"]
            escape_char = "\\"
            return "".join(escape_char + c if c in special_chars or c == escape_char else c for c in prop)
        
        return str(prop)

    def normalize_text(self, label, replace_char="_", lowercase=True):
        if isinstance(label, list):
            labels = []
            for aLabel in label:
                processed = aLabel.replace(" ", replace_char)
                labels.append(processed.lower() if lowercase else processed)
            return labels
        processed = label.replace(" ", replace_char)
        return processed.lower() if lowercase else processed

    def get_parent(self, G, node):
        """
        Get the immediate parent of a node in the ontology.
        """
        return nx.dfs_preorder_nodes(G, node, depth_limit=2)

    def show_ontology_structure(self):
        self.bcy.show_ontology_structure()

    def summary(self):
        self.bcy.summary()
