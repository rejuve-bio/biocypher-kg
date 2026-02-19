# Author Abdulrahman S. Omar <xabush@singularitynet.io>
import pathlib
import os
from biocypher._logger import logger
import networkx as nx
import re

from biocypher_metta import BaseWriter

class PrologWriter(BaseWriter):

    def __init__(self, schema_config, biocypher_config,
                 output_dir):
        super().__init__(schema_config, biocypher_config, output_dir)
        self.create_edge_types()
        #self.excluded_properties = ["license", "version", "source"]
        self.excluded_properties = []
        self.type_hierarchy = self._type_hierarchy()


    def create_edge_types(self):
        schema = self.bcy._get_ontology_mapping()._extend_schema()
        self.edge_node_types = {}

        for k, v in schema.items():
            if v["represented_as"] == "edge":
                source_type = v.get("source", None)
                target_type = v.get("target", None)
            
                if source_type is not None and target_type is not None:
                    label = self.normalize_text(v["input_label"])
                    source_type_normalized = self.normalize_text(source_type)
                    target_type_normalized = self.normalize_text(target_type)
                
                    output_label = v.get("output_label", None)

                    if '.' not in k:
                        self.edge_node_types[label] = {
                            "source": source_type_normalized, 
                            "target": target_type_normalized,
                            "output_label": output_label
                        }

    def preprocess_id(self, prev_id):
        """Ensure ID remains in CURIE format while cleaning special characters"""
        if prev_id is None:
            return None
        if ':' in prev_id:
            prefix, local_id = prev_id.split(':', 1)
            prefix = prefix.upper()
            # Clean local ID (remove duplicate prefix if present)
            clean_local = local_id.lower().replace(f"{prefix.lower()}_", "")
            clean_local = clean_local.strip().translate(str.maketrans({' ': '_'}))
            return f"{prefix}:{clean_local}"
        return prev_id.lower().strip().translate(str.maketrans({' ': '_', ':': '_'}))


    def _type_hierarchy(self):
        # to use Biolink-compatible schema
        # to not use  ontologies names but the ontologies types if their IDs occur  in edge's source/target
        return {
            'biolink:biologicalprocessoractivity': frozenset({'pathway', 'reaction'}),
            'pathway': frozenset({'pathway'}),
            'reaction': frozenset({'reaction'}),
            'biolink:geneorgeneproduct': frozenset({'gene', 'transcript', 'protein'}),
            'gene': frozenset({'gene'}),
            'transcript': frozenset({'transcript'}),
            'protein': frozenset({'protein'}),
            'snp': frozenset({'snp'}),
            'phenotype_set': frozenset({'phenotype_set'}),

            'ontology_term': frozenset({'ontology_term', 'anatomy', 'developmental_stage', 'cell_type', 'cell_line', 'small_molecule', 'experimental_factor', 'phenotype', 'disease', 'sequence_type', 'tissue', }),
            'anatomy': frozenset({'anatomy'}),
            'developmental_stage': frozenset({'developmental_stage'}),
            'cell_type': frozenset({'cell_type'}),
            'cell_line': frozenset({'cell_line'}),
            'experimental_factor': frozenset({'experimental_factor'}),
            'phenotype': frozenset({'phenotype'}),
            'disease': frozenset({'disease'}),
            'sequence_type': frozenset({'sequence_type'}),
            'small_molecule': frozenset({'small_molecule'}),
            'biological_process': frozenset({'biological_process'}),
            'molecular_function': frozenset({'molecular_function'}),
            'cellular_component': frozenset({'cellular_component'}),
            'tissue': frozenset({'tissue'}),
        }

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
                self.extract_node_info(node)
                out_str = self.write_node(node)
                for s in out_str:
                    f.write(s + "\n")

            f.write("\n")

        logger.info("Finished writing out nodes")
        return self.node_freq, self.node_props

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
                self.extract_edge_info(edge)
                out_str = self.write_edge(edge)
                for s in out_str:
                    f.write(s + "\n")

            f.write("\n")
        return self.edge_freq

    def write_node(self, node):
        id, label, properties = node
        id = self.preprocess_id(id)  # Added ID preprocessing
        if "." in label:
            label = label.split(".")[1]
        label = label.lower()
        id = self.normalize_text(id.lower())
        def_out = f"{self.normalize_text(label)}({id})"
        return self.write_property(def_out, properties)

    def write_edge(self, edge):
        source_id, target_id, label, properties = edge
        source_id_processed = source_id
        target_id_processed = target_id
        label = label.lower()
        
        if isinstance(source_id, tuple):
            source_type = source_id[0]
            source_id_processed = self.preprocess_id(source_id[1])
            if source_id_processed is None:
                logger.warning(f"Edge '{label}': skipping because source ID is None")
                return []
            if label in self.edge_node_types:
                valid_source_types = self.edge_node_types[label]["source"]
                if isinstance(valid_source_types, list):
                    if source_type not in self.type_hierarchy:
                        raise TypeError(f"Type '{source_type}' must be one of {valid_source_types}")
                else:
                    if source_type not in self.type_hierarchy:
                        raise TypeError(f"Type '{source_type}' must be '{valid_source_types}'")

            # if label in self.edge_node_types:
            #     valid_source_types = self.edge_node_types[label]["source"]
            #     if isinstance(valid_source_types, list):
            #         if source_type not in valid_source_types:
            #             raise TypeError(f"Type '{source_type}' must be one of {valid_source_types}")
            #     else:
            #         if source_type != valid_source_types:
            #             raise TypeError(f"Type '{source_type}' must be '{valid_source_types}'")
        else:
            source_id_processed = self.preprocess_id(source_id)
            if source_id_processed is None:
                logger.warning(f"Edge '{label}': skipping because source ID is None")
                return []
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
            target_id_processed = self.preprocess_id(target_id[1])
            if target_id_processed is None:
                logger.warning(f"Edge '{label}': skipping because target ID is None")
                return []
            if label in self.edge_node_types:
                valid_source_types = self.edge_node_types[label]["source"]
                if isinstance(valid_source_types, list):
                    if source_type not in self.type_hierarchy:
                        raise TypeError(f"Type '{source_type}' must be one of {valid_source_types}")
                else:
                    if source_type not in self.type_hierarchy:
                        raise TypeError(f"Type '{source_type}' must be '{valid_source_types}'")

            # if label in self.edge_node_types:
            #     valid_target_types = self.edge_node_types[label]["target"]
            #     if isinstance(valid_target_types, list):
            #         if target_type not in valid_target_types:
            #             raise TypeError(f"Type '{target_type}' must be one of {valid_target_types}")
            #     else:
            #         if target_type != valid_target_types:
            #             raise TypeError(f"Type '{target_type}' must be '{valid_target_types}'")
        else:
            target_id_processed = self.preprocess_id(target_id)
            if target_id_processed is None:
                logger.warning(f"Edge '{label}': skipping because target ID is None")
                return []
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
            source_type = source_id_processed.split('_')[0]
        if target_type == "ontology_term":
            target_type = target_id_processed.split('_')[0]
        
        source_id_processed = self.normalize_text(source_id_processed)
        target_id_processed = self.normalize_text(target_id_processed)
        label_to_use = self.normalize_text(label_to_use)
        
        def_out = f"{label_to_use}({source_type}({source_id_processed}), {target_type}({target_id_processed}))"
        return self.write_property(def_out, properties)


    def write_property(self, def_out, property):
        out_str = [f"{def_out}."]
        for k, v in property.items():
            if k in self.excluded_properties or v is None or v == "": continue
            if k == 'biological_context':
                try:
                    prop = self.normalize_text(v)
                    ontology = prop.split('_')[0]
                    out_str.append(f'{k}({def_out}, {ontology}({prop})).')
                except Exception as e:
                    print(f"An error occurred while processing the biological context '{v}': {e}.")
                    continue
            elif isinstance(v, list):
                prop = "["
                for i, e in enumerate(v):
                    prop += f'{self.normalize_text(e)}'
                    if i != len(v) - 1: prop += ","
                prop += "]"
                out_str.append(f'{k}({def_out}, {prop}).')
            elif isinstance(v, dict):
                prop = f"{k}({def_out})."
                out_str.extend(self.write_property(prop, v))
            else:
                prop = self.normalize_text(v)
                if prop is not None:
                    out_str.append(f'{k}({def_out}, {prop}).')
        return out_str

    def normalize_text(self, prop):
        replace_chars = {
            " ": "_",
            "-": "_",
            ":": "_",
            "/": "_",
            "–": "_",  # en dash
            "—": "_",  # em dash
            "&": "_",
            ";": ","
        }
        
        if isinstance(prop, str):        
            for char, replacement in replace_chars.items():
                prop = prop.replace(char, replacement).lower()     

            # sanitizes each string separated by comma ','
            if "," in prop:
                prop = ",".join([self.normalize_text(p) for p in prop.split(',') if self.normalize_text(p) not in ["", None]])
                return prop if prop != "" else None
            
            prop = re.sub(r'[^\w_,]', '', prop) # removes special characters except for underscores "_" and comma ","
            prop = re.sub(r"_+", "_", prop) # removes multiple adjacent under scores '_'
            prop.strip("_")
            if prop == "":
                return None
            try:
                float(prop)
                return prop # It's a numeric string, return as is
            except ValueError:
                # Check if the first character is a digit
                if prop[0].isdigit():
                    return f"'{prop}'"
        elif isinstance(prop, list):
            for i in range(len(prop)):
                prop[i] = self.normalize_text(prop[i])
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