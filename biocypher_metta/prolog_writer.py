# Author Abdulrahman S. Omar <xabush@singularitynet.io>
import pathlib
from biocypher._logger import logger
import networkx as nx
import re

from biocypher_metta import BaseWriter

class PrologWriter(BaseWriter):

    # Prefixes that represent ontology terms — keep prefix joined with '_'
    # All other prefixes (ENSEMBL, HGNC, STRING, …) are stripped.
    _ONTOLOGY_PREFIXES = frozenset({
        'CL', 'UBERON', 'CLO', 'EFO', 'BTO', 'GO', 'HP', 'MONDO', 'DOID',
        'CHEBI', 'NCBITAXON', 'OBI', 'PATO', 'SO', 'RO', 'IAO',
    })

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
        """
        Strip CURIE prefixes for non-ontology IDs (e.g. ENSEMBL:ENSG… → ENSG…),
        matching MeTTa writer behaviour.  Ontology prefixes (CL, UBERON, …) are
        kept joined with '_' so that downstream normalize_text produces the
        expected form (e.g. CL_0000136 → cl_0000136).
        """
        if prev_id is None:
            return None
        if ':' in prev_id:
            prefix, local_id = prev_id.split(':', 1)
            prefix_upper = prefix.strip().upper()
            local_id = local_id.strip().translate(str.maketrans({' ': '_'}))
            if prefix_upper in self._ONTOLOGY_PREFIXES:
                return f"{prefix_upper}_{local_id.upper()}"
            # Non-ontology prefix — strip it (type is already in the Prolog functor)
            return local_id
        return prev_id.strip().translate(str.maketrans({' ': '_'}))


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
            output_dir = f"{self.output_path}/{path_prefix}"
            if create_dir:
                pathlib.Path(output_dir).mkdir(parents=True, exist_ok=True)
        else:
            output_dir = self.output_path

        file_handles = {}

        try:
            for node in nodes:
                id, label, properties = node
                if not self.check_node_label(label):
                    raise ValueError(f"Invalid node label: {label}. This label is not defined in the schema configuration. Please check your adapter or schema config.")
                self.extract_node_info(node)

                if "." in label:
                    label = label.split(".")[-1]
                label = label.lower()

                if label not in file_handles:
                    file_path = f"{output_dir}/nodes.pl"
                    file_handles[label] = open(file_path, "w")

                out_str = self.write_node(node)
                for s in out_str:
                    file_handles[label].write(s + "\n")

        finally:
            for fh in file_handles.values():
                try:
                    fh.write("\n")
                    fh.close()
                except Exception:
                    pass

        logger.info("Finished writing out nodes")
        return self.node_freq, self.node_props

    def write_edges(self, edges, path_prefix=None, create_dir=True):
        if path_prefix is not None:
            output_dir = f"{self.output_path}/{path_prefix}"
            if create_dir:
                pathlib.Path(output_dir).mkdir(parents=True, exist_ok=True)
        else:
            output_dir = self.output_path

        file_handles = {}

        try:
            for edge in edges:
                source_id, target_id, label, properties = edge
                if not self.check_edge_label(label):
                    raise ValueError(f"Invalid edge label: {label}. This label is not defined in the schema configuration. Please check your adapter or schema config.")
                self.extract_edge_info(edge)

                label = label.lower()
                if label in self.edge_node_types and self.edge_node_types[label]["output_label"] is not None:
                    label_to_use = self.edge_node_types[label]["output_label"]
                else:
                    label_to_use = label

                edge_info = self.edge_node_types.get(label, {})
                source_type = edge_info.get("source", "unknown")
                target_type = edge_info.get("target", "unknown")

                if isinstance(source_type, list):
                    source_type = source_type[0]
                if isinstance(target_type, list):
                    target_type = target_type[0]

                file_key = (label, source_type, target_type)

                if file_key not in file_handles:
                    file_suffix = f"{source_type}_{label_to_use}_{target_type}"
                    file_path = f"{output_dir}/edges.pl"
                    file_handles[file_key] = open(file_path, "w")

                out_str = self.write_edge(edge)
                for s in out_str:
                    file_handles[file_key].write(s + "\n")

        finally:
            for fh in file_handles.values():
                try:
                    fh.write("\n")
                    fh.close()
                except Exception:
                    pass

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
                if v is None or v == "":
                    continue
                try:
                    ontology_id = v.upper().replace('_', ':')
                    ontology_prefix = ontology_id.split(':')[0].lower()
                    ontology_dict = {'cl': 'cell_type', 'uberon': 'anatomy', 'clo': 'cell_line', 'efo': 'experimental_factor', 'bto': 'tissue'}
                    ontology_name = ontology_dict.get(ontology_prefix, ontology_prefix)
                    prop = self.normalize_text(ontology_id)
                    out_str.append(f'{k}({def_out}, {ontology_name}({prop})).')
                except Exception as e:
                    print(f"An error occurred while processing the biological context '{v}': {e}.")
                    continue
            elif isinstance(v, list):
                for item in v:
                    if isinstance(item, tuple):
                        tuple_str = "("
                        for el in item:
                            tuple_str += f'{self.normalize_text(el)}, '
                        tuple_str = tuple_str.rstrip(", ") + ")"
                        out_str.append(f'{k}({def_out}, {tuple_str}).')
                    elif isinstance(item, dict):
                        for sub_k, sub_v in item.items():
                            if isinstance(sub_v, list):
                                for sub_item in sub_v:
                                    prop = self.normalize_text(sub_item)
                                    if prop is not None:
                                        out_str.append(f'{sub_k}({def_out}, {prop}).')
                            else:
                                prop = self.normalize_text(sub_v)
                                if prop is not None:
                                    out_str.append(f'{sub_k}({def_out}, {prop}).')
                    else:
                        prop = self.normalize_text(item)
                        if prop is not None:
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