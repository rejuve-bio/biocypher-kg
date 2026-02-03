import pickle
from pathlib import Path
from collections import defaultdict
import networkx as nx
from biocypher._logger import logger
from typing import Optional, Dict, List, Tuple, Union
from biocypher_metta import BaseWriter

class NetworkXWriter(BaseWriter):
    def __init__(self, schema_config, biocypher_config, output_dir, directed=True):
        super().__init__(schema_config, biocypher_config, output_dir)
        self.directed = directed
        self.graph = nx.DiGraph() if directed else nx.Graph()
        self.node_id_counter = 0
        self.node_mapping = {}  
        self.node_counters = defaultdict(int)
        self.edge_counters = defaultdict(int)
        self.create_edge_types()

    def create_edge_types(self):
        schema = self.bcy._get_ontology_mapping()._extend_schema()
        self.edge_node_types = {}

        for k, v in schema.items():
            if v["represented_as"] == "edge":
                source_type = v.get("source", None)
                target_type = v.get("target", None)

                if source_type is not None and target_type is not None:
                    if isinstance(v["input_label"], list):
                        label = self.convert_input_labels(v["input_label"][0])
                    else:
                        label = self.convert_input_labels(v["input_label"])
                    
                    if isinstance(source_type, list):
                        processed_source = [self.convert_input_labels(st).lower() for st in source_type]
                    else:
                        processed_source = [self.convert_input_labels(source_type).lower()]
                    
                    if isinstance(target_type, list):
                        processed_target = [self.convert_input_labels(tt).lower() for tt in target_type]
                    else:
                        processed_target = [self.convert_input_labels(target_type).lower()]
                    
                    output_label = v.get("output_label", label)
                    if isinstance(output_label, list):
                        processed_output_label = self.convert_input_labels(output_label[0]).lower()
                    else:
                        processed_output_label = self.convert_input_labels(output_label).lower() if output_label else None

                    label_lower = label.lower()
                    if label_lower not in self.edge_node_types:
                        self.edge_node_types[label_lower] = {
                            "source": set(processed_source),
                            "target": set(processed_target),
                            "output_label": processed_output_label
                        }
                    else:
                        self.edge_node_types[label_lower]["source"].update(processed_source)
                        self.edge_node_types[label_lower]["target"].update(processed_target)

    def convert_input_labels(self, label):
        if isinstance(label, list):
            return [item.lower().replace(" ", "_") for item in label]
        return label.lower().replace(" ", "_")

    def _preprocess_id(self, node_id: Union[str, tuple, list]) -> str:
        if isinstance(node_id, (tuple, list)) and len(node_id) >= 2:
            node_id = node_id[1]
        
        id_str = str(node_id).lower().strip()
        
        if ':' in id_str:
            id_str = id_str.split(':', 1)[1]  
        
        return id_str.replace(' ', '_')

    def _get_or_create_node_id(self, original_id: str) -> int:
        if original_id not in self.node_mapping:
            self.node_mapping[original_id] = self.node_id_counter
            self.node_id_counter += 1
        return self.node_mapping[original_id]

    def _extract_name_properties(self, properties: Dict) -> Dict:
        name_properties = {}
        if properties:
            for key, value in properties.items():  
                if 'name' in key.lower():
                    name_properties[key] = value
        return name_properties

    def _get_edge_type_info(self, label: str, source_id: str, target_id: str) -> Tuple[str, str]:
        edge_info = self.edge_node_types.get(label.lower(), {})
        source_type_info = edge_info.get("source", "unknown")
        target_type_info = edge_info.get("target", "unknown")
        
        if isinstance(source_type_info, list):
            source_type = source_type_info[0] if source_type_info else "unknown"
        else:
            source_type = source_type_info
            
        if isinstance(target_type_info, list):
            target_type = target_type_info[0] if target_type_info else "unknown"
        else:
            target_type = target_type_info
            
        return source_type, target_type

    def write_nodes(self, nodes: List[Tuple], path_prefix: Optional[str] = None, 
                   adapter_name: Optional[str] = None) -> Tuple[Dict[str, int], Dict[str, set]]:
        node_headers = {'all_nodes': {'id', 'label'}}
        
        id_patterns = defaultdict(int)
        sample_ids = []
        
        for i, node in enumerate(nodes):
            self.extract_node_info(node)  
            original_id, label, properties = node  
            
            if i < 10:
                sample_ids.append(str(original_id))
            
            id_str = str(original_id).lower()
            if ':' in id_str:
                prefix = id_str.split(':', 1)[0]
                id_patterns[f"prefix_{prefix}"] += 1
            else:
                id_patterns["no_prefix"] += 1
            
            if "." in label:
                label = label.split(".")[1]
            label = label.lower()
            
            clean_id = self._preprocess_id(original_id)
            node_id = self._get_or_create_node_id(clean_id)
            
            node_attrs = {
                'id': clean_id,
                'label': label,
            }
            
            if properties:
                name_properties = self._extract_name_properties(properties)
                node_attrs.update(name_properties)
                
                if name_properties:
                    node_headers['all_nodes'].update(name_properties.keys())
            
            self.graph.add_node(node_id, **node_attrs)
            self.node_counters[label] += 1
        
        logger.info(f"Node ID patterns found: {dict(id_patterns)}")
        logger.info(f"Sample node IDs: {sample_ids[:5]}")
        logger.info(f"Sample processed node IDs: {[self._preprocess_id(id) for id in sample_ids[:5]]}")
        
        return dict(self.node_counters), node_headers

    def write_edges(self, edges: List[Tuple], path_prefix: Optional[str] = None, 
                   adapter_name: Optional[str] = None) -> Dict[str, int]:
        edges_added = 0
        edges_skipped = 0
        
        for edge in edges:
            self.extract_edge_info(edge)  
            source_id, target_id, label, properties = edge  
            label = label.lower()
            
            edge_info = self.edge_node_types.get(label, {})
            edge_label = edge_info.get("output_label") or label
            
            if isinstance(source_id, tuple):
                source_clean = self._preprocess_id(source_id[1])
                source_type = source_id[0]
            else:
                source_clean = self._preprocess_id(source_id)
                source_type, _ = self._get_edge_type_info(label, source_id, target_id)
            
            if isinstance(target_id, tuple):
                target_clean = self._preprocess_id(target_id[1])
                target_type = target_id[0]
            else:
                target_clean = self._preprocess_id(target_id)
                _, target_type = self._get_edge_type_info(label, source_id, target_id)
            
            # Type validation using hierarchy
            if label in self.edge_node_types:
                edge_info = self.edge_node_types[label]
                
                # Validate source
                valid_source_types = edge_info.get("source")
                allowed_source_types = set()
                for vt in valid_source_types:
                    allowed_source_types.update(self.type_hierarchy.get(vt, {vt}))
                
                if source_type not in allowed_source_types:
                    raise TypeError(f"Type '{source_type}' for source of '{label}' must be one of {allowed_source_types}")
                
                # Validate target
                valid_target_types = edge_info.get("target")
                allowed_target_types = set()
                for tt in valid_target_types:
                    allowed_target_types.update(self.type_hierarchy.get(tt, {tt}))
                
                if target_type not in allowed_target_types:
                    raise TypeError(f"Type '{target_type}' for target of '{label}' must be one of {allowed_target_types}")
            
            if edges_skipped < 5: 
                if source_clean not in self.node_mapping or target_clean not in self.node_mapping:
                    logger.info(f"DEBUG - Edge ID mismatch for edge type '{label}':")
                    logger.info(f"  Original source: {source_id} -> Cleaned: {source_clean}")
                    logger.info(f"  Original target: {target_id} -> Cleaned: {target_clean}")
                    logger.info(f"  Source in mapping: {source_clean in self.node_mapping}")
                    logger.info(f"  Target in mapping: {target_clean in self.node_mapping}")
                    sample_nodes = list(self.node_mapping.keys())[:3]
                    logger.info(f"  Sample node IDs in mapping: {sample_nodes}")
            
            try:
                source_node_id = self.node_mapping[source_clean]
                target_node_id = self.node_mapping[target_clean]
                
                edge_attrs = {
                    'type': edge_label,  
                    'input_label': label,  
                    'weight': 1.0, 
                    'source': source_clean, 
                    'target': target_clean,
                    'source_type': source_type,
                    'target_type': target_type
                }
                
                if properties:
                    name_properties = self._extract_name_properties(properties)
                    edge_attrs.update(name_properties)
                
                if self.directed:
                    edge_attrs['directed'] = True
                    edge_attrs['direction'] = f"{source_clean} -> {target_clean}"
                else:
                    edge_attrs['directed'] = False
                    edge_attrs['direction'] = f"{source_clean} -- {target_clean}"
                
                self.graph.add_edge(source_node_id, target_node_id, **edge_attrs)
                
                edge_key = f"{label}|{source_type}|{target_type}"
                self.edge_counters[edge_key] += 1
                edges_added += 1
                
            except KeyError:
                edges_skipped += 1
                if edges_skipped <= 10: 
                    logger.warning(f"Source node {source_clean} or target node {target_clean} not found in node mapping for edge type {label}")
        
        logger.info(f"Edge processing summary: {edges_added} edges added, {edges_skipped} edges skipped due to missing nodes")
        return dict(self.edge_counters)

    def write_graph(self, path_prefix: Optional[str] = None, 
                   adapter_name: Optional[str] = None) -> Path:
        # Save the NetworkX graph to a pickle file
        output_dir = self.get_output_path(path_prefix, adapter_name)
        output_dir.mkdir(parents=True, exist_ok=True)
        
        pkl_path = output_dir / "networkx_graph.pkl"
        
        with open(pkl_path, 'wb') as f:
            pickle.dump(self.graph, f, protocol=4)
        
        graph_type = "directed" if self.directed else "undirected"
        logger.info(
            f"Saved {graph_type} graph with {len(self.graph.nodes)} nodes "
            f"and {len(self.graph.edges)} edges to {pkl_path}"
        )
        logger.info(f"Graph is directed: {self.graph.is_directed()}")
        
        return pkl_path

    def get_output_path(self, prefix: Optional[str] = None, 
                       adapter_name: Optional[str] = None) -> Path:
        # Get the output path for saving files
        if prefix:
            return self.output_path / prefix
        elif adapter_name:
            return self.output_path / adapter_name
        return self.output_path
        
    def clear_counts(self):
        # Clear/reset any internal counters
        pass