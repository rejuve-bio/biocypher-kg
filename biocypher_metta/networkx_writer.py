import pickle
from pathlib import Path
from collections import defaultdict
import networkx as nx
from biocypher._logger import logger
from typing import Optional, Dict, List, Tuple
from biocypher_metta import BaseWriter

class NetworkXWriter(BaseWriter):
    def __init__(self, schema_config, biocypher_config, output_dir):
        super().__init__(schema_config, biocypher_config, output_dir)
        self.graph = nx.Graph()
        self.node_id_counter = 0
        self.node_mapping = {}  
        self.node_counters = defaultdict(int)
        self.edge_counters = defaultdict(int)

    def _preprocess_id(self, node_id: str) -> str:
        if isinstance(node_id, (tuple, list)) and len(node_id) >= 2:
            node_id = node_id[1]
        
        return str(node_id).lower().strip().translate(str.maketrans({' ': '_', ':': '_'}))

    def _get_or_create_node_id(self, original_id: str) -> int:
        if original_id not in self.node_mapping:
            self.node_mapping[original_id] = self.node_id_counter
            self.node_id_counter += 1
        return self.node_mapping[original_id]

    def write_nodes(self, nodes: List[Tuple], path_prefix: Optional[str] = None, 
                   adapter_name: Optional[str] = None) -> Tuple[Dict[str, int], Dict[str, set]]:
        node_headers = {'all_nodes': {'id', 'label'}}
        
        for node in nodes:
            original_id, label, _ = node  
            label = label.lower() 
            
            clean_id = self._preprocess_id(original_id)
            node_id = self._get_or_create_node_id(clean_id)
            
            self.graph.add_node(node_id, id=clean_id, label=label)
            self.node_counters[label] += 1
        
        return dict(self.node_counters), node_headers

    def write_edges(self, edges: List[Tuple], path_prefix: Optional[str] = None, 
                   adapter_name: Optional[str] = None) -> Dict[str, int]:
        for edge in edges:
            source_id, target_id, label, _ = edge  
            label = label.lower()  
            
            source_clean = self._preprocess_id(source_id)
            target_clean = self._preprocess_id(target_id)
            
            try:
                source_node_id = self.node_mapping[source_clean]
                target_node_id = self.node_mapping[target_clean]
                
                self.graph.add_edge(source_node_id, target_node_id, 
                                   type=label, weight=1.0)
                
                self.edge_counters[label] += 1
                
            except KeyError:
                logger.warning(f"Source node {source_clean} not found in node mapping for edge type {label}")
                continue
        
        return dict(self.edge_counters)

    def write_graph(self, path_prefix: Optional[str] = None, 
                   adapter_name: Optional[str] = None) -> Path:
        output_dir = self.get_output_path(path_prefix, adapter_name)
        output_dir.mkdir(parents=True, exist_ok=True)
        
        pkl_path = output_dir / "networkx_graph.pkl"
        
        data_to_save = {
            'nodes': list(self.graph.nodes(data=True)),
            'edges': list(self.graph.edges(data=True))
        }
        
        with open(pkl_path, 'wb') as f:
            pickle.dump(data_to_save, f, protocol=4)
        
        logger.info(
            f"Saved compatible graph with {len(data_to_save['nodes'])} nodes "
            f"and {len(data_to_save['edges'])} edges to {pkl_path}"
        )
        return pkl_path

    def get_output_path(self, prefix: Optional[str] = None, 
                       adapter_name: Optional[str] = None) -> Path:
        if prefix:
            return self.output_path / prefix
        elif adapter_name:
            return self.output_path / adapter_name
        return self.output_path
        
    def clear_counts(self):
        pass