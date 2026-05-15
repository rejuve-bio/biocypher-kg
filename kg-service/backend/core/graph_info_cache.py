import json
import logging
from pathlib import Path
from datetime import datetime
from typing import Optional
from backend.core.neo4j_client import neo4j_client

logger = logging.getLogger(__name__)

class GraphInfoCache:
    def __init__(self, cache_file: str = "/mnt/hdd_1/abdu_md/kg-service/annotation-dashboard/neo4j_graph_info.json"):
        """
        Initialize graph info cache.
        
        Args:
            cache_file: Path where the JSON file will be saved
        """
        self.cache_file = Path(cache_file)
        self.last_generated: Optional[datetime] = None
        
    def generate_graph_info(self) -> dict:
        """Generate complete graph info from Neo4j"""
        logger.info("Generating graph_info.json...")
        
        try:
            # Basic counts
            logger.info("  → Fetching node and edge counts...")
            counts = neo4j_client.get_total_counts()
            logger.info(f"  ✓ Found {counts['node_count']:,} nodes, {counts['edge_count']:,} edges")
            
            # Distributions
            logger.info("  → Fetching node type distribution...")
            node_dist = neo4j_client.get_node_type_distribution()
            logger.info(f"  ✓ Found {len(node_dist)} node types")
            
            logger.info("  → Fetching edge type distribution...")
            edge_dist = neo4j_client.get_edge_type_distribution()
            logger.info(f"  ✓ Found {len(edge_dist)} edge types")
            
            # Metadata
            logger.info("  → Fetching last updated timestamp...")
            last_updated = neo4j_client.get_last_updated()
            logger.info(f"  ✓ Last updated: {last_updated}")
            
            logger.info("  → Fetching dataset metadata...")
            datasets_meta = neo4j_client.get_datasets_with_metadata()
            logger.info(f"  ✓ Found {len(datasets_meta)} datasets")
            
            # Database size
            logger.info("  → Calculating database size...")
            db_size_info = neo4j_client.get_database_size()
            db_size_gb = db_size_info.get("size_gb")
            data_size = f"{db_size_gb} GB" if db_size_gb else "calculating..."
            logger.info(f"  ✓ Database size: {data_size}")
            
            # Frequent relationships (this can be slow!)
            logger.info("  → Analyzing frequent relationships (this may take a while)...")
            freq_rels = neo4j_client.get_frequent_relationships(limit=50)
            logger.info(f"  ✓ Found {len(freq_rels)} relationship patterns")
            
            # Detailed schema (can also be slow!)
            logger.info("  → Building detailed schema...")
            schema = neo4j_client.get_detailed_schema()
            logger.info(f"  ✓ Schema: {len(schema['nodes'])} node types, {len(schema['edges'])} edge patterns")
            
            # Build response
            graph_info = {
                "node_count": counts["node_count"],
                "edge_count": counts["edge_count"],
                "last_updated_at": last_updated,
                "dataset_count": len(datasets_meta),
                "data_size": data_size,
                "top_entities": node_dist,
                "top_connections": edge_dist,
                "frequent_relationships": freq_rels,
                "schema": schema,
                "datasets": datasets_meta,
                "top_entites": node_dist,  # Typo preserved for compatibility
                "generated_at": datetime.now().isoformat()
            }
            
            logger.info(f"✓  Generated complete graph info!")
            return graph_info
            
        except Exception as e:
            logger.error(f" Error generating graph info: {e}")
            raise
        
    def save_to_file(self, data: dict):
        """Save graph info to JSON file"""
        try:
            # Create directory if it doesn't exist
            self.cache_file.parent.mkdir(parents=True, exist_ok=True)
            
            # Write JSON file
            with open(self.cache_file, 'w') as f:
                json.dump(data, f, indent=2)
            
            self.last_generated = datetime.now()
            logger.info(f"✓  Saved graph_info.json to {self.cache_file}")
            
        except Exception as e:
            logger.error(f" Error saving graph info: {e}")
            raise
    
    def load_from_file(self) -> Optional[dict]:
        """Load graph info from JSON file"""
        try:
            if not self.cache_file.exists():
                logger.warning(f"Cache file not found: {self.cache_file}")
                return None
            
            with open(self.cache_file, 'r') as f:
                data = json.load(f)
            
            logger.info(f"✓  Loaded graph_info.json from {self.cache_file}")
            return data
            
        except Exception as e:
            logger.error(f" Error loading graph info: {e}")
            return None
    
    def refresh(self):
        """Generate and save graph info"""
        data = self.generate_graph_info()
        self.save_to_file(data)
        return data
    
    def get_cache_age_minutes(self) -> Optional[int]:
        """Get age of cached file in minutes"""
        if not self.cache_file.exists():
            return None
        
        modified_time = datetime.fromtimestamp(self.cache_file.stat().st_mtime)
        age = datetime.now() - modified_time
        return int(age.total_seconds() / 60)

# Global instance
graph_info_cache = GraphInfoCache(
    cache_file="/mnt/hdd_1/abdu_md/kg-service/annotation-dashboard/neo4j_graph_info.json"  # ← Change this path as needed
)