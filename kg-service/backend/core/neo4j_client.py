from neo4j import GraphDatabase
from typing import List, Dict, Any, Optional
from backend.core.config import settings
import logging

logger = logging.getLogger(__name__)

class Neo4jClient:
    def __init__(self):
        self.driver = GraphDatabase.driver(
            settings.NEO4J_URI,
            auth=(settings.NEO4J_USER, settings.NEO4J_PASSWORD)
        )
        logger.info(f"Connected to Neo4j at {settings.NEO4J_URI}")

    def close(self):
        self.driver.close()

    def verify_connection(self) -> bool:
        try:
            with self.driver.session(database=settings.NEO4J_DATABASE) as session:
                session.run("RETURN 1").consume()
            logger.info("[success:] Neo4j connection verified")
            return True
        except Exception as e:
            logger.error(f"[failed:] Connection failed: {e}")
            return False

    # ===== DISCOVERY =====

    def get_labels(self) -> List[str]:
        with self.driver.session(database=settings.NEO4J_DATABASE) as session:
            result = session.run("CALL db.labels() YIELD label RETURN label ORDER BY label")
            return [record["label"] for record in result]

    def get_relationship_types(self) -> List[str]:
        with self.driver.session(database=settings.NEO4J_DATABASE) as session:
            result = session.run("CALL db.relationshipTypes() YIELD relationshipType RETURN relationshipType ORDER BY relationshipType")
            return [record["relationshipType"] for record in result]
    
    def get_database_size(self) -> dict:
        """
        Get actual Neo4j store size in GB using apoc.monitor.store().
        Always returns a dict with size_gb, even if None.
        """
        try:
            with self.driver.session(database=settings.NEO4J_DATABASE) as session:
                result = session.run("""
                    CALL apoc.monitor.store()
                    YIELD totalStoreSize
                    RETURN totalStoreSize
                """).single()

                if result and result["totalStoreSize"] is not None:
                    size_bytes = int(result["totalStoreSize"])
                    size_gb = round(size_bytes / (1024**3), 2)
                    return {"size_gb": size_gb, "method": "apoc"}

        except Exception as e:
            logger.error(f"Failed to get database size: {e}")

        return {"size_gb": None, "method": "error"}

    def get_entity_properties(self, label: str) -> List[str]:
        with self.driver.session(database=settings.NEO4J_DATABASE) as session:
            result = session.run("""
                MATCH (n)
                WHERE $label IN labels(n)
                WITH n LIMIT 100
                UNWIND keys(n) as prop
                RETURN DISTINCT prop ORDER BY prop
            """, label=label)
            return [record["prop"] for record in result]

    # ===== COUNTS =====

    def get_total_counts(self) -> Dict[str, int]:
        with self.driver.session(database=settings.NEO4J_DATABASE) as session:
            nodes = session.run("MATCH (n) RETURN count(n) as count").single()["count"]
            edges = session.run("MATCH ()-[r]->() RETURN count(r) as count").single()["count"]
            return {"node_count": nodes, "edge_count": edges}

    def get_node_type_distribution(self, limit: int = 20) -> list:
        """Get distribution of node types"""
        with self.driver.session(database=settings.NEO4J_DATABASE) as session:
            result = session.run("""
                MATCH (n)
                WHERE NOT n:DatasetHash AND NOT n:DatasetVersion 
                    AND NOT n:KGVersion AND NOT n:DatasetMapping
                WITH labels(n)[0] as type, count(*) as count
                WHERE type IS NOT NULL
                RETURN type, count
                ORDER BY count DESC
                LIMIT $limit
            """, limit=limit)
            
            return [{"name": record["type"], "count": record["count"]} 
                    for record in result]  # ← Changed "type" to "name"

    def get_edge_type_distribution(self, limit: int = 30) -> list:
        """Get distribution of relationship types"""
        with self.driver.session(database=settings.NEO4J_DATABASE) as session:
            result = session.run("""
                MATCH ()-[r]->()
                WITH type(r) as type, count(*) as count
                RETURN type, count
                ORDER BY count DESC
                LIMIT $limit
            """, limit=limit)
            
            return [{"name": record["type"], "count": record["count"]} 
                    for record in result]  # ← Changed "type" to "name"

    def get_last_updated(self) -> Optional[str]:
        with self.driver.session(database=settings.NEO4J_DATABASE) as session:
            result = session.run("""
                MATCH (n)
                WHERE n.import_timestamp IS NOT NULL
                RETURN n.import_timestamp as ts
                ORDER BY ts DESC LIMIT 1
            """).single()
            return result["ts"] if result else None

    # ===== GENERIC ENTITY QUERIES =====

    def get_entities(self, label: str, limit: int = 100, 
                     offset: int = 0, updated_since: Optional[str] = None) -> List[Dict]:
        with self.driver.session(database=settings.NEO4J_DATABASE) as session:
            where = "WHERE $label IN labels(n)"
            params = {"label": label, "limit": limit, "offset": offset}

            if updated_since:
                where += " AND n.import_timestamp > $updated_since"
                params["updated_since"] = updated_since

            result = session.run(f"""
                MATCH (n)
                {where}
                RETURN properties(n) as props
                ORDER BY n.import_timestamp DESC
                SKIP $offset LIMIT $limit
            """, **params)
            return [dict(r["props"]) for r in result]

    def get_entity_by_id(self, label: str, entity_id: str) -> Optional[Dict]:
        with self.driver.session(database=settings.NEO4J_DATABASE) as session:
            result = session.run("""
                MATCH (n)
                WHERE $label IN labels(n) AND n.id = $id
                RETURN properties(n) as props
            """, label=label, id=entity_id).single()
            return dict(result["props"]) if result else None

    def get_entity_count(self, label: str) -> int:
        with self.driver.session(database=settings.NEO4J_DATABASE) as session:
            result = session.run("""
                MATCH (n)
                WHERE $label IN labels(n)
                RETURN count(n) as count
            """, label=label).single()
            return result["count"]

    # ===== UPDATES =====

    def get_updates_since(self, since_timestamp: str) -> Dict:
        with self.driver.session(database=settings.NEO4J_DATABASE) as session:
            result = session.run("""
                MATCH (n)
                WHERE n.import_timestamp > $since
                WITH labels(n)[0] as type, count(n) as count
                RETURN type, count
                ORDER BY count DESC
            """, since=since_timestamp)
            nodes = [{"type": r["type"], "count": r["count"]} for r in result]

            result = session.run("""
                MATCH ()-[r]->()
                WHERE r.import_timestamp > $since
                WITH type(r) as type, count(r) as count
                RETURN type, count
                ORDER BY count DESC
            """, since=since_timestamp)
            edges = [{"type": r["type"], "count": r["count"]} for r in result]

            return {
                "since": since_timestamp,
                "new_nodes": nodes,
                "new_edges": edges,
                "total_new_nodes": sum(n["count"] for n in nodes),
                "total_new_edges": sum(e["count"] for e in edges)
            }

    # ===== DATASETS =====

    def get_detailed_schema(self) -> dict:
        """Get comprehensive schema with node properties and edge details"""
        with self.driver.session(database=settings.NEO4J_DATABASE) as session:
            # Filter out metadata nodes
            node_result = session.run("""
                MATCH (n)
                WHERE NOT n:DatasetHash AND NOT n:DatasetVersion 
                    AND NOT n:KGVersion AND NOT n:DatasetMapping
                WITH DISTINCT labels(n)[0] as nodeType, n
                WHERE nodeType IS NOT NULL
                UNWIND keys(n) as prop
                WITH nodeType, collect(DISTINCT prop) as properties
                RETURN nodeType, properties
                ORDER BY nodeType
            """)
            
            nodes = []
            for record in node_result:
                # Filter out metadata properties
                props = [p for p in record["properties"] 
                        if p not in ['source', 'atomspace_version', 'dataset_version', 
                                    'import_timestamp', 'build_id', 'source_url', 'id']]
                nodes.append({
                    "data": {
                        "id": record["nodeType"].lower(),
                        "properties": sorted(props)
                    }
                })
            
            # Get edge schema (already filtered implicitly)
            edge_result = session.run("""
                MATCH (a)-[r]->(b)
                WHERE NOT a:DatasetHash AND NOT a:DatasetVersion 
                    AND NOT a:KGVersion AND NOT a:DatasetMapping
                    AND NOT b:DatasetHash AND NOT b:DatasetVersion 
                    AND NOT b:KGVersion AND NOT b:DatasetMapping
                WITH DISTINCT labels(a)[0] as source,
                            labels(b)[0] as target,
                            type(r) as rel_type
                WHERE source IS NOT NULL AND target IS NOT NULL
                WITH source, target, collect(DISTINCT rel_type) as connections
                RETURN source, target, connections
                ORDER BY source, target
            """)
            
            edges = []
            for record in edge_result:
                edges.append({
                    "data": {
                        "source": record["source"].lower(),
                        "target": record["target"].lower(),
                        "possible_connections": sorted([conn.lower() for conn in record["connections"]])
                    }
                })
            
            return {"nodes": nodes, "edges": edges}

    def get_frequent_relationships(self, limit: int = 50) -> list:
        """Get most frequent entity pair connections"""
        with self.driver.session(database=settings.NEO4J_DATABASE) as session:
            result = session.run("""
                MATCH (a)-[r]->(b)
                WITH labels(a)[0] as source_type, 
                    labels(b)[0] as target_type,
                    count(r) as count
                WHERE source_type IS NOT NULL AND target_type IS NOT NULL
                RETURN source_type, target_type, count
                ORDER BY count DESC
                LIMIT $limit
            """, limit=limit)
            
            return [
                {
                    "count": record["count"],
                    "entities": [record["source_type"].lower(), record["target_type"].lower()]
                }
                for record in result
            ]

    def get_datasets_with_metadata(self) -> list:
        """Get complete dataset metadata (batched to avoid memory issues)"""
        with self.driver.session(database=settings.NEO4J_DATABASE) as session:
            # Get all datasets first
            datasets_result = session.run("""
                MATCH (dv:DatasetVersion {db_type: "neo4j"})
                OPTIONAL MATCH (dm:DatasetMapping {folder: dv.dataset, db_type: "neo4j"})
                RETURN dv.dataset as name,
                    dv.version as version,
                    dv.timestamp as imported_on,
                    dm.source as source
                ORDER BY name
            """)
            
            datasets = []
            for record in datasets_result:
                source = record["source"]
                if not source:
                    continue
                
                # Get ALL node types (batched aggregation - no memory issues!)
                node_types_result = session.run("""
                    CALL {
                        MATCH (n)
                        WHERE n.source = $source
                            AND NOT n:DatasetHash AND NOT n:DatasetVersion 
                            AND NOT n:KGVersion AND NOT n:DatasetMapping
                        RETURN DISTINCT labels(n)[0] as label
                    } IN TRANSACTIONS OF 10000 ROWS
                    RETURN collect(DISTINCT label) as labels
                """, source=source).single()
                
                node_types = [l.lower() for l in node_types_result["labels"] if l]
                
                # Get ALL edge types (batched aggregation)
                edge_types_result = session.run("""
                    CALL {
                        MATCH ()-[r]->()
                        WHERE r.source = $source
                        RETURN DISTINCT type(r) as rel_type
                    } IN TRANSACTIONS OF 10000 ROWS
                    RETURN collect(DISTINCT rel_type) as types
                """, source=source).single()
                
                edge_types = [t.lower() for t in edge_types_result["types"] if t]
                
                # Get URL (quick lookup)
                url_result = session.run("""
                    MATCH (n)
                    WHERE n.source = $source AND n.source_url IS NOT NULL
                    RETURN n.source_url as url
                    LIMIT 1
                """, source=source).single()
                
                datasets.append({
                    "name": record["name"].upper() if record["name"] else "UNKNOWN",
                    "version": record["version"],
                    "url": url_result["url"] if url_result else None,
                    "nodes": sorted(node_types),
                    "edges": sorted(edge_types),
                    "imported_on": record["imported_on"][:10] if record["imported_on"] else None
                })
            
            return datasets
neo4j_client = Neo4jClient()
