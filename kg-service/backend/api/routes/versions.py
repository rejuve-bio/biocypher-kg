"""
Version management endpoints for BioCypher KG Observatory
"""
from fastapi import APIRouter, HTTPException
from neo4j import GraphDatabase
import json
from typing import List, Dict, Any, Optional
from datetime import datetime
from pathlib import Path
import subprocess
import os
from backend.core.config import settings

router = APIRouter(prefix="/api", tags=["versions"])

# Neo4j connection
NEO4J_URI = settings.NEO4J_URI
NEO4J_USER = settings.NEO4J_USER
NEO4J_PASSWORD = settings.NEO4J_PASSWORD

ARCHIVE_BASE = settings.ARCHIVE_BASE

def get_neo4j_driver():
    return GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))


def get_mork_metadata():
    """Get MORK metadata from JSON file on local filesystem"""
    metadata_file = f"{ARCHIVE_BASE}/mork/version_metadata.json"
    
    try:
        if os.path.exists(metadata_file):
            with open(metadata_file, 'r') as f:
                return json.load(f)
        return None
    except Exception as e:
        print(f"Error reading MORK metadata: {e}")
        return None


# ========== DATABASE LISTING ==========

@router.get("/databases")
async def list_databases():
    """List available databases"""
    return [
        {
            "name": "neo4j",
            "display_name": "Neo4j",
            "description": "Graph database with CSV data",
            "type": "graph",
            "enabled": True
        },
        {
            "name": "mork",
            "display_name": "MORK",
            "description": "AtomSpace database with MeTTa data",
            "type": "atomspace",
            "enabled": True
        }
    ]


# ========== LATEST VERSION ==========

@router.get("/databases/{db_type}/versions/latest")
async def get_latest_version(db_type: str):
    """Get the latest AtomSpace version info for a database"""
    
    if db_type == "neo4j":
        driver = get_neo4j_driver()
        
        try:
            with driver.session() as session:
                result = session.run("""
                    MATCH (v:KGVersion {db_type: "neo4j"})
                    RETURN v.version as version,
                           v.build_id as build_id,
                           v.created_at as created_at,
                           v.changed_datasets as changed_datasets,
                           v.unchanged_datasets as unchanged_datasets,
                           v.dataset_versions_json as dataset_versions_json
                    ORDER BY v.created_at DESC
                    LIMIT 1
                """).single()
                
                if not result:
                    raise HTTPException(status_code=404, detail="No Neo4j versions found")
                
                dataset_versions = json.loads(result["dataset_versions_json"]) if result["dataset_versions_json"] else {}
                
                return {
                    "database": "neo4j",
                    "version": result["version"],
                    "build_id": result["build_id"],
                    "created_at": result["created_at"],
                    "changed_datasets": result["changed_datasets"],
                    "unchanged_datasets": result["unchanged_datasets"],
                    "dataset_versions": dataset_versions
                }
        finally:
            driver.close()
    
    elif db_type == "mork":
        metadata = get_mork_metadata()
        
        if not metadata:
            raise HTTPException(status_code=404, detail="No MORK versions found")
        
        return {
            "database": "mork",
            "version": metadata.get("atomspace_version"),
            "build_id": metadata.get("build_id"),
            "created_at": metadata.get("build_timestamp"),
            "dataset_versions": metadata.get("dataset_versions", {})
        }
    
    else:
        raise HTTPException(status_code=404, detail=f"Database '{db_type}' not found")


# ========== COMPARE VERSIONS ==========

@router.get("/databases/{db_type}/versions/compare/{version1}/{version2}")
async def compare_versions(db_type: str, version1: str, version2: str, dataset: Optional[str] = None):
    """Compare two archived versions"""
    
    if db_type not in ["neo4j", "mork"]:
        raise HTTPException(status_code=404, detail=f"Database '{db_type}' not found")
    
    script_path = settings.VERSION_DIFF_SCRIPT
    
    try:
        # Build local command
        cmd = ["python3", script_path, "--archive-dir", ARCHIVE_BASE, "--db-type", db_type, "--from", version1, "--to", version2, "--json"]
        
        if dataset:
            cmd.extend(["--dataset", dataset])
        
        # Run command locally
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
        
        if result.returncode != 0:
            error_msg = result.stderr if result.stderr else "Unknown error"
            raise HTTPException(status_code=500, detail=f"Comparison failed: {error_msg}")
        
        # Parse JSON output
        try:
            comparison_data = json.loads(result.stdout)
            return comparison_data
        except json.JSONDecodeError as e:
            raise HTTPException(status_code=500, detail=f"Invalid JSON: {result.stdout[:200]}")
    
    except subprocess.TimeoutExpired:
        raise HTTPException(status_code=504, detail="Comparison timed out")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error: {str(e)}")


# ========== LIST ALL VERSIONS ==========

@router.get("/databases/{db_type}/versions")
async def list_versions(db_type: str):
    """List all AtomSpace versions for a database"""
    
    if db_type == "neo4j":
        driver = get_neo4j_driver()
        
        try:
            with driver.session() as session:
                results = session.run("""
                    MATCH (v:KGVersion {db_type: "neo4j"})
                    RETURN v.version as version,
                           v.build_id as build_id,
                           v.created_at as created_at,
                           v.changed_datasets as changed_datasets,
                           size(v.changed_datasets) as num_changed
                    ORDER BY v.created_at DESC
                """)
                
                versions = []
                for record in results:
                    versions.append({
                        "version": record["version"],
                        "build_id": record["build_id"],
                        "created_at": record["created_at"],
                        "changed_datasets": record["changed_datasets"],
                        "num_changed": record["num_changed"]
                    })
                
                return {
                    "database": "neo4j",
                    "total": len(versions),
                    "versions": versions
                }
        finally:
            driver.close()
    
    elif db_type == "mork":
        # List versions from local archives
        try:
            archive_dir = f"{ARCHIVE_BASE}/mork"
            
            if not os.path.exists(archive_dir):
                return {"database": "mork", "total": 0, "versions": []}
            
            # Use find command locally
            cmd = ["find", archive_dir, "-mindepth", "2", "-maxdepth", "2", "-type", "d"]
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=10)
            
            if result.returncode == 0:
                versions_set = set()
                for line in result.stdout.strip().split('\n'):
                    if not line:
                        continue
                    # Extract version from path (e.g., /mnt/hdd_1/biocypher-kg/output/human/biocypher-archives/neo4j/mork/gencode/v1)
                    parts = line.split('/')
                    if len(parts) >= 2:
                        version = parts[-1]
                        if version.startswith('v'):
                            versions_set.add(version)
                
                versions_list = sorted(list(versions_set), key=lambda x: int(x.replace('v', '')), reverse=True)
                
                return {
                    "database": "mork",
                    "total": len(versions_list),
                    "versions": [{"version": v} for v in versions_list]
                }
            
            return {"database": "mork", "total": 0, "versions": []}
            
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))
    
    else:
        raise HTTPException(status_code=404, detail=f"Database '{db_type}' not found")


# ========== ARCHIVES ==========

@router.get("/databases/{db_type}/archives")
async def list_archives(db_type: str):
    """List all archived versions and datasets (optimized)"""
    
    if db_type not in ["neo4j", "mork"]:
        raise HTTPException(status_code=404, detail=f"Database '{db_type}' not found")
    
    archive_dir = f"{ARCHIVE_BASE}/{db_type}"
    
    try:
        if not os.path.exists(archive_dir):
            return {"database": db_type, "archives": [], "total_datasets": 0}
        
        # Find directories
        cmd = ["find", archive_dir, "-mindepth", "2", "-maxdepth", "2", "-type", "d"]
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=15)
        
        if result.returncode == 0:
            dataset_versions = {}
            
            for line in result.stdout.strip().split('\n'):
                if not line or "version_metadata.json" in line:
                    continue
                
                # Parse: /mnt/hdd_1/biocypher-kg/output/human/biocypher-archives/neo4j/mork/gencode/v1
                parts = line.split('/')
                if len(parts) >= 2:
                    dataset = parts[-2]
                    version = parts[-1]
                    
                    if dataset not in dataset_versions:
                        dataset_versions[dataset] = []
                    
                    dataset_versions[dataset].append({
                        "version": version,
                        "path": line
                    })
            
            archives = []
            for dataset, versions in dataset_versions.items():
                archives.append({
                    "dataset": dataset,
                    "versions": sorted(versions, key=lambda x: int(x['version'].replace('v', '')), reverse=True),
                    "total_versions": len(versions)
                })
            
            return {
                "database": db_type,
                "archives": sorted(archives, key=lambda x: x['dataset']),
                "total_datasets": len(archives)
            }
        
        return {"database": db_type, "archives": [], "total_datasets": 0}
    
    except subprocess.TimeoutExpired:
        raise HTTPException(status_code=504, detail="Request timed out")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error listing archives: {str(e)}")

@router.get("/databases/{db_type}/archives/{dataset}/{version}/stats")
async def get_archive_stats(db_type: str, dataset: str, version: str):
    """Get detailed stats for a specific archive (on-demand)"""
    
    if db_type not in ["neo4j", "mork"]:
        raise HTTPException(status_code=404, detail=f"Database '{db_type}' not found")
    
    archive_path = f"{ARCHIVE_BASE}/{db_type}/{dataset}/{version}"
    
    if not os.path.exists(archive_path):
        raise HTTPException(status_code=404, detail=f"Archive not found: {archive_path}")
    
    file_ext = "*.csv" if db_type == "neo4j" else "*.metta"
    
    try:
        # Get size using du
        size_cmd = ["du", "-sb", archive_path]
        size_result = subprocess.run(size_cmd, capture_output=True, text=True, timeout=10)
        
        size_bytes = 0
        if size_result.returncode == 0:
            try:
                size_bytes = int(size_result.stdout.strip().split()[0])
            except:
                pass
        
        # Count files safely (no shell pipeline)
        count_cmd = ["find", archive_path, "-type", "f", "-name", file_ext]
        count_result = subprocess.run(count_cmd, capture_output=True, text=True, timeout=10)

        file_count = 0
        if count_result.returncode == 0:
            file_count = sum(1 for line in count_result.stdout.splitlines() if line.strip())
        
        return {
            "database": db_type,
            "dataset": dataset,
            "version": version,
            "size_bytes": size_bytes,
            "size_mb": round(size_bytes / 1024 / 1024, 2),
            "file_count": file_count
        }
    
    except subprocess.TimeoutExpired:
        raise HTTPException(status_code=504, detail="Request timed out")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/databases/{db_type}/stats/current")
async def get_current_stats(db_type: str):
    """Get statistics for current database"""
    
    if db_type == "neo4j":
        driver = get_neo4j_driver()
        
        try:
            with driver.session() as session:
                # Get latest version
                version_result = session.run("""
                    MATCH (v:KGVersion {db_type: "neo4j"})
                    RETURN v.version as version, v.created_at as created_at
                    ORDER BY v.created_at DESC
                    LIMIT 1
                """).single()
                
                if not version_result:
                    raise HTTPException(status_code=404, detail="No versions found")
                
                # Get node count
                node_result = session.run("""
                    MATCH (n)
                    WHERE NOT n:KGVersion AND NOT n:DatasetVersion 
                      AND NOT n:DatasetHash AND NOT n:DatasetMapping
                    RETURN count(n) as count
                """).single()
                
                # Get edge count
                edge_result = session.run("""
                    MATCH ()-[r]->()
                    RETURN count(r) as count
                """).single()
                
                # Get nodes by source
                source_results = session.run("""
                    MATCH (n)
                    WHERE NOT n:KGVersion AND NOT n:DatasetVersion 
                      AND NOT n:DatasetHash AND NOT n:DatasetMapping
                      AND n.source IS NOT NULL
                    RETURN n.source as source, count(*) as count
                    ORDER BY count DESC
                """)
                
                sources = {}
                for record in source_results:
                    sources[record["source"]] = record["count"]
                
                return {
                    "database": "neo4j",
                    "version": version_result["version"],
                    "created_at": version_result["created_at"],
                    "total_nodes": node_result["count"],
                    "total_edges": edge_result["count"],
                    "nodes_by_source": sources
                }
        finally:
            driver.close()
    
    elif db_type == "mork":
        # Get MORK stats from metadata + count atoms locally
        metadata = get_mork_metadata()
        
        if not metadata:
            raise HTTPException(status_code=404, detail="No MORK versions found")
        
        # Count atoms in MORK using local Python and configured URL
        try:
            cmd = [
                "python3", "-c",
                "import os; from mork import MORK; s=MORK(os.getenv('MORK_URL', 'http://localhost:8432')); scope=s.work_at('annotation').__enter__(); data=scope.download_(max_results=100000); data.block(); lines=[l for l in data.data.split('\\n') if l.strip() and not l.strip().startswith(';')]; print(len(lines))"
            ]

            env = os.environ.copy()
            env["MORK_URL"] = getattr(settings, "MORK_URL", "http://localhost:8432")
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=30, env=env)
            
            atom_count = 0
            if result.returncode == 0:
                try:
                    atom_count = int(result.stdout.strip())
                except:
                    pass
            
            dataset_count = len(metadata.get('dataset_versions', {}))
            
            return {
                "database": "mork",
                "version": metadata.get('atomspace_version'),
                "created_at": metadata.get('build_timestamp'),
                "total_atoms": atom_count,
                "total_datasets": dataset_count,
                "dataset_versions": metadata.get('dataset_versions', {})
            }
            
        except subprocess.TimeoutExpired:
            raise HTTPException(status_code=504, detail="MORK query timed out")
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Error querying MORK: {str(e)}")
    
    else:
        raise HTTPException(status_code=404, detail=f"Database '{db_type}' not found")
       
@router.get("/databases/{db_type}/archives/{dataset}/{version}/files")
async def get_archive_files(db_type: str, dataset: str, version: str):
    """List files in a specific archive"""
    
    if db_type not in ["neo4j", "mork"]:
        raise HTTPException(status_code=404, detail=f"Database '{db_type}' not found")
    
    archive_path = f"{ARCHIVE_BASE}/{db_type}/{dataset}/{version}"
    
    if not os.path.exists(archive_path):
        raise HTTPException(status_code=404, detail=f"Archive not found: {db_type}/{dataset}/{version}")
    
    file_ext = "*.csv" if db_type == "neo4j" else "*.metta"
    
    try:
        cmd = ["find", archive_path, "-name", file_ext, "-type", "f"]
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=10)
        
        if result.returncode != 0:
            raise HTTPException(status_code=404, detail=f"Archive not found: {db_type}/{dataset}/{version}")
        
        files = []
        for line in result.stdout.strip().split('\n'):
            if not line:
                continue
            
            # Get file size using stat
            try:
                stat_result = os.stat(line)
                size_bytes = stat_result.st_size
            except:
                size_bytes = 0
            
            # Get relative path
            relative_path = line.replace(f"{archive_path}/", "")
            name = line.split('/')[-1]
            
            files.append({
                "path": relative_path,
                "name": name,
                "size_bytes": size_bytes,
                "size_kb": round(size_bytes / 1024, 2)
            })
        
        return {
            "database": db_type,
            "dataset": dataset,
            "version": version,
            "files": files,
            "total_files": len(files)
        }
    
    except subprocess.TimeoutExpired:
        raise HTTPException(status_code=504, detail="Request timed out")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# ========== MORK SUMMARY (Same structure as Neo4j) ==========

@router.get("/databases/mork/summary")
async def get_mork_summary():
    """Get MORK database summary (same structure as Neo4j summary)"""
    
    try:
        metadata = get_mork_metadata()
        
        if not metadata:
            raise HTTPException(status_code=404, detail="No MORK metadata found")
        
        # Call the get_mork_summary.py script locally
        script_path = settings.MORK_SUMMARY_SCRIPT
        
        if not os.path.exists(script_path):
            raise HTTPException(status_code=500, detail=f"Script not found: {script_path}")
        
        cmd = ["python3", script_path]
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=120)
        
        if result.returncode != 0:
            raise HTTPException(status_code=500, detail=f"Script failed: {result.stderr}")
        
        # Parse JSON output
        try:
            stats = json.loads(result.stdout.strip())
        except json.JSONDecodeError as e:
            raise HTTPException(status_code=500, detail=f"Invalid JSON: {result.stdout[:200]}")
        
        if "error" in stats:
            raise HTTPException(status_code=404, detail=f"MORK error: {stats['error']}")
        
        # Build datasets list from metadata
        dataset_versions = metadata.get('dataset_versions', {})
        datasets_list = []
        
        for dataset, version in sorted(dataset_versions.items()):
            datasets_list.append([
                dataset,
                version,
                metadata.get('build_timestamp', 'N/A')[:10],
                "N/A"
            ])
        
        # Return same structure as Neo4j summary
        return {
            "database": "mork",
            "node_count": stats["total_nodes"],
            "edge_count": stats["total_edges"],
            "dataset_count": len(dataset_versions),
            "last_updated_at": metadata.get('build_timestamp', 'N/A'),
            "top_entities": stats["top_atoms"],
            "top_connections": stats["top_edges"],
            "datasets": datasets_list,
            "schema": {
                "node_types": stats["all_atom_types"],
                "relationship_types": stats["all_edge_types"]
            }
        }
        
    except subprocess.TimeoutExpired:
        raise HTTPException(status_code=504, detail="MORK analysis timed out (120s)")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error: {str(e)}")
    
# ========== BACKWARD COMPATIBILITY (Old routes default to neo4j) ==========

@router.get("/versions/latest")
async def get_latest_version_legacy():
    """Get latest Neo4j version (legacy endpoint)"""
    return await get_latest_version("neo4j")


@router.get("/versions/compare/{version1}/{version2}")
async def compare_versions_legacy(version1: str, version2: str, dataset: Optional[str] = None):
    """Compare Neo4j versions (legacy endpoint)"""
    return await compare_versions("neo4j", version1, version2, dataset)


@router.get("/versions")
async def list_versions_legacy():
    """List Neo4j versions (legacy endpoint)"""
    return await list_versions("neo4j")


@router.get("/versions/archives")
async def list_archives_legacy():
    """List Neo4j archives (legacy endpoint)"""
    return await list_archives("neo4j")