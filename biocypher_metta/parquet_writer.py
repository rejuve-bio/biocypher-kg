# Author: Abdu Mohammed <abdu.kebede@singularitynet.io>
import pathlib
import os
from typing import Dict, List, Optional, Any, Union
from biocypher._logger import logger
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import numpy as np
from datetime import datetime
from biocypher_metta import BaseWriter

class ParquetWriter(BaseWriter):
    """
    A BioCypher writer that outputs nodes and edges as Parquet files.
    Provides efficient columnar storage with robust type handling.
    """
    
    def __init__(
        self,
        schema_config: dict,
        biocypher_config: dict,
        output_dir: str,
        buffer_size: int = 10000,
        overwrite: bool = False,
        excluded_properties: Optional[List[str]] = None,
    ):
        """
        Initialize the Parquet writer with enhanced type handling.
        
        Args:
            schema_config: BioCypher schema configuration
            biocypher_config: BioCypher main configuration
            output_dir: Directory to write Parquet files to
            buffer_size: Number of entities to buffer before writing to disk
            overwrite: Whether to overwrite existing files
            excluded_properties: List of property keys to exclude from output
        """
        super().__init__(schema_config, biocypher_config, output_dir)
        
        self.buffer_size = buffer_size
        self.overwrite = overwrite
        self.excluded_properties = excluded_properties or []
        
        # Initialize data structures
        self.node_buffers: Dict[str, List[dict]] = {}
        self.edge_buffers: Dict[str, List[dict]] = {}
        self.node_schemas: Dict[str, pa.Schema] = {}
        self.edge_schemas: Dict[str, pa.Schema] = {}
        
        # Prepare output directories
        self.node_dir = os.path.join(self.output_path, "nodes")
        self.edge_dir = os.path.join(self.output_path, "edges")
        pathlib.Path(self.node_dir).mkdir(parents=True, exist_ok=True)
        pathlib.Path(self.edge_dir).mkdir(parents=True, exist_ok=True)
        
        logger.info(f"Initialized Parquet writer with output directory: {output_dir}")

    def write_nodes(self, nodes: List[tuple], path_prefix: Optional[str] = None, create_dir: bool = True) -> tuple:
        """
        Write nodes to Parquet files with robust error handling.
        
        Args:
            nodes: List of node tuples (id, label, properties)
            path_prefix: Optional subdirectory for output
            create_dir: Whether to create the subdirectory if needed
            
        Returns:
            Tuple of (node frequency dict, node properties dict)
        """
        output_dir = self.node_dir
        if path_prefix:
            output_dir = os.path.join(self.node_dir, path_prefix)
            if create_dir:
                pathlib.Path(output_dir).mkdir(parents=True, exist_ok=True)
        
        for node in nodes:
            try:
                self.extract_node_info(node)
                node_id, label, properties = node
                label = self._normalize_label(label)
                
                # Initialize buffer and schema if needed
                if label not in self.node_buffers:
                    self._initialize_node_type(label, properties, output_dir)
                
                # Process properties 
                processed_props = self._process_properties(properties)
                
                # Add to buffer
                self.node_buffers[label].append({
                    "id": node_id,
                    **processed_props
                })
                
                # Write if buffer full
                if len(self.node_buffers[label]) >= self.buffer_size:
                    self._flush_nodes(label)
                    
            except Exception as e:
                logger.error(f"Error processing node {node_id}: {str(e)}")
                continue

        return self.node_freq, self.node_props

    def write_edges(self, edges: List[tuple], path_prefix: Optional[str] = None, create_dir: bool = True) -> dict:
        """
        Write edges to Parquet files with robust error handling.
        
        Args:
            edges: List of edge tuples (source_id, target_id, label, properties)
            path_prefix: Optional subdirectory for output
            create_dir: Whether to create the subdirectory if needed
            
        Returns:
            Edge frequency dictionary
        """
        output_dir = self.edge_dir
        if path_prefix:
            output_dir = os.path.join(self.edge_dir, path_prefix)
            if create_dir:
                pathlib.Path(output_dir).mkdir(parents=True, exist_ok=True)
        
        for edge in edges:
            try:
                self.extract_edge_info(edge)
                src_id, tgt_id, label, properties = edge
                label = self._normalize_label(label)
                
                # Initialize buffer and schema if needed
                if label not in self.edge_buffers:
                    self._initialize_edge_type(label, properties, output_dir)
                
                # Process properties with enhanced type handling
                processed_props = self._process_properties(properties)
                
                # Add to buffer
                self.edge_buffers[label].append({
                    "source_id": src_id,
                    "target_id": tgt_id,
                    **processed_props
                })
                
                # Write if buffer full
                if len(self.edge_buffers[label]) >= self.buffer_size:
                    self._flush_edges(label)
                    
            except Exception as e:
                logger.error(f"Error processing edge {src_id}-{tgt_id}: {str(e)}")
                continue

        return self.edge_freq

    def _initialize_node_type(self, label: str, properties: dict, output_dir: str):
        """Initialize schema and buffer for a node type with safe defaults."""
        try:
            schema = pa.schema([
                pa.field("id", pa.string()),
                *self._create_schema_fields(properties)
            ])
            self.node_schemas[label] = schema
            self.node_buffers[label] = []
            
            # Create empty file with schema if overwrite is True
            if self.overwrite:
                path = os.path.join(output_dir, f"{label}.parquet")
                empty_table = pa.Table.from_pandas(
                    pd.DataFrame({k: [] for k in schema.names}),
                    schema=schema
                )
                pq.write_table(empty_table, path)
                logger.debug(f"Created empty Parquet file for node type: {label}")
                
        except Exception as e:
            logger.error(f"Error initializing node type {label}: {str(e)}")
            raise

    def _initialize_edge_type(self, label: str, properties: dict, output_dir: str):
        """Initialize schema and buffer for an edge type with safe defaults."""
        try:
            schema = pa.schema([
                pa.field("source_id", pa.string()),
                pa.field("target_id", pa.string()),
                *self._create_schema_fields(properties)
            ])
            self.edge_schemas[label] = schema
            self.edge_buffers[label] = []
            
            # Create empty file with schema if overwrite is True
            if self.overwrite:
                path = os.path.join(output_dir, f"{label}.parquet")
                empty_table = pa.Table.from_pandas(
                    pd.DataFrame({k: [] for k in schema.names}),
                    schema=schema
                )
                pq.write_table(empty_table, path)
                logger.debug(f"Created empty Parquet file for edge type: {label}")
                
        except Exception as e:
            logger.error(f"Error initializing edge type {label}: {str(e)}")
            raise

    def _create_schema_fields(self, properties: dict) -> List[pa.Field]:
        """
        Convert properties to PyArrow schema fields with robust type inference.
        Handles empty lists and None values safely.
        """
        fields = []
        for k, v in properties.items():
            if k in self.excluded_properties:
                continue
            
            field_name = str(k)
            
            try:
                if v is None:
                    # Default to string for None values
                    fields.append(pa.field(field_name, pa.string(), nullable=True))
                    continue
                    
                if isinstance(v, list):
                    # Handle list types with empty list fallback
                    if not v:  # Empty list
                        fields.append(pa.field(field_name, pa.list_(pa.string()), nullable=True))
                    else:
                        # Try to infer type from first element
                        try:
                            elem_type = self._infer_arrow_type(v[0])
                            fields.append(pa.field(field_name, pa.list_(elem_type), nullable=True))
                        except:
                            fields.append(pa.field(field_name, pa.list_(pa.string()), nullable=True))
                elif isinstance(v, dict):
                    # Handle nested structures
                    try:
                        nested_fields = self._create_schema_fields(v)
                        fields.append(pa.field(field_name, pa.struct(nested_fields), nullable=True))
                    except:
                        fields.append(pa.field(field_name, pa.string()), nullable=True)
                else:
                    # Handle scalar types
                    try:
                        fields.append(pa.field(field_name, self._infer_arrow_type(v), nullable=True))
                    except:
                        fields.append(pa.field(field_name, pa.string()), nullable=True)
                        
            except Exception as e:
                logger.warning(f"Could not infer type for property {k}, defaulting to string: {str(e)}")
                fields.append(pa.field(field_name, pa.string()), nullable=True)
        
        return fields

    def _infer_arrow_type(self, value: Any) -> pa.DataType:
        """Infer the appropriate Arrow data type for a given value with fallbacks."""
        try:
            if isinstance(value, bool):
                return pa.bool_()
            elif isinstance(value, int):
                return pa.int64()
            elif isinstance(value, float):
                return pa.float64()
            elif isinstance(value, str):
                return pa.string()
            elif isinstance(value, datetime):
                return pa.timestamp('us')
            elif isinstance(value, (np.integer, np.floating, np.bool_)):
                return pa.from_numpy_dtype(type(value))
            elif hasattr(value, 'isoformat'):  # Handle date-like objects
                return pa.timestamp('us')
            else:
                return pa.string()
        except:
            return pa.string()

    def _process_properties(self, properties: dict) -> dict:
        """
        Convert properties to Parquet-compatible formats with robust handling.
        Returns a new dictionary with processed values.
        """
        processed = {}
        if not properties:
            return processed
            
        for k, v in properties.items():
            if k in self.excluded_properties:
                continue
                
            try:
                processed[k] = self._convert_value(v)
            except Exception as e:
                logger.warning(f"Could not convert property {k}, skipping: {str(e)}")
                continue
        
        return processed

    def _convert_value(self, value: Any) -> Any:
        """
        Recursively convert values to Parquet-compatible formats with fallbacks.
        """
        if value is None:
            return None
            
        try:
            if isinstance(value, list):
                if not value:  # Empty list
                    return []
                return [self._convert_value(v) for v in value]
            elif isinstance(value, dict):
                return {str(k): self._convert_value(v) for k, v in value.items()}
            elif isinstance(value, (int, float, str, bool)):
                return value
            elif isinstance(value, datetime):
                return value
            elif hasattr(value, 'isoformat'):  # Handle date-like objects
                return value.isoformat()
            else:
                return str(value)
        except:
            return str(value)

    def _flush_nodes(self, label: str):
        """Write buffered nodes to Parquet file with error handling."""
        if not self.node_buffers.get(label):
            return

        try:
            df = pd.DataFrame(self.node_buffers[label])
            
            # Ensure all schema fields are present in DataFrame
            for field in self.node_schemas[label]:
                if field.name not in df.columns:
                    df[field.name] = None
                    
            table = pa.Table.from_pandas(
                df,
                schema=self.node_schemas[label],
                preserve_index=False
            )
            
            path = os.path.join(self.node_dir, f"{label}.parquet")
            self._write_parquet(table, path)
            
            logger.debug(f"Flushed {len(df)} nodes of type {label} to Parquet")
            self.node_buffers[label] = []
        except Exception as e:
            logger.error(f"Error flushing nodes of type {label}: {str(e)}")
            raise

    def _flush_edges(self, label: str):
        """Write buffered edges to Parquet file with error handling."""
        if not self.edge_buffers.get(label):
            return

        try:
            df = pd.DataFrame(self.edge_buffers[label])
            
            # Ensure all schema fields are present in DataFrame
            for field in self.edge_schemas[label]:
                if field.name not in df.columns:
                    df[field.name] = None
                    
            table = pa.Table.from_pandas(
                df,
                schema=self.edge_schemas[label],
                preserve_index=False
            )
            
            path = os.path.join(self.edge_dir, f"{label}.parquet")
            self._write_parquet(table, path)
            
            logger.debug(f"Flushed {len(df)} edges of type {label} to Parquet")
            self.edge_buffers[label] = []
        except Exception as e:
            logger.error(f"Error flushing edges of type {label}: {str(e)}")
            raise

    def _write_parquet(self, table: pa.Table, path: str):
        """
        Write PyArrow table to Parquet with append/overwrite logic.
        Handles partitioning if needed.
        """
        write_options = {
            'compression': 'snappy',
            'version': '2.6',
            'data_page_size': 1024 * 1024,  # 1MB
            'coerce_timestamps': 'us',
            'allow_truncated_timestamps': True
        }
        
        try:
            if self.overwrite or not os.path.exists(path):
                pq.write_table(table, path, **write_options)
            else:
                # Read existing data
                existing = pq.read_table(path)
                # Combine with new data
                combined = pa.concat_tables([existing, table])
                # Write back
                pq.write_table(combined, path, **write_options)
        except Exception as e:
            logger.error(f"Error writing to Parquet file {path}: {str(e)}")
            raise

    def _normalize_label(self, label: str) -> str:
        """
        Normalize labels for filesystem safety.
        Replaces problematic characters with underscores.
        """
        if not label:
            return "unknown"
            
        if "." in label:
            label = label.split(".")[1]
        return (
            label.replace(" ", "_")
                .replace(":", "_")
                .replace("/", "_")
                .replace("\\", "_")
                .replace("*", "_")
                .replace("?", "_")
                .replace('"', "_")
                .replace("<", "_")
                .replace(">", "_")
                .replace("|", "_")
                .lower()
        )

    def finalize(self):
        """
        Flush all remaining buffers to disk.
        Should be called after all data has been processed.
        """
        logger.info("Finalizing Parquet writer - flushing all remaining data")
        
        try:
            for label in list(self.node_buffers.keys()):
                if self.node_buffers[label]:
                    self._flush_nodes(label)
                    
            for label in list(self.edge_buffers.keys()):
                if self.edge_buffers[label]:
                    self._flush_edges(label)
                    
            logger.info("Parquet writing completed successfully")
        except Exception as e:
            logger.error(f"Error during finalization: {str(e)}")
            raise

    def __del__(self):
        """Destructor to ensure all data is flushed."""
        try:
            self.finalize()
        except:
            pass