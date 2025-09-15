import json
import os
from pathlib import Path
from typing import Dict, List, Optional, Any, Tuple, Set
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from collections import defaultdict
from biocypher._logger import logger
from biocypher_metta import BaseWriter


class ParquetWriter(BaseWriter):
    """
    A BioCypher writer that outputs nodes and edges to Parquet files.
     
    """

    def __init__(
        self,
        schema_config: str,
        biocypher_config: str,
        output_dir: str,
        buffer_size: int = 10000,
        overwrite: bool = False,
        excluded_properties: Optional[List[str]] = None,
    ):
        """
        Initialize the Parquet writer.
        
        Args:
            schema_config: BioCypher schema configuration
            biocypher_config: BioCypher main configuration
            output_dir: Directory to write Parquet files to
        """
        super().__init__(schema_config, biocypher_config, output_dir)

        # Configure serialization settings
        self.batch_size = buffer_size
        self.overwrite = overwrite
        self.excluded_properties = excluded_properties or []
        self.translation_table = str.maketrans({
            "'": "",
            '"': ""
        })

        # Create edge type mapping  
        self.ontologies = set(['go', 'bto', 'efo', 'cl', 'clo', 'uberon'])
        self.create_edge_types()

        # Initialize data structures for batched writing
        self._node_headers = defaultdict(set)
        self._edge_headers = defaultdict(set)
        self._temp_files = {}
        self.temp_buffer = defaultdict(list)
    def safe_schema(self):
        schema = self.bcy._get_ontology_mapping()._extend_schema()
        safe = {}
        for k, v in schema.items():
            try:
                # Will raise TypeError if entity belongs to multiple types
                if v.get("represented_as"):
                    safe[k] = v
            except TypeError:
                logger.warning(f"Skipping conflicting entity: {k}")
                continue
        return safe

    def create_edge_types(self):
        """
        Map edge types to their source and target node types based on the schema.
        """
        schema = schema = self.safe_schema()
        self.edge_node_types = {}

        for k, v in schema.items():
            if v["represented_as"] == "edge":
                edge_type = self.convert_input_labels(k)
                source_type = v.get("source", None)
                target_type = v.get("target", None)

                if source_type is not None and target_type is not None:
                    if isinstance(v["input_label"], list):
                        label = self.convert_input_labels(v["input_label"][0])
                        source_type = self.convert_input_labels(source_type[0])
                        target_type = self.convert_input_labels(target_type[0])
                    else:
                        label = self.convert_input_labels(v["input_label"])
                        source_type = self.convert_input_labels(source_type)
                        target_type = self.convert_input_labels(target_type)
                    output_label = v.get("output_label", None)

                    self.edge_node_types[label.lower()] = {
                        "source": source_type.lower(),
                        "target": target_type.lower(),
                        "output_label": output_label.lower() if output_label else label
                    }

    def preprocess_value(self, value):
        """
        Preprocess values for Parquet compatibility.
        """
        value_type = type(value)
        if value_type is list:
            return [self.preprocess_value(item) for item in value]
        if value_type is str:
            return value.translate(self.translation_table)
        return value

    def convert_input_labels(self, label):
        """
        Convert input labels to a uniform format.
        """
        return label.lower().replace(" ", "_")

    def preprocess_id(self, prev_id):
        """
        Preprocess IDs for consistent referencing.
        """
        replace_map = str.maketrans({' ': '_', ':':'_'})
        return prev_id.lower().strip().translate(replace_map)

    def _write_buffer_to_temp(self, label_or_key, buffer):
        """
        Write buffer data to temporary file for batch processing.
        """
        if buffer and label_or_key in self._temp_files:
            with open(self._temp_files[label_or_key], 'a') as f:
                for entry in buffer:
                    json.dump(entry, f)
                    f.write('\n')
            buffer.clear()

    def _init_node_writer(self, label, properties, path_prefix=None, adapter_name=None):
        """
        Initialize node writer for a specific label.
        """
        output_dir = self.get_output_path(path_prefix, adapter_name)
        # Filter out excluded properties
        filtered_props = {k: v for k, v in properties.items() if k not in self.excluded_properties}
        self._node_headers[label].update(filtered_props.keys())
        self._node_headers[label].add('id')

        if label not in self._temp_files:
            temp_file_path = output_dir / f"temp_nodes_{label}.jsonl"
            if temp_file_path.exists():
                temp_file_path.unlink()
            self._temp_files[label] = temp_file_path
        return label

    def _init_edge_writer(self, label, source_type, target_type, properties, path_prefix=None, adapter_name=None):
        """
        Initialize edge writer for a specific label and source/target combination.
        """
        output_dir = self.get_output_path(path_prefix, adapter_name)
        key = (label, source_type, target_type)
        # Filter out excluded properties
        filtered_props = {k: v for k, v in properties.items() if k not in self.excluded_properties}
        self._edge_headers[key].update(filtered_props.keys())
        self._edge_headers[key].update({'source_id', 'target_id', 'label', 'source_type', 'target_type'})

        if key not in self._temp_files:
            temp_file_path = output_dir / f"temp_edges_{label}_{source_type}_{target_type}.jsonl"
            if temp_file_path.exists():
                temp_file_path.unlink()
            self._temp_files[key] = temp_file_path
        return key

    def write_nodes(self, nodes, path_prefix=None, adapter_name=None):
        """
        Write nodes to Parquet files, skipping nodes that belong to multiple entity types.
        """
        self.temp_buffer.clear()
        self._temp_files.clear()
        self._node_headers.clear()
        node_freq = defaultdict(int)
        output_dir = self.get_output_path(path_prefix, adapter_name)

        try:
            # First pass: collect data and schema information
            for node in nodes:
                try:
                    id, label, properties = node
                    if "." in label:
                        label = label.split(".")[1]
                    label = label.lower()
                    node_freq[label] += 1

                    writer_key = self._init_node_writer(label, properties, path_prefix, adapter_name)
                    filtered_props = {k: v for k, v in properties.items() if k not in self.excluded_properties}
                    node_data = {'id': self.preprocess_id(id), **filtered_props}
                    self.temp_buffer[label].append(
                        {k: (json.dumps(v) if isinstance(v, list) else self.preprocess_value(v))
                        for k, v in node_data.items()}
                    )

                    if len(self.temp_buffer[label]) >= self.batch_size:
                        self._write_buffer_to_temp(label, self.temp_buffer[label])

                except TypeError as e:
                    if "belongs to more than one entity types" in str(e):
                        logger.warning(f"Skipping conflicting node {id}: {e}")
                        continue
                    else:
                        raise

            # Flush remaining buffers
            for label in list(self.temp_buffer.keys()):
                self._write_buffer_to_temp(label, self.temp_buffer[label])

            # Second pass: convert to Parquet
            for label in self._node_headers.keys():
                parquet_file_path = output_dir / f"nodes_{label}.parquet"
                if parquet_file_path.exists():
                    parquet_file_path.unlink()

                data_rows = []
                if label in self._temp_files and self._temp_files[label].exists():
                    with open(self._temp_files[label], 'r') as temp_f:
                        for line in temp_f:
                            data_rows.append(json.loads(line))

                if data_rows:
                    df = pd.DataFrame(data_rows)
                    for col in self._node_headers[label]:
                        if col not in df.columns:
                            df[col] = None

                    table = pa.Table.from_pandas(df)
                    pq.write_table(table, parquet_file_path, compression='snappy')

                if label in self._temp_files and self._temp_files[label].exists():
                    self._temp_files[label].unlink()

        finally:
            self.temp_buffer.clear()
            for temp_file in self._temp_files.values():
                if isinstance(temp_file, Path) and temp_file.exists():
                    temp_file.unlink()
            self._temp_files.clear()

        return node_freq, self._node_headers

    def write_edges(self, edges, path_prefix=None, adapter_name=None):
        """
        Write edges to Parquet files, skipping edges that belong to multiple entity types.
        """
        self.temp_buffer.clear()
        self._temp_files.clear()
        self._edge_headers.clear()
        edge_freq = defaultdict(int)
        output_dir = self.get_output_path(path_prefix, adapter_name)

        try:
            # First pass: collect data and schema information
            for edge in edges:
                try:
                    source_id, target_id, label, properties = edge
                    label = label.lower()
                    edge_freq[label] += 1

                    if label in self.edge_node_types:
                        edge_info = self.edge_node_types[label]
                        source_types = edge_info["source"]
                        target_types = edge_info["target"]

                        filtered_props = {k: v for k, v in properties.items() if k not in self.excluded_properties}

                        # Generate edges for all source/target type combinations
                        for src_type in source_types:
                            for tgt_type in target_types:
                                src_type_final = src_type
                                tgt_type_final = tgt_type

                                if src_type == "ontology_term":
                                    src_type_final = self.preprocess_id(source_id).split('_')[0]
                                if tgt_type == "ontology_term":
                                    tgt_type_final = self.preprocess_id(target_id).split('_')[0]

                                edge_data = {
                                    'source_id': self.preprocess_id(source_id),
                                    'target_id': self.preprocess_id(target_id),
                                    'source_type': src_type_final,
                                    'target_type': tgt_type_final,
                                    'label': edge_info.get("output_label", label),
                                    **filtered_props
                                }

                                writer_key = self._init_edge_writer(label, src_type_final, tgt_type_final, properties, path_prefix, adapter_name)
                                self.temp_buffer[writer_key].append(
                                    {k: (json.dumps(v) if isinstance(v, list) else self.preprocess_value(v))
                                    for k, v in edge_data.items()}
                                )

                                if len(self.temp_buffer[writer_key]) >= self.batch_size:
                                    self._write_buffer_to_temp(writer_key, self.temp_buffer[writer_key])

                except TypeError as e:
                    if "belongs to more than one entity types" in str(e):
                        logger.warning(f"Skipping conflicting edge {source_id}->{target_id} ({label}): {e}")
                        continue
                    else:
                        raise

            # Flush remaining buffers
            for key in list(self.temp_buffer.keys()):
                self._write_buffer_to_temp(key, self.temp_buffer[key])

            # Second pass: convert to Parquet
            for key in self._edge_headers.keys():
                input_label, source_type, target_type = key
                file_suffix = f"{input_label}_{source_type}_{target_type}".lower()
                parquet_file_path = output_dir / f"edges_{file_suffix}.parquet"

                if parquet_file_path.exists():
                    parquet_file_path.unlink()

                data_rows = []
                if key in self._temp_files and self._temp_files[key].exists():
                    with open(self._temp_files[key], 'r') as temp_f:
                        for line in temp_f:
                            data_rows.append(json.loads(line))

                if data_rows:
                    df = pd.DataFrame(data_rows)
                    for col in self._edge_headers[key]:
                        if col not in df.columns:
                            df[col] = None

                    table = pa.Table.from_pandas(df)
                    pq.write_table(table, parquet_file_path, compression='snappy')

                if key in self._temp_files and self._temp_files[key].exists():
                    self._temp_files[key].unlink()

        finally:
            self.temp_buffer.clear()
            for temp_file in self._temp_files.values():
                if isinstance(temp_file, Path) and temp_file.exists():
                    temp_file.unlink()
            self._temp_files.clear()

        return edge_freq


    def get_output_path(self, prefix=None, adapter_name=None):
        """
        Get the output path for files, creating directories as needed.
        """
        if prefix:
            output_dir = self.output_path / prefix
        elif adapter_name:
            output_dir = self.output_path / adapter_name
        else:
            output_dir = self.output_path

        output_dir.mkdir(parents=True, exist_ok=True)
        return output_dir

    def finalize(self):
        """
        Ensure all data is flushed when the writer is finalized.
        """
        for temp_file in self._temp_files.values():
            if isinstance(temp_file, Path) and temp_file.exists():
                temp_file.unlink()
        self._temp_files.clear()

        logger.info("ParquetWriter finalized - all data written and temp files cleaned up")