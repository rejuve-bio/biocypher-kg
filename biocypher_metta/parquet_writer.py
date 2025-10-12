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
import re

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
        Ensures source/target are always lists of lowercase strings. 
        """
        schema = self.safe_schema()
        self.edge_node_types = {}

        for k, v in schema.items():
            if v["represented_as"] == "edge":
                edge_type = self.convert_input_labels(k)

                # Handle input_label
                input_label = v.get("input_label")
                if isinstance(input_label, list):
                    label = self.convert_input_labels(input_label[0])
                else:
                    label = self.convert_input_labels(input_label)

                # Normalize source/target to lists
                source_types = v.get("source", [])
                target_types = v.get("target", [])
                if not isinstance(source_types, list):
                    source_types = [source_types]
                if not isinstance(target_types, list):
                    target_types = [target_types]

                # Lowercase everything
                source_types = [self.convert_input_labels(s) for s in source_types]
                target_types = [self.convert_input_labels(t) for t in target_types]

                output_label = v.get("output_label")
                self.edge_node_types[label.lower()] = {
                    "source": source_types,
                    "target": target_types,
                    "output_label": output_label.lower() if output_label else label,
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
        if not label:
            logger.warning("Received None or empty label in convert_input_labels")
            return "unknown_label"
        return str(label).lower().replace(" ", "_")


    

    def preprocess_id(self, prev_id):
        """
        Normalize IDs for Parquet output:
        - Accept (type, id) tuples and strings
        - Remove provider prefixes like 'ensembl_' or 'react_' or 'ensembl:' / 'react:'
        - Remove surrounding parentheses
        - Remove leading 'gene'/'protein'/'transcript' tokens if attached
        - Return a lowercase, normalized id (e.g. 'ensg00000000419' or 'r-hsa-162699')
        """
        if prev_id is None:
            return None

        # If tuple like ("GENE", "ENSEMBL:ENSG...") take second element
        if isinstance(prev_id, tuple):
            # typical tuple is (type, id)
            prev_id = prev_id[1] if len(prev_id) > 1 else prev_id[0]

        # ensure string
        prev_id = str(prev_id).strip()

        # remove surrounding parentheses if any
        if prev_id.startswith("(") and prev_id.endswith(")"):
            prev_id = prev_id[1:-1].strip()

        # remove a leading type token e.g. "gene " or "GENE " that may have been prepended
        prev_id = re.sub(r'^(gene|protein|transcript)\s*', '', prev_id, flags=re.IGNORECASE)

        # unify separators and lowercase
        normalized = prev_id.strip()

        # remove common provider prefixes and separators
        # examples to handle:
        #   "ensembl_ensg00000000419" -> "ensg00000000419"
        #   "ensembl:ENSG00000000419" -> "ensg00000000419"
        #   "react_r-hsa-162699" or "REACT:R-HSA-162699" -> "r-hsa-162699"
        normalized = normalized.replace("ENSEMBL:", "").replace("ensembl:", "")
        normalized = normalized.replace("REACT:", "").replace("react:", "")
        normalized = normalized.replace("ensembl_", "").replace("react_", "")

        # replace spaces with underscore, keep hyphens as-is, lowercase
        normalized = normalized.replace(" ", "_").lower()

        return normalized



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
        This version normalizes IDs and resolves gene/transcript/protein by CURIE prefix.
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
                    source_id_raw, target_id_raw, label, properties = edge
                    label = label.lower()
                    edge_freq[label] += 1

                    if label not in self.edge_node_types:
                        # If label unknown in schema, skip or fallback to writing generic edges
                        logger.debug(f"Edge label '{label}' not in schema, skipping.")
                        continue

                    edge_info = self.edge_node_types[label]
                    source_types = edge_info["source"]
                    target_types = edge_info["target"]

                    # Filter props per user exclusion before writing
                    filtered_props = {k: v for k, v in properties.items() if k not in self.excluded_properties}

                    # Clean IDs once
                    clean_source = self.preprocess_id(source_id_raw)
                    clean_target = self.preprocess_id(target_id_raw)

                    # Resolve 'gene'/'transcript'/'protein' by inspecting cleaned id prefix
                    def resolve_bio_type_from_id(cid):
                        if not cid:
                            return "unknown"
                        c = cid.lower()
                        if c.startswith("ensg"):
                            return "gene"
                        if c.startswith("enst"):
                            return "transcript"
                        if c.startswith("ensp"):
                            return "protein"
                        # fallback to trying ontology-derived type if provided
                        return None

                    # Generate edges for all source/target type combinations
                    for src_type in source_types:
                        for tgt_type in target_types:
                            src_type_final = src_type
                            tgt_type_final = tgt_type

                            # if the schema says 'ontology_term' or a generic 'gene' class,
                            # try to infer exact biological type from the ID
                            if isinstance(src_type, str) and src_type.lower() in ("ontology_term", "gene", "transcript", "protein"):
                                inferred = resolve_bio_type_from_id(clean_source)
                                if inferred:
                                    src_type_final = inferred
                            if isinstance(tgt_type, str) and tgt_type.lower() in ("ontology_term", "gene", "transcript", "protein"):
                                inferred = resolve_bio_type_from_id(clean_target)
                                if inferred:
                                    tgt_type_final = inferred

                            edge_label = edge_info.get("output_label", label)

                            # Build edge row using cleaned ids and filtered props
                            edge_row = {
                                "source_id": clean_source,
                                "target_id": clean_target,
                                "source_type": src_type_final,
                                "target_type": tgt_type_final,
                                "label": edge_label,
                                **{k: (json.dumps(v) if isinstance(v, list) else self.preprocess_value(v))
                                for k, v in filtered_props.items()}
                            }

                            # writer_key uses original label (schema key) for grouping
                            writer_key = self._init_edge_writer(label, src_type_final, tgt_type_final, edge_row, path_prefix, adapter_name)
                            self.temp_buffer[writer_key].append(edge_row)

                            if len(self.temp_buffer[writer_key]) >= self.batch_size:
                                self._write_buffer_to_temp(writer_key, self.temp_buffer[writer_key])

                except TypeError as e:
                    if "belongs to more than one entity types" in str(e):
                        logger.warning(f"Skipping conflicting edge {source_id_raw}->{target_id_raw} ({label}): {e}")
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