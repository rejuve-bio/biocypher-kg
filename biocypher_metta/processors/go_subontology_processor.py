"""
Gene Ontology Subontology Processor.

Maintains mappings between GO term IDs and their subontologies
(biological_process, molecular_function, cellular_component).

Data source: Gene Ontology OWL file (provided by OntologyAdapter)
Update strategy: Dependency-based (updates when GO.owl file changes)
"""

import rdflib
import json
from datetime import datetime
from typing import Dict, Any, Optional
from pathlib import Path
from biocypher._logger import logger
from .base_mapping_processor import BaseMappingProcessor


class GOSubontologyProcessor(BaseMappingProcessor):

    BIOLOGICAL_PROCESS = 'biological_process'
    MOLECULAR_FUNCTION = 'molecular_function'
    CELLULAR_COMPONENT = 'cellular_component'

    NAMESPACE_URI = 'http://www.geneontology.org/formats/oboInOwl#hasOBONamespace'

    def __init__(
        self,
        cache_dir: str = 'aux_files/hsa/go_subontology',
        dependency_file: Optional[str] = None
    ):
        super().__init__(
            name='go_subontology',
            cache_dir=cache_dir,
            update_interval_hours=None,
            dependency_file=dependency_file
        )

        self.graph = None
        self.namespace_predicate = rdflib.term.URIRef(self.NAMESPACE_URI)

    def set_graph(self, graph: rdflib.Graph):
        self.graph = graph

    def _read_dependency_signature(self) -> Optional[Dict[str, str]]:
        """
        Read stable dependency signature from go_meta.json-like files.

        Uses ontology `version` and `hash` fields (ignores mutable `date`).
        """
        if not self.dependency_file or not self.dependency_file.exists():
            return None

        try:
            with open(self.dependency_file, "r") as f:
                meta = json.load(f)

            version = meta.get("version")
            content_hash = meta.get("hash")

            if version is None and content_hash is None:
                return None

            return {
                "version": version,
                "hash": content_hash,
            }
        except Exception:
            return None

    def check_update_needed(self) -> bool:
        """
        Use dependency signature when available to avoid unnecessary rebuilds.

        This prevents re-generation when go_meta.json is rewritten but ontology
        version/hash are unchanged.
        """
        if not self.mapping_file.exists() or not self.version_file.exists():
            logger.info(f"{self.name}: Mapping or version file not found. Update needed.")
            return True

        version_info = self._load_version_info()
        if not version_info:
            logger.warning(f"{self.name}: Invalid version file. Update needed.")
            return True

        current_sig = self._read_dependency_signature()
        cached_sig = version_info.get("dependency_signature")

        if current_sig and cached_sig:
            if current_sig != cached_sig:
                logger.info(f"{self.name}: Dependency signature changed. Update needed.")
                return True

            logger.info(f"{self.name}: Dependency signature unchanged. Using cached mapping.")
            return False

        if self.dependency_file and self.dependency_file.exists() and "timestamp" in version_info:
            dep_mtime = datetime.fromtimestamp(self.dependency_file.stat().st_mtime)
            mapping_time = datetime.fromisoformat(version_info["timestamp"])

            if dep_mtime > mapping_time:
                logger.info(f"{self.name}: Dependency file is newer. Update needed.")
                return True

        logger.info(f"{self.name}: Using cached mapping.")
        return False

    def save_version_info(self):
        super().save_version_info()

        dependency_signature = self._read_dependency_signature()
        if not dependency_signature:
            return

        try:
            with open(self.version_file, "r") as f:
                version_info = json.load(f)

            version_info["dependency_signature"] = dependency_signature

            with open(self.version_file, "w") as f:
                json.dump(version_info, f, indent=2)
        except Exception as e:
            logger.warning(f"{self.name}: Could not persist dependency signature: {e}")

    def fetch_data(self) -> rdflib.Graph:
        if self.graph is None:
            raise ValueError(
                f"{self.name}: No RDF graph provided. "
                "Call set_graph() before updating."
            )
        return self.graph

    def process_data(self, graph: rdflib.Graph) -> Dict[str, str]:
        logger.info(f"{self.name}: Extracting GO term namespaces...")

        namespace_mapping = {}
        for subject, obj in graph.subject_objects(predicate=self.namespace_predicate):
            if isinstance(subject, rdflib.term.URIRef):
                go_id = self._uri_to_go_id(str(subject))
                if go_id:
                    namespace = str(obj)
                    if namespace in [self.BIOLOGICAL_PROCESS, self.MOLECULAR_FUNCTION, self.CELLULAR_COMPONENT]:
                        namespace_mapping[go_id] = namespace

        logger.info(f"{self.name}: Extracted {len(namespace_mapping)} GO term subontologies")

        bp_count = sum(1 for v in namespace_mapping.values() if v == self.BIOLOGICAL_PROCESS)
        mf_count = sum(1 for v in namespace_mapping.values() if v == self.MOLECULAR_FUNCTION)
        cc_count = sum(1 for v in namespace_mapping.values() if v == self.CELLULAR_COMPONENT)

        logger.info(f"{self.name}: Distribution - BP: {bp_count}, MF: {mf_count}, CC: {cc_count}")

        return namespace_mapping

    def _uri_to_go_id(self, uri: str) -> Optional[str]:
        if 'GO_' in uri:
            parts = uri.split('GO_')
            if len(parts) == 2:
                return f"GO:{parts[1]}"
        return None

    def get_subontology(self, go_id: str) -> Optional[str]:
        if not self.mapping:
            self.load_or_update()

        return self.mapping.get(go_id)

    def is_biological_process(self, go_id: str) -> bool:
        return self.get_subontology(go_id) == self.BIOLOGICAL_PROCESS

    def is_molecular_function(self, go_id: str) -> bool:
        return self.get_subontology(go_id) == self.MOLECULAR_FUNCTION

    def is_cellular_component(self, go_id: str) -> bool:
        return self.get_subontology(go_id) == self.CELLULAR_COMPONENT

    def filter_by_subontology(self, go_ids: list, subontology: str) -> list:
        if not self.mapping:
            self.load_or_update()

        return [go_id for go_id in go_ids if self.mapping.get(go_id) == subontology]
