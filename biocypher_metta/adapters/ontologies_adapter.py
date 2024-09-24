import hashlib
import json
import rdflib
import requests
import os
import re
import tempfile
from datetime import datetime as dt, timedelta
from rdflib import Graph, URIRef
from owlready2 import *
from abc import ABC, abstractmethod
from biocypher_metta.adapters import Adapter
from xml.etree import ElementTree as ET

class OntologyAdapter(Adapter):
    HAS_PART = rdflib.term.URIRef('http://purl.obolibrary.org/obo/BFO_0000051')
    PART_OF = rdflib.term.URIRef('http://purl.obolibrary.org/obo/BFO_0000050')
    SUBCLASS = rdflib.term.URIRef('http://www.w3.org/2000/01/rdf-schema#subClassOf')
    DB_XREF = rdflib.term.URIRef('http://www.geneontology.org/formats/oboInOwl#hasDbXref')

    LABEL = rdflib.term.URIRef('http://www.w3.org/2000/01/rdf-schema#label')
    RESTRICTION = rdflib.term.URIRef('http://www.w3.org/2002/07/owl#Restriction')
    TYPE = rdflib.term.URIRef('http://www.w3.org/1999/02/22-rdf-syntax-ns#type')
    ON_PROPERTY = rdflib.term.URIRef('http://www.w3.org/2002/07/owl#onProperty')
    SOME_VALUES_FROM = rdflib.term.URIRef('http://www.w3.org/2002/07/owl#someValuesFrom')
    ALL_VALUES_FROM = rdflib.term.URIRef('http://www.w3.org/2002/07/owl#allValuesFrom')
    NAMESPACE = rdflib.term.URIRef('http://www.geneontology.org/formats/oboInOwl#hasOBONamespace')
    EXACT_SYNONYM = rdflib.term.URIRef('http://www.geneontology.org/formats/oboInOwl#hasExactSynonym')
    RELATED_SYNONYM = rdflib.term.URIRef('http://www.geneontology.org/formats/oboInOwl#hasRelatedSynonym')
    DESCRIPTION = rdflib.term.URIRef('http://purl.obolibrary.org/obo/IAO_0000115')

    PREDICATES = [SUBCLASS, DB_XREF]
    RESTRICTION_PREDICATES = [HAS_PART, PART_OF]

    def __init__(self, write_properties, add_provenance, ontology, type, label, dry_run=False, add_description=False, cache_dir=None, cache_expiration_days=30):
        self.cache_dir = cache_dir
        self.cache_expiration_days = cache_expiration_days
        if self.cache_dir and not os.path.exists(self.cache_dir):
            os.makedirs(self.cache_dir)
        self.type = type
        self.label = label
        self.dry_run = dry_run
        self.graph = None
        self.cache = {}
        self.ontology = ontology
        self.add_description = add_description

        # Set source and source_url based on the ontology
        self.source, self.source_url = self.get_ontology_source()

        super(OntologyAdapter, self).__init__(write_properties, add_provenance)
    @abstractmethod
    def get_ontology_source(self):
        """
        Returns the source and source URL for a given ontology.
        This method should be overridden in child classes for specific ontologies.
        """
        pass

    def update_graph(self):
        if self.ontology not in self.ONTOLOGIES:
            raise ValueError(f"Ontology '{self.ontology}' is not defined in this adapter.")

        ontology_url = self.ONTOLOGIES[self.ontology]
        use_cached = False
        cached_path = None
        meta = None

        if self.cache_dir:
            cached_path = os.path.join(self.cache_dir, f"{self.ontology}.owl")
            meta_path = os.path.join(self.cache_dir, f"{self.ontology}_meta.json")

            if os.path.exists(cached_path) and os.path.exists(meta_path):
                with open(meta_path, 'r') as f:
                    meta = json.load(f)
        
                cached_date = dt.fromisoformat(meta['date'])
                cache_expired = dt.now() - cached_date > timedelta(days=self.cache_expiration_days)
        
                remote_version = self._get_remote_version()
                current_version = meta.get('version')

                print(f"Cache status: Expired: {cache_expired}, Remote version: {remote_version}, Current version: {current_version}")

                if remote_version is None or current_version is None or current_version == "unknown":
                    if not cache_expired:
                        use_cached = True
                        print("Using cached data as version information is incomplete and cache is not expired")
                    else:
                        print("Cache has expired and version information is incomplete. Updating data.")
                elif remote_version == current_version and not cache_expired:
                    use_cached = True
                else:
                    print(f"Not using cache: Expired: {cache_expired}, New version available: {remote_version != current_version}")

        if use_cached:
            print(f"Using cached ontology from {cached_path}")
            onto = get_ontology(cached_path).load()
            self.version = meta.get('version', 'unknown')
        else:
            print(f"Downloading ontology from {ontology_url}")
            onto = get_ontology(ontology_url).load()

        # Explicitly initialize the graph, whether cached or fresh
        self.graph = default_world.as_rdflib_graph()
    
        if not use_cached:
            # Extract version information
            self._extract_version_info()

            if self.cache_dir:
                print(f"Caching ontology to {cached_path}")
                onto.save(cached_path)

                # Calculate and store metadata
                meta = {
                    'date': dt.now().isoformat(),
                    'url': ontology_url,
                    'hash': self._calculate_file_hash(cached_path),
                    'version': self.version
                }
                with open(meta_path, 'w') as f:
                    json.dump(meta, f)

        self.clear_cache()
    
        # Verify that the graph is initialized
        if self.graph is None:
            raise ValueError("Failed to initialize graph from ontology")

        print(f"Graph initialized with {len(self.graph)} triples")

    def _calculate_file_hash(self, file_path):
        """Calculate MD5 hash of a file."""
        hash_md5 = hashlib.md5()
        with open(file_path, "rb") as f:
            for chunk in iter(lambda: f.read(4096), b""):
                hash_md5.update(chunk)
        return hash_md5.hexdigest()

    def _extract_version_info(self):
        if not self.graph:
            print("Warning: Graph is not initialized. Unable to extract version information.")
            self.version = "unknown"
            return

        try:
            # First, try to get the version from the RDF graph
            version_iri = self.graph.value(predicate=URIRef("http://www.w3.org/2002/07/owl#versionIRI"))
            if version_iri:
                match = re.search(r'/(\d{4}-\d{2}-\d{2})/', str(version_iri))
                if match:
                    self.version = match.group(1)
                else:
                    self.version = str(version_iri).split('/')[-2]
            else:
                # If not found in the graph, try parsing the XML directly
                ontology_url = self.ONTOLOGIES[self.ontology]
                response = requests.get(ontology_url)
                response.raise_for_status()
                
                # Parse the XML content
                root = ET.fromstring(response.content)
                
                # Find the owl:versionIRI element
                namespaces = {'owl': 'http://www.w3.org/2002/07/owl#', 'rdf': 'http://www.w3.org/1999/02/22-rdf-syntax-ns#'}
                version_iri_elem = root.find(".//owl:versionIRI", namespaces)
                
                if version_iri_elem is not None:
                    version_iri = version_iri_elem.get('{http://www.w3.org/1999/02/22-rdf-syntax-ns#}resource')
                    match = re.search(r'/(\d{4}-\d{2}-\d{2})/', version_iri)
                    if match:
                        self.version = match.group(1)
                    else:
                        version_match = re.search(r'/releases/v?([\d.]+)/', version_iri)
                        if version_match:
                            self.version = version_match.group(1)  # Store version without 'v' prefix
                        else:
                            self.version = version_iri.split('/')[-2]
                else:
                    self.version = "unknown"
        except Exception as e:
            print(f"Error extracting version information: {e}")
            self.version = "unknown"

        # Ensure the version is stored without the 'v' prefix
        self.version = self.version.lstrip('v')
        print(f"Ontology version: {self.version}")

    def _is_new_version_available(self, meta):
        """Check if a new version is available based on the remote version."""
        remote_version = self._get_remote_version()
        current_version = meta.get('version')
    
        print(f"Checking versions - Remote: {remote_version}, Current: {current_version}")
    
        if remote_version is None or current_version is None or current_version == "unknown":
            print("Version information is incomplete. Checking cache expiration.")
            cached_date = dt.fromisoformat(meta['date'])
            if dt.now() - cached_date > timedelta(days=self.cache_expiration_days):
                print("Cache has expired. Updating data.")
                return True
            else:
                print("Using cached data as version information is incomplete and cache is not expired")
                return False
    
        # Remove 'v' prefix if present for consistent comparison
        remote_version = remote_version.lstrip('v')
        current_version = current_version.lstrip('v')
    
        return remote_version != current_version

    def _get_remote_version(self):
        """Retrieve the version of the remote ontology."""
        ontology_url = self.ONTOLOGIES[self.ontology]

        try:
            # Make a GET request to fetch the ontology file
            response = requests.get(ontology_url)
            response.raise_for_status()

            # Parse the XML content
            root = ET.fromstring(response.content)

            # Find the version IRI tag in the ontology
            version_iri = root.find(".//{http://www.w3.org/2002/07/owl#}versionIRI")
            if version_iri is not None:
                version_url = version_iri.get("{http://www.w3.org/1999/02/22-rdf-syntax-ns#}resource")
            
                # Extract the version date from the IRI
                version_match = re.search(r'/releases/(\d{4}-\d{2}-\d{2})/', version_url)
                if version_match:
                    return version_match.group(1)
                
                # Handle the version format with or without 'v' prefix
                version_match = re.search(r'/releases/v?([\d.]+)/', version_url)
                if version_match:
                    return version_match.group(1)  # Return version without 'v' prefix

            print("No version IRI found or unrecognized format.")
            return None

        except requests.exceptions.HTTPError as e:
            print(f"HTTP error occurred: {e}")
        except Exception as e:
            print(f"An error occurred: {e}")

        return None

    def check_for_updates(self):
        """Check if there's a new version available or if the cache has expired."""
        if not self.cache_dir:
            return True  # Always update if there's no cache

        meta_path = os.path.join(self.cache_dir, f"{self.ontology}_meta.json")
        if not os.path.exists(meta_path):
            return True  # Update if no metadata exists

        with open(meta_path, 'r') as f:
            meta = json.load(f)

        return self._is_new_version_available(meta)

    def get_nodes(self):
        self.update_graph()
        self.cache_node_properties()

        nodes = self.graph.all_nodes()

        i = 0  # dry run is set to true just output the first 1000 nodes
        for node in nodes:
            if i > 100 and self.dry_run:
                break
            # avoiding blank nodes and other arbitrary node types
            if not isinstance(node, rdflib.term.URIRef):
                continue
            
            # term_id = str(node).split('/')[-1]
            term_id = OntologyAdapter.to_key(node)
            # 'uri': str(node),
            term_name = ', '.join(self.get_all_property_values_from_node(node, 'term_names'))
            synonyms = self.get_all_property_values_from_node(node, 'related_synonyms') + self.get_all_property_values_from_node(node, 'exact_synonyms')

            props = {}
            if self.write_properties:
                props['term_name'] = term_name
                props['synonyms'] = synonyms

                if self.add_description:
                    description = ' '.join(self.get_all_property_values_from_node(node, 'descriptions'))
                    props['description'] = description

                if self.add_provenance:
                    props['source'] = self.source
                    props['source_url'] = self.source_url
            i += 1
            yield term_id, self.label, props

    def get_edges(self):
        self.update_graph()
        self.cache_edge_properties()

        for predicate in OntologyAdapter.PREDICATES:
            edges = list(self.graph.subject_objects(predicate=predicate, unique=True))
            i = 0  # dry run is set to true just output the first 100 relationships
            for edge in edges:
                if i > 100 and self.dry_run:
                    break
                from_node, to_node = edge

                if self.is_blank(from_node):
                    continue

                if self.is_blank(to_node) and self.is_a_restriction_block(to_node):
                    restriction_predicate, restriction_node = self.read_restriction_block(to_node)
                    if restriction_predicate is None or restriction_node is None:
                        continue

                    predicate = restriction_predicate
                    to_node = restriction_node

                if self.type == 'edge':
                    from_node_key = OntologyAdapter.to_key(from_node)
                    predicate_key = OntologyAdapter.to_key(predicate)
                    to_node_key = OntologyAdapter.to_key(to_node)

                    if predicate == OntologyAdapter.DB_XREF:
                        if to_node.__class__ == rdflib.term.Literal:
                            if str(to_node) == str(from_node):
                                print('Skipping self xref for: ' + from_node_key)
                                continue

                            # only accepting IDs in the form <ontology>:<ontology_id>
                            if len(str(to_node).split(':')) != 2:
                                print('Unsupported format for xref: ' + str(to_node))
                                continue

                            to_node_key = str(to_node).replace(':', '_')

                            if from_node_key == to_node_key:
                                print('Skipping self xref for: ' + from_node_key)
                                continue
                        else:
                            print('Ignoring non-literal xref: {}'.format(str(to_node)))
                            continue

                    predicate_name = self.predicate_name(predicate)
                    if predicate_name == 'dbxref':
                        continue  
                    props = {}
                    if self.write_properties:
                        props['rel_type'] = predicate_name
                        if self.add_provenance:
                            props['source'] = self.source
                            props['source_url'] = self.source_url

                    yield from_node_key, to_node_key, self.label, props
                    i += 1

    def predicate_name(self, predicate):
        predicate = str(predicate)
        if predicate == str(OntologyAdapter.HAS_PART):
            return 'has_part'
        elif predicate == str(OntologyAdapter.PART_OF):
            return 'part_of'
        elif predicate == str(OntologyAdapter.SUBCLASS):
            return 'subclass'
        elif predicate == str(OntologyAdapter.DB_XREF):
            return 'dbxref'
        return ''
    
    # "http://purl.obolibrary.org/obo/CLO_0027762#subclass?id=123" => "CLO_0027762.subclass_id=123"
    # "12345" => "number_12345" - there are cases where URIs are just numbers, e.g. HPO

    @classmethod
    def to_key(cls, node_uri):
        key = str(node_uri).split('/')[-1]
        key = key.replace('#', '.').replace('?', '_')
        key = key.replace('&', '.').replace('=', '_')
        key = key.replace('/', '_').replace('~', '.')
        key = key.replace('_', ':')
        key = key.replace(' ', '')

        if key.replace('.', '').isnumeric():
            key = '{}_{}'.format('number', key)

        return key
    
    # Example of a restriction block:
    # <rdfs:subClassOf>
    #     <owl:Restriction>
    #         <owl:onProperty rdf:resource="http://purl.obolibrary.org/obo/RO_0001000"/>
    #         <owl:someValuesFrom rdf:resource="http://purl.obolibrary.org/obo/CL_0000056"/>
    #     </owl:Restriction>
    # </rdfs:subClassOf>
    # This block must be interpreted as the triple (s, p, o):
    # (parent object, http://purl.obolibrary.org/obo/RO_0001000, http://purl.obolibrary.org/obo/CL_0000056)

    
    def is_a_restriction_block(self, node):
        node_type = self.get_all_property_values_from_node(node, 'node_types')
        return node_type and node_type[0] == OntologyAdapter.RESTRICTION

    def read_restriction_block(self, node):
        restricted_property = self.get_all_property_values_from_node(node, 'on_property')
        
        # assuming a restriction block will always contain only one `owl:onProperty` triple
        if restricted_property and restricted_property[0] not in OntologyAdapter.RESTRICTION_PREDICATES:
            return None, None

        restriction_predicate = str(restricted_property[0])
        
        # returning the pair (owl:onProperty value, owl:someValuesFrom or owl:allValuesFrom value)
        # assuming a owl:Restriction block in a rdf:subClassOf will contain only one `owl:someValuesFrom` or `owl:allValuesFrom` triple
        some_values_from = self.get_all_property_values_from_node(node, 'some_values_from')
        if some_values_from:
            return (restriction_predicate, some_values_from[0])

        all_values_from = self.get_all_property_values_from_node(node, 'all_values_from')
        if all_values_from:
            return (restriction_predicate, all_values_from[0])

        return (None, None)
    
    def is_blank(self, node):
        # a BNode according to rdflib is a general node (as a 'catch all' node) that doesn't have any type such as Class, Literal, etc.
        BLANK_NODE = rdflib.term.BNode
        return isinstance(node, BLANK_NODE)
    
    # it's faster to load all subject/objects beforehand
    def clear_cache(self):
        self.cache = {}

    def cache_edge_properties(self):
        for predicate in OntologyAdapter.PREDICATES:
            self.cache_predicate(predicate=predicate)

    def cache_node_properties(self):
        self.cache_predicate(predicate=OntologyAdapter.LABEL, collection='term_names')
        self.cache_predicate(predicate=OntologyAdapter.NAMESPACE, collection='namespaces')
        self.cache_predicate(predicate=OntologyAdapter.DESCRIPTION, collection='descriptions')
        self.cache_predicate(predicate=OntologyAdapter.RELATED_SYNONYM, collection='related_synonyms')
        self.cache_predicate(predicate=OntologyAdapter.EXACT_SYNONYM, collection='exact_synonyms')
        self.cache_predicate(predicate=OntologyAdapter.TYPE, collection='node_types')
        self.cache_predicate(predicate=OntologyAdapter.ON_PROPERTY, collection='on_property')
        self.cache_predicate(predicate=OntologyAdapter.SOME_VALUES_FROM, collection='some_values_from')

    def cache_predicate(self, predicate, collection=None):
        triples = list(self.graph.subject_objects(predicate=predicate, unique=True))
        for triple in triples:
            s, o = triple
            s_key = OntologyAdapter.to_key(s)

            if s_key not in self.cache:
                self.cache[s_key] = {}

            if not collection:
                self.cache[s_key][predicate] = o
                continue

            if collection not in self.cache[s_key]:
                self.cache[s_key][collection] = []

            self.cache[s_key][collection].append(o)

    def get_all_property_values_from_node(self, node, collection):
        node_key = OntologyAdapter.to_key(node)
        return self.cache.get(node_key, {}).get(collection, [])


