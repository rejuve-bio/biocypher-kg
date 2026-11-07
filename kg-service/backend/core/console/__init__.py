"""Console: configuration + build management layer for the KG pipeline.

This package is strictly additive to (and independent of) the read-only
"Observatory" routes. It never imports ``neo4j_client``; its only shared
dependency is ``backend.core.config.settings``.

It also never imports the build CLI (``create_knowledge_graph.py``) into this
process — that module pulls in BioCypher and other heavy deps and has import
side effects. Instead:
  * cheap config introspection / validation reuses only ``load_yaml_with_includes``
  * actually building shells out to the CLI via subprocess (see ``job_runner``).
"""
