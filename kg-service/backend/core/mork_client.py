"""
MORK Client for API
Wraps MORK operations for API endpoints
"""

import json
import importlib
import importlib.util
from pathlib import Path
from backend.core.config import settings


def _load_mork_class():
    """Load the MORK client class.

    The client is vendored at <repo_root>/biocypher-mork/client.py (the same
    module every loader imports via ``from client import MORK``). We load it
    directly by path under a unique module name to avoid depending on sys.path
    order or colliding with the generic ``client`` module name. As a fallback
    we accept an installed ``mork`` package that exposes ``MORK``.
    """
    # kg-service/backend/core/mork_client.py -> parents[3] is the repo root
    # (locally) and /app (in the Docker image, where `COPY . .` lands).
    repo_root = Path(__file__).resolve().parents[3]
    client_py = repo_root / "biocypher-mork" / "client.py"
    if client_py.is_file():
        spec = importlib.util.spec_from_file_location("biocypher_mork_client", client_py)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        return module.MORK
    try:
        mork_module = importlib.import_module("mork")
        return getattr(mork_module, "MORK")
    except Exception as exc:
        raise ImportError(
            f"Could not import the MORK client. Looked for the vendored client at "
            f"{client_py} and for an installed 'mork' package, but neither is available."
        ) from exc


class MORKClient:
    """Client for MORK database operations"""

    def __init__(self, mork_url: str = None, archive_base: str = None):
        if mork_url is None:
            mork_url = settings.MORK_URL
        if archive_base is None:
            archive_base = settings.ARCHIVE_BASE

        mork_cls = _load_mork_class()

        self.mork_url = mork_url
        self.server = mork_cls(mork_url)
        self.archive_dir = Path(archive_base) / "mork"
        self.metadata_file = self.archive_dir / "version_metadata.json"
    
    def get_latest_version(self):
        """Get latest version from metadata file"""
        if not self.metadata_file.exists():
            return None
        
        try:
            with open(self.metadata_file, 'r') as f:
                data = json.load(f)
            
            return {
                "version": data.get("atomspace_version"),
                "build_id": data.get("build_id"),
                "timestamp": data.get("build_timestamp"),
                "datasets": data.get("dataset_versions", {})
            }
        except Exception as e:
            return None
    
    def get_all_versions(self):
        """Get all versions from archives"""
        if not self.archive_dir.exists():
            return []
        
        versions = set()
        for dataset_dir in self.archive_dir.iterdir():
            if dataset_dir.is_dir() and dataset_dir.name != "version_metadata.json":
                for version_dir in dataset_dir.iterdir():
                    if version_dir.is_dir():
                        versions.add(version_dir.name)
        
        # Sort versions (v1, v2, v3, ...)
        sorted_versions = sorted(versions, key=lambda x: int(x.replace('v', '')))
        return sorted_versions
    
    def get_dataset_count(self):
        """Get number of datasets in latest version"""
        latest = self.get_latest_version()
        if latest and latest.get("datasets"):
            return len(latest["datasets"])
        return 0
    
    def get_atom_count(self):
        """Approximate atom count from the live MORK server.

        This actually talks to the server, so it doubles as the reachability
        probe. A connection error propagates to the caller (reported as
        offline/unreachable) rather than being swallowed into a misleading 0.
        An empty-but-reachable atomspace legitimately returns 0.
        """
        with self.server.work_at("annotation") as scope:
            data = scope.download_(max_results=1000)
            data.block()
            if data.data:
                lines = [l for l in data.data.split('\n') if l.strip() and not l.strip().startswith(';')]
                return len(lines)
        return 0
    
    def _download_projected(self, pattern, template, max_results):
        """Read-only pattern projection against the live atomspace.

        Returns the matched lines (with duplicates). The projected templates only
        ever emit clean type/label/property symbols, so callers dedupe locally.
        """
        with self.server.work_at("annotation") as scope:
            cmd = scope.download(pattern, template, max_results=max_results)
            cmd.block()
            return [l.strip() for l in (cmd.data or "").split("\n") if l.strip()]

    @staticmethod
    def _tokens(line):
        """Split a projected atom line into its symbols.

        Projected outputs are either a bare symbol (`exon`) or a flat paren list
        of clean symbols (`(part_of exon transcript)`), so whitespace splitting
        after stripping the outer parens is sufficient. We never project ids or
        values (which could contain spaces), only schema-level symbols.
        """
        line = line.strip()
        if line.startswith("(") and line.endswith(")"):
            line = line[1:-1]
        return line.split()

    def get_schema(self, max_results: int = 20000):
        """Derive a node/edge schema from the live MORK atomspace.

        MORK is an atomspace of MeTTa atoms, not a labeled property graph, so it
        has no native schema. But the KG atoms are regular:

            node        (<type> <id>)
            node prop   (<prop> (<type> <id>) <value>)
            edge        (<label> (<srcType> <srcId>) (<tgtType> <tgtId>))

        so node types, edge types + their connections, and per-type properties
        are all recoverable by projecting each shape and deduping. Returns the
        same ``{"nodes": [...], "edges": [...]}`` shape the Neo4j schema uses,
        plus a ``sampled`` flag when any projection hit ``max_results`` (rare
        types may then be missing from a very large space).
        """
        from collections import defaultdict

        edge_lines = self._download_projected("($rel ($st $sx) ($tt $tx))", "($rel $st $tt)", max_results)
        node_lines = self._download_projected("($t $id)", "$t", max_results)
        prop_lines = self._download_projected("($prop ($t $id) $val)", "($t $prop)", max_results)

        sampled = any(len(lines) >= max_results for lines in (edge_lines, node_lines, prop_lines))

        # Edges: distinct (label, srcType, tgtType).
        edge_triples = set()
        for line in edge_lines:
            parts = self._tokens(line)
            if len(parts) == 3:
                edge_triples.add(tuple(parts))
        edge_labels = {rel for rel, _, _ in edge_triples}

        # Node types: declarations, plus any type that only appears as an endpoint.
        node_types = {t for t in node_lines if t}
        for _, st, tt in edge_triples:
            node_types.add(st)
            node_types.add(tt)

        # Properties per type. The (prop (type id) value) shape also matches edges
        # (label (src) (tgt)), so drop any "property" whose name is an edge label.
        props_by_type = defaultdict(set)
        for line in prop_lines:
            parts = self._tokens(line)
            if len(parts) == 2:
                t, prop = parts
                if prop not in edge_labels:
                    props_by_type[t].add(prop)

        nodes = [
            {"data": {"id": t, "properties": sorted(props_by_type.get(t, []))}}
            for t in sorted(node_types)
        ]

        connections = defaultdict(set)
        for rel, st, tt in edge_triples:
            connections[(st, tt)].add(rel)
        edges = [
            {"data": {"source": s, "target": t, "possible_connections": sorted(rels)}}
            for (s, t), rels in sorted(connections.items())
        ]

        return {
            "schema": {"nodes": nodes, "edges": edges},
            "node_type_count": len(node_types),
            "edge_type_count": len(edge_labels),
            "sampled": sampled,
        }

    # ===== INSTANCE-LEVEL EXPLORATION =====

    _SAFE_SYMBOL = None  # compiled lazily below

    @staticmethod
    def _is_safe_symbol(s: str) -> bool:
        """A MeTTa symbol we can safely interpolate into a pattern.

        We only ever interpolate type names and node ids (e.g. `gene`,
        `CL:0000000`, `ENSG00000141510`). Anything with whitespace or parens
        could change the pattern's shape, so we refuse it.
        """
        return bool(s) and not any(c in s for c in " \t\n()")

    @staticmethod
    def _split_triple(line: str):
        """Parse `(id prop value...)` keeping the (possibly nested) value intact."""
        s = line.strip()
        if s.startswith("(") and s.endswith(")"):
            s = s[1:-1].strip()
        parts = s.split(None, 2)
        if len(parts) < 2:
            return None
        return parts[0], parts[1], (parts[2] if len(parts) == 3 else "")

    def get_instances(self, node_type: str, limit: int = 50, scan: int = 8000):
        """List instances of a node type with their scalar/list properties.

        Read-only: projects `(<prop> (<type> $id) $val)` and groups by id.
        Outgoing edges share that shape, so we drop any "property" whose name is
        an outgoing edge label. Returns up to ``limit`` instances plus whether
        more exist within the scan window.
        """
        if not self._is_safe_symbol(node_type):
            raise ValueError(f"Unsafe node type: {node_type!r}")

        edge_labels = set(
            self._download_projected(f"($rel ({node_type} $id) ($tt $tid))", "$rel", scan)
        )
        prop_lines = self._download_projected(f"($p ({node_type} $id) $v)", "($id $p $v)", scan)
        bare_ids = self._download_projected(f"({node_type} $id)", "$id", scan)

        order = []
        props = {}
        for line in prop_lines:
            parsed = self._split_triple(line)
            if not parsed:
                continue
            ident, prop, value = parsed
            if prop in edge_labels:
                continue
            if ident not in props:
                props[ident] = {}
                order.append(ident)
            props[ident][prop] = value
        for ident in bare_ids:
            if ident not in props:
                props[ident] = {}
                order.append(ident)

        instances = [{"id": i, "properties": props.get(i, {})} for i in order[:limit]]
        return {
            "type": node_type,
            "instances": instances,
            "returned": len(instances),
            "has_more": len(order) > limit,
            "scanned": max(len(prop_lines), len(bare_ids)) >= scan,
        }

    def get_neighbors(self, node_type: str, node_id: str, limit: int = 50):
        """Immediate neighborhood of one instance: outgoing/incoming edges + props."""
        if not self._is_safe_symbol(node_type) or not self._is_safe_symbol(node_id):
            raise ValueError("Unsafe node type or id")

        anchor = f"({node_type} {node_id})"
        out_lines = self._download_projected(f"($rel {anchor} ($tt $tid))", "($rel $tt $tid)", limit + 1)
        in_lines = self._download_projected(f"($rel ($st $sid) {anchor})", "($rel $st $sid)", limit + 1)
        prop_lines = self._download_projected(f"($p {anchor} $v)", "($p $v)", 500)

        edge_labels = set()
        outgoing = []
        for line in out_lines[:limit]:
            parts = self._tokens(line)
            if len(parts) == 3:
                rel, tt, tid = parts
                edge_labels.add(rel)
                outgoing.append({"rel": rel, "target_type": tt, "target_id": tid})
        incoming = []
        for line in in_lines[:limit]:
            parts = self._tokens(line)
            if len(parts) == 3:
                rel, st, sid = parts
                incoming.append({"rel": rel, "source_type": st, "source_id": sid})

        # Properties project to `(prop value...)`; keep the value (possibly nested)
        # intact and drop outgoing edges that share the shape.
        properties = {}
        for line in prop_lines:
            s = line.strip()
            if s.startswith("(") and s.endswith(")"):
                s = s[1:-1].strip()
            bits = s.split(None, 1)
            if not bits:
                continue
            prop = bits[0]
            value = bits[1] if len(bits) == 2 else ""
            if prop in edge_labels:
                continue
            properties[prop] = value

        return {
            "type": node_type,
            "id": node_id,
            "properties": properties,
            "outgoing": outgoing,
            "incoming": incoming,
            "out_truncated": len(out_lines) > limit,
            "in_truncated": len(in_lines) > limit,
        }

    def run_pattern(self, pattern: str, template: str = None, limit: int = 200, distinct: bool = True):
        """Run a read-only pattern download against the live atomspace.

        ``download`` never mutates the space (unlike ``transform``/``query``),
        and the pattern is scoped to the ``annotation`` namespace. It streams one
        row per matching atom with no dedup, so a template coarser than the
        pattern (e.g. projecting every edge to its type triple) yields the same
        row many times. With ``distinct`` (the default) we scan a wider window
        and dedupe, so schema-shaped queries return the handful of real rows;
        the raw match count is reported separately.
        """
        template = template or pattern
        scan = max(limit, 5000) if distinct else limit
        raw = self._download_projected(pattern, template, scan + 1)
        capped = len(raw) > scan
        raw = raw[:scan]
        matched = len(raw)

        if distinct:
            seen = set()
            rows = []
            for r in raw:
                if r not in seen:
                    seen.add(r)
                    rows.append(r)
            truncated = capped or len(rows) > limit
            rows = rows[:limit]
        else:
            rows = raw[:limit]
            truncated = capped or matched > limit

        parsed = [self._tokens(r) for r in rows]
        return {
            "rows": rows,
            "parsed": parsed,
            "count": len(rows),
            "matched": matched,
            "distinct": distinct,
            "truncated": truncated,
        }

    def get_raw_atoms(self, limit: int = 200):
        """A sample of raw atoms for the S-expression tree view."""
        lines = self._download_projected("$x", "$x", limit + 1)
        truncated = len(lines) > limit
        return {"atoms": lines[:limit], "truncated": truncated}

    def compare_versions(self, version1: str, version2: str, dataset: str = None):
        """Compare two versions"""
        # This will use the version_diff.py logic
        # For now, return basic info
        return {
            "version1": version1,
            "version2": version2,
            "dataset": dataset,
            "message": "Comparison available via version_diff.py CLI"
        }
    
    def get_archives(self):
        """List all archived datasets"""
        if not self.archive_dir.exists():
            return []
        
        archives = []
        for dataset_dir in self.archive_dir.iterdir():
            if dataset_dir.is_dir() and dataset_dir.name != "version_metadata.json":
                dataset_versions = []
                for version_dir in dataset_dir.iterdir():
                    if version_dir.is_dir():
                        # Count files
                        file_count = len(list(version_dir.rglob("*.metta")))
                        dataset_versions.append({
                            "version": version_dir.name,
                            "file_count": file_count,
                            "path": str(version_dir)
                        })
                
                # Sort by version
                dataset_versions.sort(key=lambda x: int(x['version'].replace('v', '')))
                
                archives.append({
                    "dataset": dataset_dir.name,
                    "versions": dataset_versions
                })
        
        return archives