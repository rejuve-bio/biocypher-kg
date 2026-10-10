#!/bin/bash
# Switch the local Neo4j viewer (docker/neo4j.env) to a different species'
# connected sample build. Only one species fits in the container at a time —
# this wipes the current load and loads the requested one instead.
#
# Usage: scripts/switch_species.sh <hsa|dmel|mmu|rno|cel>
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"

VALID_SPECIES=(hsa dmel mmu rno cel)
SPECIES="${1:-}"

if [ -z "$SPECIES" ]; then
  echo "Usage: $0 <${VALID_SPECIES[*]// /|}>" >&2
  exit 1
fi

valid=false
for s in "${VALID_SPECIES[@]}"; do
  [ "$s" = "$SPECIES" ] && valid=true
done
if [ "$valid" != "true" ]; then
  echo "Unknown species '$SPECIES'. Must be one of: ${VALID_SPECIES[*]}" >&2
  exit 1
fi

OUTPUT_DIR="$REPO_ROOT/output_${SPECIES}_sample"

if [ ! -d "$OUTPUT_DIR" ]; then
  echo "No build found at $OUTPUT_DIR -- build it first with create_knowledge_graph.py" >&2
  exit 1
fi

# Point the Neo4j container at this species' build output
sed -i "s|^NEO4J_OUTPUT_DIR=.*|NEO4J_OUTPUT_DIR=${OUTPUT_DIR}|" docker/neo4j.env

# Wipe the container's data volume and restart fresh (required -- the loader
# doesn't merge across species)
DOCKER_API_VERSION=1.44 make neo4j-reset

# Wait for Neo4j to finish starting
ready=false
for i in $(seq 1 30); do
  if docker logs biocypher_neo4j-neo4j-1 2>&1 | grep -q "Started."; then
    ready=true
    break
  fi
  sleep 2
done
if [ "$ready" != "true" ]; then
  echo "Neo4j did not report ready within 60s -- check: docker logs biocypher_neo4j-neo4j-1" >&2
  exit 1
fi

# Load the CSVs
set -a; . docker/neo4j.env; set +a
uv run python scripts/neo4j_loader.py --env-file docker/neo4j.env

echo ""
echo "Ready: http://localhost:${NEO4J_HTTP_PORT:-7674}/browser/  (${NEO4J_AUTH:-neo4j/<see docker/neo4j.env>})"
