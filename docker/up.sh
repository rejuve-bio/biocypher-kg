#!/usr/bin/env bash
# docker/up.sh — bring up the BioCypher KG Console, attaching to existing Neo4j/MORK
# instances or creating bundled ones as needed.
#
# For EACH of Neo4j and MORK, the launcher decides in this order:
#   1. our bundled service is already running   -> reuse it (compose service name)
#   2. a remote host is configured (NEO4J_HOST / MORK_HOST is non-local)
#                                               -> attach to it (error if unreachable;
#                                                  we can't create a DB on another box)
#   3. something is already listening on the local port
#                                               -> attach via host.docker.internal
#   4. nothing is there                         -> create the bundled service
# ...then bring up the console pointing at whatever was chosen.
#
# Config comes from docker/console.env:
#   NEO4J_HOST (blank = this host), NEO4J_BOLT_PORT (default 7687)
#   MORK_HOST  (blank = this host), MORK_HOST_PORT  (default 8432)
# Extra args are passed through to `docker compose up` (e.g. --build, --force-recreate).
set -euo pipefail

cd "$(dirname "$0")/.."
ENV_FILE="docker/console.env"
COMPOSE_FILE="docker/docker-compose.yml"
COMPOSE=(docker compose --env-file "$ENV_FILE" -f "$COMPOSE_FILE")

if [ ! -f "$ENV_FILE" ]; then
  echo "✗ Missing $ENV_FILE — copy the example first:" >&2
  echo "    cp docker/console.env.example $ENV_FILE" >&2
  exit 1
fi

# Load env so we can read host/port config (and pass it on to compose).
set -a; . "$ENV_FILE"; set +a

NEO4J_HOST="${NEO4J_HOST:-}"
NEO4J_BOLT_PORT="${NEO4J_BOLT_PORT:-7687}"
MORK_HOST="${MORK_HOST:-}"
MORK_HOST_PORT="${MORK_HOST_PORT:-8432}"

is_local() {  # blank / loopback counts as "this host"
  case "$1" in ""|localhost|127.0.0.1|0.0.0.0|::1) return 0 ;; *) return 1 ;; esac
}

port_open() {  # host port  → 0 if a TCP connection succeeds within 2s
  timeout 2 bash -c "exec 3<>/dev/tcp/$1/$2" 2>/dev/null
}

port_taken() {  # host port → 0 if ANY container publishes it, or something answers TCP
  # Docker refuses to bind a host port that any container already maps — even one from
  # another project (e.g. a standalone MORK). The TCP probe alone misses that (the app
  # may not be answering on 127.0.0.1 at probe time), so we'd wrongly try to create a
  # bundled service and hit "port is already allocated". Checking `docker ps --filter
  # publish` matches Docker's own view, so we attach instead of colliding.
  [ -n "$(docker ps --filter "publish=$1" --format '{{.ID}}' 2>/dev/null)" ] && return 0
  port_open 127.0.0.1 "$1"
}

service_running() {  # compose service name → 0 if it's up in THIS project
  # Enable both profiles for the query so profiled services (neo4j/mork) that are
  # already running are listed — `ps` otherwise filters to active profiles only.
  "${COMPOSE[@]}" --profile neo4j --profile mork ps --status running --services 2>/dev/null \
    | grep -qx "$1"
}

PROFILES=()

# ── Neo4j ────────────────────────────────────────────────────────────────────
if service_running neo4j; then
  echo "✔ Neo4j: reusing the bundled service already running"
  PROFILES+=(--profile neo4j)
  export NEO4J_URI="bolt://neo4j:7687"
elif ! is_local "$NEO4J_HOST"; then
  echo "→ Neo4j: attaching to remote ${NEO4J_HOST}:${NEO4J_BOLT_PORT}"
  port_open "$NEO4J_HOST" "$NEO4J_BOLT_PORT" \
    || { echo "✗ Neo4j unreachable at ${NEO4J_HOST}:${NEO4J_BOLT_PORT} (can't create it remotely)"; exit 1; }
  export NEO4J_URI="bolt://${NEO4J_HOST}:${NEO4J_BOLT_PORT}"
elif port_taken "$NEO4J_BOLT_PORT"; then
  owner=$(docker ps --filter "publish=$NEO4J_BOLT_PORT" --format '{{.Names}}' 2>/dev/null | head -1)
  echo "✔ Neo4j: attaching to existing instance on localhost:${NEO4J_BOLT_PORT}${owner:+ (container: $owner)}"
  export NEO4J_URI="bolt://host.docker.internal:${NEO4J_BOLT_PORT}"
else
  echo "＋ Neo4j: none found on :${NEO4J_BOLT_PORT} — creating the bundled service"
  PROFILES+=(--profile neo4j)
  export NEO4J_URI="bolt://neo4j:7687"
fi

# ── MORK ─────────────────────────────────────────────────────────────────────
if service_running mork; then
  echo "✔ MORK: reusing the bundled service already running"
  PROFILES+=(--profile mork)
  export MORK_URL="http://mork:8027"
elif ! is_local "$MORK_HOST"; then
  echo "→ MORK: attaching to remote ${MORK_HOST}:${MORK_HOST_PORT}"
  port_open "$MORK_HOST" "$MORK_HOST_PORT" \
    || { echo "✗ MORK unreachable at ${MORK_HOST}:${MORK_HOST_PORT} (can't create it remotely)"; exit 1; }
  export MORK_URL="http://${MORK_HOST}:${MORK_HOST_PORT}"
elif port_taken "$MORK_HOST_PORT"; then
  owner=$(docker ps --filter "publish=$MORK_HOST_PORT" --format '{{.Names}}' 2>/dev/null | head -1)
  echo "✔ MORK: attaching to existing instance on localhost:${MORK_HOST_PORT}${owner:+ (container: $owner)}"
  export MORK_URL="http://host.docker.internal:${MORK_HOST_PORT}"
else
  echo "＋ MORK: none found on :${MORK_HOST_PORT} — creating the bundled service"
  PROFILES+=(--profile mork)
  export MORK_URL="http://mork:8027"
fi

echo
echo "─────────────────────────────────────────────"
echo "  NEO4J_URI = ${NEO4J_URI}"
echo "  MORK_URL  = ${MORK_URL}"
echo "  profiles  = ${PROFILES[*]:-<console only>}"
echo "─────────────────────────────────────────────"
echo

# Bring up the chosen DB services + the console.
#   --build          keeps the console image current with the latest code
#   --remove-orphans clears stray containers from THIS project left by a prior run —
#                    e.g. a bundled mork we no longer start because we're now attaching
#                    to an existing one. Scoped to this compose project, so it never
#                    touches foreign stacks (that's why we attach to, never delete, a
#                    MORK/Neo4j owned by another project).
#   $@               extra args pass through (e.g. --force-recreate)
exec "${COMPOSE[@]}" "${PROFILES[@]}" up -d --build --remove-orphans "$@"
