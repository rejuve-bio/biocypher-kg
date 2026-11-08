#!/usr/bin/env bash
# docker/up.sh: start the BioCypher KG Console, then open http://localhost:9000/console/.
#
# Nothing to configure first, and no env file: each species' databases, the data and
# Builds folders and how builds run are set in the console's Settings page.
#
#   ./docker/up.sh              build the console image if needed and start it
#   ./docker/up.sh --no-build   start a pulled image (CONSOLE_IMAGE) without building
set -euo pipefail

cd "$(dirname "$0")/.."
COMPOSE=(docker compose -f docker/docker-compose.yml)

# The MORK image Settings > Species databases > Run here uses. Built once (it takes a
# while); without biocypher-mork/, MORK can still be connected with "Use existing".
if [ -d biocypher-mork ] && ! docker image inspect biocypher-console-mork >/dev/null 2>&1; then
  echo "building MORK image (first time only, takes a while)…"
  "${COMPOSE[@]}" --profile mork-image build mork \
    || echo "MORK image failed · the console still runs · connect a MORK with \"Use existing\"" >&2
fi

build=(--build)
for arg in "$@"; do
  [ "$arg" = "--no-build" ] && build=()
done
"${COMPOSE[@]}" up -d ${build[@]+"${build[@]}"} "$@" console

echo
echo "console → http://localhost:${CONSOLE_PORT:-9000}/console/"
echo "set the data and Builds folders, Neo4j memory and each species' databases in Settings"
