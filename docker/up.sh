#!/usr/bin/env bash
# docker/up.sh: start the BioCypher KG Console; it prints the address to open.
#
# Nothing to configure first, and no env file: each species' databases, the data and
# Builds folders and how builds run are set in the console's Settings page.
#
#   ./docker/up.sh                       build the console image if needed and start it
#   ./docker/up.sh --host 37.27.231.93   on a server: open it at that address (asks for
#                                        a password, printed below); remembered
#   ./docker/up.sh --host localhost      back to this computer only
#   ./docker/up.sh --no-build            start a pulled image (CONSOLE_IMAGE) without building
#   CONSOLE_PORT=9100 ./docker/up.sh     use this port instead of the one picked for you
set -euo pipefail

cd "$(dirname "$0")/.."
COMPOSE=(docker compose -f docker/docker-compose.yml)

# --host <address>: where people open the console. Not this computer's name, so it
# listens on the network, accepts that address, and asks for a password (made once,
# kept in docker/.console-password). Remembered in docker/.console-host.
HOST_FILE=docker/.console-host
PASSWORD_FILE=docker/.console-password
host=""
pass=()
while [ $# -gt 0 ]; do
  case "$1" in
    --host) host="${2:?--host needs an address, e.g. --host 37.27.231.93}"; shift 2 ;;
    --host=*) host="${1#--host=}"; shift ;;
    *) pass+=("$1"); shift ;;
  esac
done
set -- ${pass[@]+"${pass[@]}"}
case "$host" in
  "") host=$(cat "$HOST_FILE" 2>/dev/null || true) ;;
  localhost|127.0.0.1) host=""; rm -f "$HOST_FILE" ;;
  *) echo "$host" > "$HOST_FILE" ;;
esac
if [ -n "$host" ]; then
  if [ ! -s "$PASSWORD_FILE" ]; then
    (umask 077 && openssl rand -hex 12 > "$PASSWORD_FILE")
  fi
  export CONSOLE_BIND=0.0.0.0 CONSOLE_ALLOWED_HOSTS="$host"
  CONSOLE_PASSWORD=$(cat "$PASSWORD_FILE")
  export CONSOLE_PASSWORD
else
  export CONSOLE_BIND=127.0.0.1 CONSOLE_ALLOWED_HOSTS="" CONSOLE_PASSWORD=""
fi

# The console's port: CONSOLE_PORT if given, else the one picked last time, else a
# random free one (20000-39999), so it never collides with another service (a common
# port like 9000 often is taken). Kept in docker/.console-port so the address stays
# the same across restarts.
PORT_FILE=docker/.console-port
CONTAINER=biocypher-console-console-1

port_free() {  # nothing listens on it here, and no container publishes it
  ! (exec 3<>"/dev/tcp/127.0.0.1/$1") 2>/dev/null &&
    [ -z "$(docker ps -q --filter "publish=$1")" ]
}
ours() {  # the console itself already has it (a restart)
  [ -n "$(docker ps -q --filter "name=^${CONTAINER}$" --filter "publish=$1")" ]
}

if [ -z "${CONSOLE_PORT:-}" ]; then
  saved=$(cat "$PORT_FILE" 2>/dev/null || true)
  if [[ "$saved" =~ ^[0-9]+$ ]] && { ours "$saved" || port_free "$saved"; }; then
    CONSOLE_PORT=$saved
  else
    for _ in $(seq 1 50); do
      candidate=$((20000 + RANDOM % 20000))
      if port_free "$candidate"; then
        CONSOLE_PORT=$candidate
        break
      fi
    done
    if [ -z "${CONSOLE_PORT:-}" ]; then
      echo "no free port found · start with CONSOLE_PORT=<port> ./docker/up.sh" >&2
      exit 1
    fi
  fi
fi
echo "$CONSOLE_PORT" > "$PORT_FILE"
export CONSOLE_PORT

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
echo "console → http://${host:-localhost}:${CONSOLE_PORT}/console/"
if [ -n "$host" ]; then
  # Not printed, so it doesn't end up in pasted output or terminal logs.
  echo "password → cat $PASSWORD_FILE"
  echo "on the network · allow port ${CONSOLE_PORT} only from your own IP in the server's firewall"
fi
echo "set the data and Builds folders, Neo4j memory and each species' databases in Settings"
