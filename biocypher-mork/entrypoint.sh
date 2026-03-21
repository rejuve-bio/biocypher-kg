#!/usr/bin/env bash


SERVER_BIN=/app/mork-server
PERSIST_DIR=${SNAPSHOT_DIR:-/app/persist}
INTERVAL=${SNAPSHOT_INTERVAL_SECONDS:-300}   # 5 minutes default
SERVER_URL=${MORK_URL:-http://127.0.0.1:8027}

SNAPSHOT_PATHS="$PERSIST_DIR/snapshot.paths"
WAL_FILE="$PERSIST_DIR/wal.metta"

log() { echo "[entrypoint] $(date -u '+%Y-%m-%dT%H:%M:%SZ') $*"; }

log "Verifying persistent storage at $PERSIST_DIR ..."
if [ ! -d "$PERSIST_DIR" ]; then
    log "ERROR: $PERSIST_DIR not found. Bind-mount might be failed."
    exit 1
fi
# Basic write test
touch "$PERSIST_DIR/.mount_test" && rm "$PERSIST_DIR/.mount_test" || {
    log "ERROR: $PERSIST_DIR is not writable."
    exit 1
}

restore_snapshot_files() {
    if [ -f "$SNAPSHOT_PATHS" ]; then
        log "Found PATHS snapshot ($SNAPSHOT_PATHS). Ready for import."
    else
        log "No snapshot found in $PERSIST_DIR — starting with an empty graph."
    fi
}


wait_for_server() {
    local max_wait=60
    local waited=0
    log "Waiting for MORK server to become ready..."
    until curl -sf "$SERVER_URL/status/-" > /dev/null 2>&1; do
        sleep 1
        waited=$((waited + 1))
        if [ "$waited" -ge "$max_wait" ]; then
            log "ERROR: Server did not become ready within ${max_wait}s."
            exit 1
        fi
    done
    log "Server is ready."
}

import_snapshot_to_server() {
    local encoded
    encoded=$(python3 -c "from urllib.parse import quote; print(quote('\$x'))")

    local restored=false

    if [ -f "$SNAPSHOT_PATHS" ]; then
        log "Performing high-speed binary recovery from $SNAPSHOT_PATHS ..."
        cp "$SNAPSHOT_PATHS" /tmp/restore.paths
        if curl -sf "$SERVER_URL/import/${encoded}/${encoded}?uri=file:///tmp/restore.paths&format=paths" -o /dev/null; then
            log "Binary recovery triggered."
            restored=true
        else
            log "WARNING: Binary import failed."
        fi
        rm -f /tmp/restore.paths
    fi

    if [ "$restored" = "true" ]; then
        log "Waiting for server to finish background ingestion..."
        local tries=0
        while [ $tries -lt 60 ]; do
            local status
            status=$(curl -s "$SERVER_URL/status/${encoded}" | python3 -c "import sys, json; print(json.load(sys.stdin).get('status',''))")
            if [ "$status" = "pathClear" ]; then
                log "Server ready. Background ingestion complete."
                return 0
            fi
            sleep 2
            tries=$((tries + 1))
        done
        log "WARNING: Timed out waiting for ingestion to finish."
    fi
}

export_snapshot() {
    log "Exporting current state to binary (.paths)..."
    local encoded
    encoded=$(python3 -c "from urllib.parse import quote; print(quote('\$x'))")

    # Kick off the async export to /tmp
    rm -f /tmp/snapshot.paths
    if ! curl -sf "$SERVER_URL/export/${encoded}/${encoded}?uri=file:///tmp/snapshot.paths&format=paths" -o /dev/null; then
        log "WARNING: Export request failed."
        return 1
    fi

    # Wait for MORK to finish writing the file (export is async — poll until stable)
    log "Waiting for export to finish writing..."
    local tries=0 prev_size=-1 cur_size=0
    while [ $tries -lt 120 ]; do
        sleep 2
        if [ -f /tmp/snapshot.paths ]; then
            cur_size=$(wc -c < /tmp/snapshot.paths)
            if [ "$cur_size" -gt 0 ] && [ "$cur_size" -eq "$prev_size" ]; then
                break  
            fi
            prev_size=$cur_size
        fi
        tries=$((tries + 1))
    done

    if [ ! -f /tmp/snapshot.paths ] || [ "$cur_size" -eq 0 ]; then
        log "WARNING: Export file missing or empty after waiting. Snapshot skipped."
        return 1
    fi

    mv -f /tmp/snapshot.paths "$SNAPSHOT_PATHS"
    log "Snapshot saved to $SNAPSHOT_PATHS (${cur_size} bytes)"

    # Truncate WAL only after confirmed snapshot
    local is_idle
    is_idle=$(curl -s "$SERVER_URL/status/${encoded}" | python3 -c "import sys, json; print(json.load(sys.stdin).get('status',''))" 2>/dev/null)
    if [ "$is_idle" = "pathClear" ]; then
        sync
        cat /dev/null > "$WAL_FILE" && log "SUCCESS: Snapshot persistent. WAL cleared." \
            || log "WARNING: WAL truncation failed."
    else
        log "SKIPPED: WAL not cleared (server busy). Will clear at next snapshot."
    fi
}

replay_wal() {
    if [ ! -f "$WAL_FILE" ] || [ ! -s "$WAL_FILE" ]; then
        log "WAL is empty — no crash recovery needed."
        return
    fi

    local wal_size
    wal_size=$(wc -c < "$WAL_FILE")
    log "Found WAL ($wal_size bytes). Replaying crash-recovery changes..."

    local encoded
    encoded=$(python3 -c "from urllib.parse import quote; print(quote('\$x'))")
    curl -sf -X POST "$SERVER_URL/upload/${encoded}/${encoded}/" \
        -H 'Content-Type: text/plain' \
        --data-binary @"$WAL_FILE" -o /dev/null \
        && log "WAL replay complete." \
        || log "WARNING: WAL replay failed."
}

is_server_busy() {
    local encoded
    encoded=$(python3 -c "from urllib.parse import quote; print(quote('\$x'))")
    local status
    # Use a timeout on curl and handle non-zero exit or empty response
    status=$(curl -sf --max-time 5 "$SERVER_URL/status/${encoded}" | python3 -c "import sys, json; try: print(json.load(sys.stdin).get('status','')) except: print('')" 2>/dev/null)
    
    if [ "$status" = "pathClear" ]; then
        return 1 
    elif [ -z "$status" ]; then
        log "WARNING: Status check failed or returned empty. Assuming not busy to avoid infinite skip."
        return 1 
    else
        log "INFO: Server is busy (status: $status). Skipping snapshot this cycle."
        return 0
    fi
}

snapshot_daemon() {
    log "Periodic snapshot daemon started (interval: ${INTERVAL}s)."
    while true; do
        sleep "$INTERVAL"
        
        if is_server_busy; then
            log "SKIPPED: Periodic snapshot (Server is busy with mutations). Retrying next interval."
            continue
        fi
        
        log "Periodic snapshot triggered."
        export_snapshot || true
    done
}

shutdown_handler() {
    log "Shutdown signal received. Running final export..."
    export_snapshot || true
    sync
    cat /dev/null > "$WAL_FILE"
    log "Stopping MORK server..."
    kill -TERM "$SERVER_PID" 2>/dev/null || true
    wait "$SERVER_PID" 2>/dev/null || true
    log "Shutdown complete."
    exit 0
}

# MAIN
restore_snapshot_files
log "Starting MORK server..."
export MORK_SERVER_ADDR=0.0.0.0
export MORK_SERVER_PORT=8027
"$SERVER_BIN" &
SERVER_PID=$!
wait_for_server
import_snapshot_to_server
replay_wal
trap shutdown_handler SIGTERM SIGINT SIGQUIT
snapshot_daemon &
DAEMON_PID=$!
wait "$SERVER_PID"
kill "$DAEMON_PID" 2>/dev/null || true
log "MORK server exited."