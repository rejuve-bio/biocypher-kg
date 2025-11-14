#!/bin/bash

BASE_PATH="${BASE_PATH:-/mnt/hdd_1/metta_kg_versions}"

VERSION="$1"

if [ -z "$VERSION" ]; then
  echo "Usage: $0 <version>"
  echo ""
  echo "Running builds:"
  for dir in "${BASE_PATH}"/v-*; do
    if [ -f "$dir/build.pid" ]; then
      PID=$(cat "$dir/build.pid")
      if kill -0 "$PID" 2>/dev/null; then
        echo "  $(basename "$dir") (PID: $PID)"
      fi
    fi
  done
  exit 1
fi

PID_FILE="${BASE_PATH}/${VERSION}/build.pid"
STATUS_FILE="${BASE_PATH}/${VERSION}/build_status.json"

if [ ! -f "$PID_FILE" ]; then
  echo "❌ No PID file found for version: $VERSION"
  exit 1
fi

PID=$(cat "$PID_FILE")

if ! kill -0 "$PID" 2>/dev/null; then
  echo "⚠️  Build process not running (PID: $PID)"
  rm -f "$PID_FILE"
  exit 1
fi

echo "=========================================="
echo "Stopping build: $VERSION"
echo "PID: $PID"
echo "=========================================="

kill "$PID"

sleep 2

if kill -0 "$PID" 2>/dev/null; then
  echo "Process still running, sending SIGKILL..."
  kill -9 "$PID"
  sleep 1
fi

if ! kill -0 "$PID" 2>/dev/null; then
  echo "✅ Build process stopped"

  cat > "$STATUS_FILE" << EOF
{
  "status": "cancelled",
  "message": "Build manually stopped by user",
  "version": "$VERSION",
  "stopped_at": "$(date -u +"%Y-%m-%dT%H:%M:%SZ")"
}
EOF

  rm -f "$PID_FILE"
  echo "✅ Status updated to 'cancelled'"
else
  echo "❌ Failed to stop process"
  exit 1
fi

echo ""
echo "Build output remains at: ${BASE_PATH}/${VERSION}"
echo "Delete with: rm -rf ${BASE_PATH}/${VERSION}"
