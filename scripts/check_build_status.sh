#!/bin/bash


BASE_PATH="${BASE_PATH:-/mnt/hdd_1/kedist/metta_kg_versions}"

VERSION="$1"

if [ -z "$VERSION" ]; then
  echo "Usage: $0 <version>"
  echo ""
  echo "Available versions:"
  ls -1t "${BASE_PATH}" | grep "^v-" | head -10
  exit 1
fi

STATUS_FILE="${BASE_PATH}/${VERSION}/build_status.json"
PID_FILE="${BASE_PATH}/${VERSION}/build.pid"
LOG_FILE="${BASE_PATH}/logs/${VERSION}.log"

if [ ! -f "$STATUS_FILE" ]; then
  echo "❌ No build found for version: $VERSION"
  exit 1
fi

echo "=========================================="
echo "Build Status for: $VERSION"
echo "=========================================="
echo ""

if command -v jq &> /dev/null; then
  cat "$STATUS_FILE" | jq .
else
  cat "$STATUS_FILE"
fi

echo ""
echo "=========================================="

if [ -f "$PID_FILE" ]; then
  PID=$(cat "$PID_FILE")
  if kill -0 "$PID" 2>/dev/null; then
    echo "✅ Build process is running (PID: $PID)"
  else
    echo "⚠️  Build process not found (PID file exists but process $PID is not running)"
  fi
else
  echo "ℹ️  No PID file (build may be completed or not started)"
fi

echo ""
echo "Log file: $LOG_FILE"

if [ -f "$LOG_FILE" ]; then
  echo ""
  echo "Last 20 lines of log:"
  echo "----------------------------------------"
  tail -20 "$LOG_FILE"
fi

echo ""
echo "=========================================="
echo "Monitor live with: tail -f $LOG_FILE"
echo "=========================================="
