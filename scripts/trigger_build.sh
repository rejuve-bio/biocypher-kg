#!/bin/bash

set -e

BASE_PATH="${BASE_PATH:-/mnt/hdd_1/kedist/metta_kg_versions}"
REPO_PATH="${REPO_PATH:-/mnt/hdd_1/kedist/biocypherKG-deploy/biocypher-kg}"
LOGS_PATH="${BASE_PATH}/logs"
BUILD_SCRIPT="${REPO_PATH}/scripts/server_build.sh"
VERSION=""
BUILD_MODE="full"
CHANGED_ADAPTERS=""
CHANGED_OUTDIRS=""
COMMIT_SHA=""
BRANCH=""
CONFIG_PATH=""

while [[ $# -gt 0 ]]; do
  case $1 in
    --version)
      VERSION="$2"
      shift 2
      ;;
    --build-mode)
      BUILD_MODE="$2"
      shift 2
      ;;
    --changed-adapters)
      CHANGED_ADAPTERS="$2"
      shift 2
      ;;
    --changed-outdirs)
      CHANGED_OUTDIRS="$2"
      shift 2
      ;;
    --commit)
      COMMIT_SHA="$2"
      shift 2
      ;;
    --branch)
      BRANCH="$2"
      shift 2
      ;;
    --config)
      CONFIG_PATH="$2"
      shift 2
      ;;
    *)
      echo "Unknown option: $1"
      exit 1
      ;;
  esac
done

if [ -z "$VERSION" ]; then
  echo "Error: --version is required"
  exit 1
fi

mkdir -p "$LOGS_PATH"
mkdir -p "${BASE_PATH}/${VERSION}"

LOG_FILE="${LOGS_PATH}/${VERSION}.log"
PID_FILE="${BASE_PATH}/${VERSION}/build.pid"
STATUS_FILE="${BASE_PATH}/${VERSION}/build_status.json"

if [ -f "$PID_FILE" ]; then
  OLD_PID=$(cat "$PID_FILE")
  if kill -0 "$OLD_PID" 2>/dev/null; then
    echo "Build already running for version $VERSION (PID: $OLD_PID)"
    echo "Status: $(cat $STATUS_FILE 2>/dev/null || echo 'unknown')"
    exit 0
  else
    echo "Cleaning up stale PID file"
    rm -f "$PID_FILE"
  fi
fi
cat > "$STATUS_FILE" << EOF
{
  "status": "queued",
  "message": "Build queued, starting soon...",
  "version": "$VERSION",
  "build_mode": "$BUILD_MODE",
  "commit": "$COMMIT_SHA",
  "branch": "$BRANCH",
  "queued_at": "$(date -u +"%Y-%m-%dT%H:%M:%SZ")",
  "log_file": "$LOG_FILE"
}
EOF

echo "=========================================="
echo "Triggering background build"
echo "Version: $VERSION"
echo "Build mode: $BUILD_MODE"
echo "Commit: $COMMIT_SHA"
echo "Branch: $BRANCH"
echo "Log file: $LOG_FILE"
echo "Status file: $STATUS_FILE"
echo "=========================================="

nohup bash "$BUILD_SCRIPT" \
  --version "$VERSION" \
  --build-mode "$BUILD_MODE" \
  --changed-adapters "$CHANGED_ADAPTERS" \
  --changed-outdirs "$CHANGED_OUTDIRS" \
  --commit "$COMMIT_SHA" \
  --branch "$BRANCH" \
  --config "$CONFIG_PATH" \
  > /dev/null 2>&1 &

BUILD_PID=$!
echo $BUILD_PID > "$PID_FILE"

echo "✅ Build started in background"
echo "PID: $BUILD_PID"
echo ""
echo "Monitor the build with:"
echo "  tail -f $LOG_FILE"
echo ""
echo "Check status with:"
echo "  cat $STATUS_FILE"
echo ""
echo "Check if running:"
echo "  ps -p $BUILD_PID"

sleep 2

if kill -0 "$BUILD_PID" 2>/dev/null; then
  echo "✅ Build process confirmed running"
  exit 0
else
  echo "❌ Build process failed to start"
  cat > "$STATUS_FILE" << EOF
{
  "status": "failed",
  "message": "Build process failed to start",
  "version": "$VERSION",
  "failed_at": "$(date -u +"%Y-%m-%dT%H:%M:%SZ")"
}
EOF
  exit 1
fi
