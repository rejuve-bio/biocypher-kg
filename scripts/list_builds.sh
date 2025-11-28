#!/bin/bash

BASE_PATH="${BASE_PATH:-/mnt/hdd_1/kedist/data/metta_kg_versions}"

echo "=========================================="
echo "Knowledge Graph Builds"
echo "=========================================="
echo ""

if [ ! -d "$BASE_PATH" ]; then
  echo "❌ Base path not found: $BASE_PATH"
  exit 1
fi

cd "$BASE_PATH"

if [ -L "latest" ]; then
  LATEST_TARGET=$(readlink -f latest)
  LATEST_VERSION=$(basename "$LATEST_TARGET")
  echo "📌 Current 'latest': $LATEST_VERSION"
  echo ""
fi

VERSIONS=$(ls -1dt v-* 2>/dev/null)

if [ -z "$VERSIONS" ]; then
  echo "No builds found"
  exit 0
fi

echo "Recent builds:"
echo ""

printf "%-20s %-12s %-50s %-10s\n" "VERSION" "STATUS" "MESSAGE" "SIZE"
echo "--------------------------------------------------------------------------------------------------------"

for version in $VERSIONS; do
  STATUS_FILE="${BASE_PATH}/${version}/build_status.json"

  if [ -f "$STATUS_FILE" ]; then
    STATUS=$(grep -o '"status"[[:space:]]*:[[:space:]]*"[^"]*"' "$STATUS_FILE" | sed 's/.*"\([^"]*\)"/\1/')
    MESSAGE=$(grep -o '"message"[[:space:]]*:[[:space:]]*"[^"]*"' "$STATUS_FILE" | sed 's/.*"\([^"]*\)"/\1/' | cut -c1-48)

    case "$STATUS" in
      "completed")
        STATUS="✅ completed"
        ;;
      "running")
        STATUS="🔄 running"
        ;;
      "queued")
        STATUS="⏳ queued"
        ;;
      "failed")
        STATUS="❌ failed"
        ;;
      *)
        STATUS="❓ $STATUS"
        ;;
    esac
  else
    STATUS="❓ unknown"
    MESSAGE="No status file"
  fi

  if [ -d "$version" ]; then
    SIZE=$(du -sh "$version" 2>/dev/null | cut -f1)
  else
    SIZE="N/A"
  fi

  if [ "$version" == "$LATEST_VERSION" ]; then
    version="$version *"
  fi

  printf "%-20s %-12s %-50s %-10s\n" "$version" "$STATUS" "$MESSAGE" "$SIZE"
done

echo ""
echo "* = current 'latest' version"
echo ""
echo "For details on a specific build, run:"
echo "  bash scripts/check_build_status.sh <version>"
