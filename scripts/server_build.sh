#!/bin/bash

set -e

REPO_PATH="${REPO_PATH:-/mnt/hdd_1/kedist/biocypher-deploy}"
BASE_PATH="${BASE_PATH:-/mnt/hdd_1/kedist/metta_kg_versions}"
BUILD_WORKSPACE="${BASE_PATH}/build_workspace"
LOGS_PATH="${BASE_PATH}/logs"
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

mkdir -p "${BUILD_WORKSPACE}"
mkdir -p "${LOGS_PATH}"
mkdir -p "${BASE_PATH}/${VERSION}"

LOG_FILE="${LOGS_PATH}/${VERSION}.log"
STATUS_FILE="${BASE_PATH}/${VERSION}/build_status.json"

log() {
  echo "[$(date -u +"%Y-%m-%d %H:%M:%S UTC")] $1" | tee -a "$LOG_FILE"
}

update_status() {
  local status=$1
  local message=$2
  cat > "$STATUS_FILE" << EOF
{
  "status": "$status",
  "message": "$message",
  "last_update": "$(date -u +"%Y-%m-%dT%H:%M:%SZ")",
  "version": "$VERSION",
  "build_mode": "$BUILD_MODE",
  "commit": "$COMMIT_SHA",
  "branch": "$BRANCH",
  "log_file": "$LOG_FILE"
}
EOF
}

build_kg() {
  log "=========================================="
  log "Starting KG build: $VERSION"
  log "Build mode: $BUILD_MODE"
  log "Commit: $COMMIT_SHA"
  log "Branch: $BRANCH"
  log "=========================================="

  update_status "running" "Build started"

  cd "$REPO_PATH"

  if [ ! -d ".git" ]; then
    log "ERROR: Not a git repository: $REPO_PATH"
    update_status "failed" "Not a git repository"
    exit 1
  fi

  log "Pulling latest code from branch: $BRANCH"

  git stash push -m "Auto-stash before build $VERSION" 2>&1 | tee -a "$LOG_FILE" || true

  git fetch origin "$BRANCH" 2>&1 | tee -a "$LOG_FILE"
  git checkout "$BRANCH" 2>&1 | tee -a "$LOG_FILE"
  git pull origin "$BRANCH" 2>&1 | tee -a "$LOG_FILE"

  CURRENT_COMMIT=$(git rev-parse HEAD)
  log "Current commit: $CURRENT_COMMIT"

  if [ -n "$COMMIT_SHA" ] && [ "$CURRENT_COMMIT" != "$COMMIT_SHA" ]; then
    log "Warning: Current commit ($CURRENT_COMMIT) differs from expected ($COMMIT_SHA)"
    log "This may happen if commits were pushed after the workflow started"
  fi

  log "=========================================="
  log "Checking and installing dependencies with UV..."
  log "=========================================="

  if ! command -v uv &> /dev/null; then
    log "ERROR: UV is not installed. Please install UV first: curl -LsSf https://astral.sh/uv/install.sh | sh"
    update_status "failed" "UV package manager not found"
    exit 1
  fi

  log "UV version: $(uv --version)"

  NEEDS_INSTALL=false

  if [ ! -f ".venv/.uv_managed" ]; then
    log "UV marker not found - this may be a first run or Poetry environment"
    if [ -d ".venv" ]; then
      log "Removing old virtual environment (possibly from Poetry)"
      rm -rf ".venv"
    fi
    NEEDS_INSTALL=true
  fi

  if [ ! -d ".venv" ]; then
    log "Virtual environment not found, will create new one"
    NEEDS_INSTALL=true
  fi

  if [ "$NEEDS_INSTALL" == "false" ]; then
    log "Checking if biocypher is installed..."
    if ! .venv/bin/python -c "import biocypher" 2>/dev/null; then
      log "biocypher not found in virtual environment, reinstalling dependencies"
      NEEDS_INSTALL=true
    else
      log "biocypher is installed"
    fi
  fi

  if [ "$NEEDS_INSTALL" == "false" ] && [ -f ".venv/.uv_sync_marker" ]; then
    log "Checking if pyproject.toml changed..."
    LAST_SYNC_TIME=$(stat -c %Y ".venv/.uv_sync_marker" 2>/dev/null || echo 0)
    PYPROJECT_MOD_TIME=$(stat -c %Y "pyproject.toml" 2>/dev/null || echo 0)

    if [ "$PYPROJECT_MOD_TIME" -gt "$LAST_SYNC_TIME" ]; then
      log "pyproject.toml changed since last sync, updating dependencies"
      NEEDS_INSTALL=true
    else
      log "pyproject.toml unchanged since last sync"
    fi
  fi

  if [ "$NEEDS_INSTALL" == "true" ]; then
    log "Installing/syncing dependencies with UV..."
    log "Running: uv sync"

    if uv sync 2>&1 | tee -a "$LOG_FILE"; then
      mkdir -p ".venv"
      touch ".venv/.uv_managed"
      touch ".venv/.uv_sync_marker"

      log "Verifying biocypher installation..."
      if .venv/bin/python -c "import biocypher" 2>&1 | tee -a "$LOG_FILE"; then
        log "✅ Dependencies installed and verified successfully"
      else
        log "ERROR: biocypher still not found after UV sync"
        update_status "failed" "Dependency installation verification failed"
        exit 1
      fi
    else
      log "ERROR: UV sync failed"
      update_status "failed" "UV sync failed"
      exit 1
    fi
  else
    log "✅ Dependencies are up to date, skipping installation"
  fi

  log "=========================================="

  if [ "$BUILD_MODE" == "full" ]; then
    log "=========================================="
    log "Building FULL knowledge graph..."
    log "This is the first build or a forced full rebuild"
    log "=========================================="
    update_status "running" "Building full KG"

    ADAPTERS_CONFIG="config/adapters_config.yaml"
    log "Using FULL config: $ADAPTERS_CONFIG"

    make run-direct \
      OUTPUT_DIR="${BUILD_WORKSPACE}/${VERSION}" \
      ADAPTERS_CONFIG="$ADAPTERS_CONFIG" \
      DBSNP_RSIDS=/mnt/hdd_2/abdu/dbsnp/dbsnp_rsids.pkl \
      DBSNP_POS=/mnt/hdd_2/abdu/dbsnp/dbsnp_pos.pkl \
      WRITER_TYPE=metta 2>&1 | tee -a "$LOG_FILE"

  elif [ "$BUILD_MODE" == "incremental" ]; then
    log "=========================================="
    log "Building INCREMENTAL knowledge graph..."
    log "Changed adapters: $CHANGED_ADAPTERS"
    log "Changed outdirs: $CHANGED_OUTDIRS"
    log "=========================================="

    log "Checking if schema changed..."
    SCHEMA_CHANGED=false

    if git diff --quiet HEAD~1 HEAD -- config/schema_config.yaml config/biocypher_config.yaml 2>/dev/null; then
      log "✓ Schema configs unchanged - incremental build OK"
    else
      log "⚠️  Schema configs changed - forcing FULL rebuild"
      SCHEMA_CHANGED=true
      BUILD_MODE="full"
      update_status "running" "Building full KG (schema changed)"

      ADAPTERS_CONFIG="config/adapters_config.yaml"
      log "Using FULL config: $ADAPTERS_CONFIG"

      make run-direct \
        OUTPUT_DIR="${BUILD_WORKSPACE}/${VERSION}" \
        ADAPTERS_CONFIG="$ADAPTERS_CONFIG" \
        DBSNP_RSIDS=/mnt/hdd_2/abdu/dbsnp/dbsnp_rsids.pkl \
        DBSNP_POS=/mnt/hdd_2/abdu/dbsnp/dbsnp_pos.pkl \
        WRITER_TYPE=metta 2>&1 | tee -a "$LOG_FILE"
    fi

    if [ "$SCHEMA_CHANGED" == "true" ]; then
      log "Skipping incremental build logic (already did full build due to schema change)"
    elif [ ! -L "${BASE_PATH}/latest" ]; then
      log "=========================================="
      log "⚠️  No previous version found - this is the FIRST BUILD"
      log "Switching to FULL build mode with FULL config"
      log "=========================================="
      BUILD_MODE="full"
      update_status "running" "Building full KG (first build)"

      ADAPTERS_CONFIG="config/adapters_config.yaml"
      log "Using FULL config (not deploy_config): $ADAPTERS_CONFIG"

      make run-direct \
        OUTPUT_DIR="${BUILD_WORKSPACE}/${VERSION}" \
        ADAPTERS_CONFIG="$ADAPTERS_CONFIG" \
        DBSNP_RSIDS=/mnt/hdd_2/abdu/dbsnp/dbsnp_rsids.pkl \
        DBSNP_POS=/mnt/hdd_2/abdu/dbsnp/dbsnp_pos.pkl \
        WRITER_TYPE=metta 2>&1 | tee -a "$LOG_FILE"
    else
      PREVIOUS_PATH=$(readlink -f "${BASE_PATH}/latest")
      PREVIOUS_VERSION=$(basename "$PREVIOUS_PATH")

      log "=========================================="
      log "📋 INCREMENTAL BUILD STRATEGY:"
      log "1. Build changed/affected adapters into temporary directory"
      log "   - Code changes: $CHANGED_ADAPTERS"
      log "   - Affected by config changes: (included in deploy_config.yaml)"
      log "2. Copy entire previous KG (version: $PREVIOUS_VERSION)"
      log "3. Skip incomplete metadata from partial build"
      log "4. Replace changed/affected output directories: $CHANGED_OUTDIRS"
      log "5. Regenerate metadata by scanning complete merged KG"
      log "6. Clean up temporary build directory"
      log "=========================================="

      update_status "running" "Building changed and config-affected adapters"

      if [ -n "$CONFIG_PATH" ] && [ -f "$CONFIG_PATH" ]; then
        ADAPTERS_CONFIG="$CONFIG_PATH"
        log "Using INCREMENTAL config (includes code changes + config-affected adapters): $ADAPTERS_CONFIG"
      else
        ADAPTERS_CONFIG="c.yaml"
        log "Warning: No deploy_config found, using full config: $ADAPTERS_CONFIG"
      fi

      log "Step 1: Building changed adapters..."
      rm -rf "${BUILD_WORKSPACE}/${VERSION}_new"
      mkdir -p "${BUILD_WORKSPACE}/${VERSION}_new"

      make run-direct \
        OUTPUT_DIR="${BUILD_WORKSPACE}/${VERSION}_new" \
        ADAPTERS_CONFIG="$ADAPTERS_CONFIG" \
        DBSNP_RSIDS=/mnt/hdd_2/abdu/dbsnp/dbsnp_rsids.pkl \
        DBSNP_POS=/mnt/hdd_2/abdu/dbsnp/dbsnp_pos.pkl \
        WRITER_TYPE=metta 2>&1 | tee -a "$LOG_FILE"

      log "=========================================="
      log "Step 2: Copying unchanged parts from previous KG..."
      update_status "running" "Copying previous KG version"

      mkdir -p "${BUILD_WORKSPACE}/${VERSION}"
      log "Copying from: $PREVIOUS_PATH"
      log "Copying to: ${BUILD_WORKSPACE}/${VERSION}/"
      cp -rv "$PREVIOUS_PATH"/* "${BUILD_WORKSPACE}/${VERSION}/" 2>&1 | tee -a "$LOG_FILE"
      log "✅ Previous KG copied successfully"

      log "=========================================="
      log "Step 3: Removing incomplete metadata files from partial build..."
      log "Note: Metadata will be regenerated after merge using delta calculation"

      if [ -f "${BUILD_WORKSPACE}/${VERSION}_new/type_defs.metta" ]; then
        rm -f "${BUILD_WORKSPACE}/${VERSION}_new/type_defs.metta"
        log "  ✅ Removed partial type_defs.metta"
      fi
      log "Keeping partial graph_info.json for delta calculation"

      log "=========================================="
      log "Step 4: Replacing changed/affected adapter output directories..."
      log "These directories will be replaced:"
      log "  - Adapters with code changes"
      log "  - Adapters affected by config changes"
      log "  - Output dirs: $CHANGED_OUTDIRS"
      update_status "running" "Merging changed adapters with previous KG"

      IFS=',' read -ra OUTDIR_ARRAY <<< "$CHANGED_OUTDIRS"
      for outdir in "${OUTDIR_ARRAY[@]}"; do
        outdir=$(echo "$outdir" | xargs)  # trim whitespace
        if [ -n "$outdir" ]; then
          log "  → Replacing outdir: $outdir"
          rm -rf "${BUILD_WORKSPACE}/${VERSION}/${outdir}"
          if [ -d "${BUILD_WORKSPACE}/${VERSION}_new/${outdir}" ]; then
            cp -rv "${BUILD_WORKSPACE}/${VERSION}_new/${outdir}" "${BUILD_WORKSPACE}/${VERSION}/${outdir}" 2>&1 | tee -a "$LOG_FILE"
            log "  ✅ Replaced: $outdir"
          else
            log "  ⚠️  Warning: New outdir not found: $outdir"
          fi
        fi
      done

      log "=========================================="
      log "Step 5: Updating metadata for merged knowledge graph..."
      log "Strategy: Delta calculation using graph_info.json from partial build"
      log "Formula: new_total = old_total - old_adapter + new_partial"
      log "This preserves dataset information and accurately updates counts"
      update_status "running" "Updating metadata"

      rm -f "${BUILD_WORKSPACE}/${VERSION}_new/type_defs.metta"

      log "Removing old metadata files from merged directory..."
      rm -f "${BUILD_WORKSPACE}/${VERSION}/graph_info.json"
      rm -f "${BUILD_WORKSPACE}/${VERSION}/type_defs.metta"
      log "Old metadata files removed"

      if [ -f "${PREVIOUS_PATH}/type_defs.metta" ]; then
        cp "${PREVIOUS_PATH}/type_defs.metta" "${BUILD_WORKSPACE}/${VERSION}/type_defs.metta"
        log "✅ Copied type_defs.metta from previous version"
      fi

      if [ -f "${REPO_PATH}/scripts/regenerate_metadata.py" ]; then
        log "Running metadata update script with delta calculation..."
        log "  Old version: ${PREVIOUS_PATH}"
        log "  New partial: ${BUILD_WORKSPACE}/${VERSION}_new"
        log "  Merged output: ${BUILD_WORKSPACE}/${VERSION}"
        log "  Changed adapters: ${CHANGED_OUTDIRS}"
        log "  Adapters config: ${ADAPTERS_CONFIG}"

        OLD_FULL_CONFIG=""
        NEW_FULL_CONFIG=""

        OLD_VERSION_PATH="${PREVIOUS_PATH}" \
        NEW_PARTIAL_PATH="${BUILD_WORKSPACE}/${VERSION}_new" \
        CHANGED_ADAPTERS="${CHANGED_OUTDIRS}" \
        OLD_ADAPTERS_CONFIG="${OLD_FULL_CONFIG}" \
        NEW_ADAPTERS_CONFIG="${NEW_FULL_CONFIG}" \
        PYTHONUNBUFFERED=1 uv run python "${REPO_PATH}/scripts/regenerate_metadata.py" \
          "${BUILD_WORKSPACE}/${VERSION}" \
          "${PREVIOUS_PATH}/graph_info.json" 2>&1 | tee -a "$LOG_FILE"

        if [ $? -eq 0 ]; then
          log "✅ Metadata updated successfully using delta calculation"
        else
          log "⚠️  Warning: Metadata update failed, but continuing with build"
        fi
      else
        log "⚠️  Warning: regenerate_metadata.py not found, skipping metadata update"
      fi

      log "=========================================="
      log "Step 6: Cleaning up temporary build directory..."
      rm -rf "${BUILD_WORKSPACE}/${VERSION}_new"
      log "✅ Incremental build merge completed successfully"
      log "=========================================="
    fi
  fi

  log "Moving build to final location..."
  update_status "running" "Finalizing deployment"

  if [ -d "${BUILD_WORKSPACE}/${VERSION}" ]; then
    cat > "${BUILD_WORKSPACE}/${VERSION}/metadata.json" << EOF
{
  "version": "$VERSION",
  "timestamp": "$(date -u +"%Y-%m-%dT%H:%M:%SZ")",
  "commit": "$COMMIT_SHA",
  "branch": "$BRANCH",
  "build_mode": "$BUILD_MODE",
  "changed_adapters": "$CHANGED_ADAPTERS",
  "changed_outdirs": "$CHANGED_OUTDIRS",
  "writer_type": "metta",
  "build_duration_seconds": $SECONDS
}
EOF


    cp -r "${BUILD_WORKSPACE}/${VERSION}"/* "${BASE_PATH}/${VERSION}/"

    ln -sfn "$VERSION" "${BASE_PATH}/latest"

    rm -rf "${BUILD_WORKSPACE}/${VERSION}"

    log "Build size: $(du -sh ${BASE_PATH}/${VERSION} | cut -f1)"

    log "Cleaning up old versions..."
    cd "$BASE_PATH"
    ls -dt v-* 2>/dev/null | tail -n +6 | xargs -r rm -rf

    log "=========================================="
    log "Build completed successfully!"
    log "Version: $VERSION"
    log "Location: ${BASE_PATH}/${VERSION}"
    log "Log file: $LOG_FILE"
    log "=========================================="

    update_status "completed" "Build completed successfully"
  else
    log "ERROR: Build output directory not found"
    update_status "failed" "Build output directory not found"
    exit 1
  fi
}

if build_kg; then
  exit 0
else
  ERROR_CODE=$?
  log "ERROR: Build failed with exit code $ERROR_CODE"
  update_status "failed" "Build failed with exit code $ERROR_CODE"
  exit $ERROR_CODE
fi
