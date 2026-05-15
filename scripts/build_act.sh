set -euo pipefail

DIR="${1:-${MORK_DATA_DIR:-output}}"

if [ ! -d "$DIR" ]; then
    echo "Error: Directory '$DIR' does not exist."
    echo "Usage: bash scripts/build_act.sh <output_directory>"
    exit 1
fi

METTA_COUNT=$(find "$DIR" -name "*.metta" | wc -l | tr -d '[:space:]')

if [ "$METTA_COUNT" -eq 0 ]; then
    echo "Error: No .metta files found in '$DIR'."
    if [ "$DIR" == "output" ]; then
        echo "Hint: You might need to specify your data folder, e.g.:"
        echo "      bash scripts/build_act.sh output_human"
    else
        echo "Verify that the BioCypher generation generated .metta files."
    fi
    exit 1
fi

ACT_FILE="$DIR/annotation.act"

if [ -f "$ACT_FILE" ]; then
    NEWER_SOURCE=$(find "$DIR" -name "*.metta" -newer "$ACT_FILE" -print -quit)
    
    if [ -z "$NEWER_SOURCE" ]; then
        echo "annotation.act is up to date; no rebuild needed."
        exit 0
    fi
    echo "Change detected in source files. Rebuilding..."
fi

export MORK_DATA_DIR=$(realpath "$DIR")

echo "Building unified annotation.act from $METTA_COUNT files in $MORK_DATA_DIR"

docker compose run --rm -T mork bash <<'EOF'
set -euo pipefail
DATA=/app/data
BIN=/app/MORK/target/release/mork
MERGED_TMP=$DATA/annotation.tmp.metta
OUT_TMP=$DATA/annotation.tmp.act
OUT_FINAL=$DATA/annotation.act

echo "1. Consolidating MeTTa fragments..."
> "$MERGED_TMP"

#load types -> nodes -> edges for correct resolution
#typedefs
if [ -f "$DATA/type_defs.metta" ]; then
    cat "$DATA/type_defs.metta" >> "$MERGED_TMP"
    echo "" >> "$MERGED_TMP"
fi

#nodes
find "$DATA" -name "nodes.metta" -exec cat {} + >> "$MERGED_TMP"
echo "" >> "$MERGED_TMP"

#edges
find "$DATA" -name "edges.metta" -exec cat {} + >> "$MERGED_TMP"
echo "" >> "$MERGED_TMP"

echo "2. Compiling into binary Arena (ACT)..."
"$BIN" convert metta act "$" "_1" "$MERGED_TMP" "$OUT_TMP"

echo "3. Finalizing..."
mv -f "$OUT_TMP" "$OUT_FINAL"
rm -f "$MERGED_TMP"

echo "Done: $OUT_FINAL"
EOF
