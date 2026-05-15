set -euo pipefail

DIR="${1:-${MORK_DATA_DIR:-output}}"

if [ ! -d "$DIR" ]; then
    echo "Error: Directory '$DIR' does not exist."
    echo "Usage: bash scripts/build_paths.sh <output_directory>"
    exit 1
fi

METTA_FILES=$(find "$DIR" -name "*.metta")
METTA_COUNT=$(echo "$METTA_FILES" | wc -l | tr -d '[:space:]')

if [ "$METTA_COUNT" -eq 0 ]; then
    echo "Error: No .metta files found in '$DIR'."
    exit 1
fi

export MORK_DATA_DIR=$(realpath "$DIR")

echo "Converting $METTA_COUNT MeTTa files to .paths in $MORK_DATA_DIR"

docker compose run --rm -T mork bash -c '
set -euo pipefail
DATA=/app/data
BIN=/app/MORK/target/release/mork

#finding all .metta files relative to /app/data
find "$DATA" -name "*.metta" | while read -r metta_file; do
    paths_file="${metta_file%.metta}.paths"
    
    #incremental build
    if [ ! -f "$paths_file" ] || [ "$metta_file" -nt "$paths_file" ]; then
        echo "Converting ${metta_file#$DATA/} -> ${paths_file#$DATA/}"
        "$BIN" convert metta paths "$" "_1" "$metta_file" "$paths_file"
    else
        echo "Skipping ${metta_file#$DATA/} (up to date)"
    fi
done
'

echo "Done: .paths files generated in $DIR"
