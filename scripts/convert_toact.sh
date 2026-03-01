DIR="${1:-output}"

export MORK_DATA_DIR=$(realpath "$DIR")

echo "Starting MORK batch conversion..."

docker compose run --rm -T mork bash << 'EOF'
    DIR="/app/data"
    MORK_BIN="/app/MORK/target/release/mork"

    echo "Scanning for .metta files in $DIR..."
    files=$(find "$DIR" -name "*.metta")

    if [ -z "$files" ]; then
        echo "No .metta files found."
        exit 0
    fi

    total=$(echo "$files" | wc -l)
    echo "Found $total .metta files"

    converted_count=0
    skipped_count=0

    for file in $files; do
        out_file="${file%.*}.act"
        
        if [ -f "$out_file" ] && [ "$out_file" -nt "$file" ]; then
            echo "Skipping $file"
            skipped_count=$((skipped_count + 1))
            continue
        fi
        
        echo "Converting $file ..."
        "$MORK_BIN" convert metta act "$" "_1" "$file" "$out_file"
        
        if [ $? -eq 0 ]; then
            echo "  Successfully created $(basename "$out_file")"
            converted_count=$((converted_count + 1))
        else
            echo "  Error: Failed to convert $file"
        fi
    done

    echo ""
    echo "Summary: Converted: $converted_count, Skipped: $skipped_count, Total: $total"
EOF

status=$?

if [ $status -eq 0 ]; then
    echo "Conversion process completed."
else
    echo "Conversion process failed with status $status"
fi
