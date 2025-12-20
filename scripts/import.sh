#!/bin/bash
set -e

# Wait for Neo4j to be ready
NEO4J_URI="${NEO4J_URI:-bolt://deploy:7687}"
NEO4J_USERNAME="${NEO4J_USERNAME:-neo4j}"
NEO4J_PASSWORD="${NEO4J_PASSWORD:-password123}"

max_attempts=60
attempt=0
while [ $attempt -lt $max_attempts ]; do
    if python3 -c "from neo4j import GraphDatabase; driver = GraphDatabase.driver('$NEO4J_URI', auth=('$NEO4J_USERNAME', '$NEO4J_PASSWORD')); driver.verify_connectivity(); driver.close()" 2>/dev/null; then
        echo "Neo4j is ready!"
        break
    fi
    attempt=$((attempt + 1))
    echo "Waiting for Neo4j... (attempt $attempt/$max_attempts)"
    sleep 3

done

if [ $attempt -eq $max_attempts ]; then
    echo "ERROR: Neo4j did not become ready in time"
    exit 1
fi

# Fix .cypher file paths before running the loader - create inline script
echo "Fixing .cypher file paths for Neo4j import..."
cat > /tmp/fix_paths.py << 'EOF'
import os
import glob

# Find all .cypher files in the neo4j output directory
cypher_files = glob.glob('/usr/app/data/output_human/neo4j/**/*.cypher', recursive=True)

print(f"Found {len(cypher_files)} .cypher files to process")

if len(cypher_files) == 0:
    print("No .cypher files found! Make sure the build process completed successfully.")
    exit(1)

for file_path in cypher_files:
    print(f"Processing: {file_path}")
    try:
        with open(file_path, 'r') as f:
            content = f.read()
        
        # Check if file needs updating
        if 'file:///usr/app/data/' in content:
            # Replace file:///usr/app/data/ with file:/// to make paths relative to Neo4j import directory
            updated_content = content.replace('file:///usr/app/data/', 'file:///')
            
            with open(file_path, 'w') as f:
                f.write(updated_content)
            
            print(f"✓ Successfully updated {file_path}")
        else:
            print(f"- No changes needed for {file_path}")
    except Exception as e:
        print(f"✗ Error updating {file_path}: {e}")

print("Path fix completed!")
EOF

python3 /tmp/fix_paths.py

# Run the original loader (provide password via echo to avoid interactive prompt)
echo "$NEO4J_PASSWORD" | python3 /scripts/neo4j_loader.py --output-dir /usr/app/data/output_human/neo4j --uri "$NEO4J_URI" --username "$NEO4J_USERNAME"

echo "Import completed successfully!"

# Keep container running
tail -f /dev/null