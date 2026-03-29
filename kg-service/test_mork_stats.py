#!/usr/bin/env python3
"""
Test script to extract MORK statistics
Run this directly on the remote server
"""

import sys
from pathlib import Path
import re
from collections import Counter

# Add MORK client to path
sys.path.insert(0, str(Path(__file__).parent.parent / "biocypher-mork"))
from client import MORK

def analyze_mork():
    """Analyze MORK atoms and return statistics"""
    
    # Connect to MORK 
    server = MORK("http://localhost:8432")
    
    print("Connecting to MORK and downloading atoms...")
    
    try:
        with server.work_at("annotation") as scope:
            # Download MORE atoms - increase limit
            data = scope.download_(max_results=500000)  # Increased to 500k
            data.block()
            
            if not data.data:
                print("ERROR: No data in annotation namespace")
                return None
            
            print(f"Downloaded data, parsing...")
            
            # Parse lines
            lines = [l.strip() for l in data.data.split('\n') if l.strip() and not l.strip().startswith(';')]
            
            print(f"Total lines downloaded: {len(lines)}")
            
            # Show sample of first 20 lines to see what we're getting
            print("\nSample atoms (first 20):")
            for i, line in enumerate(lines[:20], 1):
                print(f"  {i}. {line[:100]}...")  # Truncate long lines
            
            # Analyze atoms
            atom_types = Counter()
            edge_types = Counter()
            total_nodes = 0
            total_edges = 0
            
            for line in lines:
                # Skip metadata atoms
                if 'data-metadata' in line:
                    continue
                
                # Parse atom type - match pattern like (Type ...)
                match = re.match(r'^\(([A-Za-z_][A-Za-z0-9_]*)\s', line)
                if not match:
                    continue
                
                atom_type = match.group(1)
                
                # Check if it's an edge (has nested parentheses)
                # Simple heuristic: if there's a '(' after the type, it's an edge
                rest = line[len(atom_type)+1:].strip()
                
                if rest.startswith('('):
                    # It's an edge like: (has_annotation (Gene X) (GO Y))
                    edge_types[atom_type] += 1
                    total_edges += 1
                else:
                    # It's a node like: (Gene "ENSG123") or (snp RS10)
                    atom_types[atom_type] += 1
                    total_nodes += 1
            
            # Print results
            print("\n" + "="*60)
            print("MORK STATISTICS")
            print("="*60)
            print(f"Total Atoms (Nodes): {total_nodes:,}")
            print(f"Total Relations (Edges): {total_edges:,}")
            
            if len(lines) >= 500000:
                print("\n  WARNING: Hit download limit (500k atoms)")
                print("   There may be more data not shown")
            
            print(f"\nTop 10 Atom Types:")
            for atom, count in atom_types.most_common(10):
                print(f"  {atom}: {count:,}")
            
            print(f"\nTop 10 Edge Types:")
            if edge_types:
                for edge, count in edge_types.most_common(10):
                    print(f"  {edge}: {count:,}")
            else:
                print("  (none found - may need to download more atoms)")
            
            print(f"\nAll Atom Types ({len(atom_types)}):")
            print(f"  {', '.join(sorted(atom_types.keys()))}")
            
            print(f"\nAll Edge Types ({len(edge_types)}):")
            if edge_types:
                print(f"  {', '.join(sorted(edge_types.keys()))}")
            else:
                print("  (none)")
            
            # Return structured data
            return {
                'total_nodes': total_nodes,
                'total_edges': total_edges,
                'top_atoms': [{'type': atom, 'count': count} for atom, count in atom_types.most_common(10)],
                'top_edges': [{'type': edge, 'count': count} for edge, count in edge_types.most_common(10)],
                'all_atom_types': sorted(atom_types.keys()),
                'all_edge_types': sorted(edge_types.keys())
            }
            
    except Exception as e:
        print(f"ERROR: {e}")
        import traceback
        traceback.print_exc()
        return None

if __name__ == "__main__":
    result = analyze_mork()
    
    if result:
        print("\n" + "="*60)
        print("SUCCESS! Stats extracted.")
        print("="*60)
