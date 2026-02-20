from pathlib import Path
from mork_client import MORK
import argparse
import sys

def main():
    parser = argparse.ArgumentParser(description="Convert .metta files to .paths using MORK.")
    parser.add_argument("--input-dir", type=str, default="output")
    parser.add_argument("--mork-url", type=str, default="http://localhost:8027")
    args = parser.parse_args()

    output_dir = Path(args.input_dir)
    metta_files = list(output_dir.rglob("*.metta"))
    
    if not metta_files:
        print(f"No .metta files found in {output_dir}")
        sys.exit(1)
    
    print(f"Found {len(metta_files)} .metta files")
    
    try:
        mork = MORK(base_url=args.mork_url)
    except Exception as e:
        print(f"Failed to connect to MORK: {e}")
        sys.exit(1)
    
    converted_count = 0
    skipped_count = 0
    
    for metta_file in metta_files:
        relative_path = metta_file.relative_to(output_dir)
        paths_file_host = metta_file.with_suffix('.paths')
        paths_file_container = f"/app/data/{relative_path.with_suffix('.paths')}"
        metta_file_container = f"/app/data/{relative_path}"
        
        if paths_file_host.exists() and paths_file_host.stat().st_mtime > metta_file.stat().st_mtime:
            print(f"Skipping {relative_path} (up to date)")
            skipped_count += 1
            continue
        
        print(f"Converting {relative_path}")
        
        try:
            mork.sexpr_import_(f"file://{metta_file_container}").block()
            mork.paths_export_(f"file://{paths_file_container}").block()
            mork.clear().block()
            
            if paths_file_host.exists():
                size_kb = paths_file_host.stat().st_size / 1024
                print(f"  Created {paths_file_host.name} ({size_kb:.1f} KB)")
                converted_count += 1
        except Exception as e:
            print(f"  Error: {e}")
    
    print(f"\nConverted: {converted_count}, Skipped: {skipped_count}, Total: {len(metta_files)}")

if __name__ == "__main__":
    main()
