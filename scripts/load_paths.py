from pathlib import Path
from mork_client import MORK
import argparse
import sys

def main():
    parser = argparse.ArgumentParser(description="Load .paths files into MORK.")
    parser.add_argument("--input-dir", type=str, default="output")
    parser.add_argument("--mork-url", type=str,default="http://localhost:8027")
    args = parser.parse_args()

    output_dir = Path(args.input_dir)
    paths_files = list(output_dir.rglob("*.paths"))
    
    if not paths_files:
        print(f"No .paths files found in {output_dir}")
        sys.exit(1)
    
    print(f"Found {len(paths_files)} .paths files")
    
    try:
        mork = MORK(base_url=args.mork_url)
    except Exception as e:
        print(f"Failed to connect to MORK: {e}")
        sys.exit(1)
    
    loaded_count = 0
    failed_count = 0
    
    with mork.work_at("annotation") as scope:
        for paths_file in paths_files:
            relative_path = paths_file.relative_to(output_dir)
            container_path = f"/app/data/{relative_path}"
            
            print(f"Loading {relative_path}")
            
            try:
                scope.paths_import_(f"file://{container_path}").block()
                size_kb = paths_file.stat().st_size / 1024
                print(f"  Loaded ({size_kb:.1f} KB)")
                loaded_count += 1
            except Exception as e:
                print(f"  Error: {e}")
                failed_count += 1
    
    print(f"\nLoaded: {loaded_count}, Failed: {failed_count}, Total: {len(paths_files)}")
    
    if loaded_count > 0:
        print("Bio Atomspace ready for queries")

if __name__ == "__main__":
    main()
