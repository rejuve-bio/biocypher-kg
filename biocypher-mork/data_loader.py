import os
import glob
from pathlib import Path
from client import MORK   

def connect_to_mork(host="localhost", port=8431):
    """Connect to the running MORK instance."""
    url = f"http://{host}:{port}"
    print(f"Connecting to MORK at {url} ...")
    server = MORK(url)
    
    # Proper connection check - test namespace access
    try:
        with server.work_at("annotation") as scope:
            # Simply accessing the namespace to test the connection
            pass
        print("[SUCCESS] Successfully connected to MORK.")
    except Exception as e:
        raise ConnectionError(f"[FAILED] Connection failed: {e}")
    
    return server

def load_metta_files(server, data_dir):
    """Load all .metta files from the given directory into MORK."""
    path = Path(data_dir)
    if not path.exists():
        raise ValueError(f"[FAILED] Data directory '{path}' not found.")
    
    files = list(path.rglob("*.metta"))
    if not files:
        raise ValueError(f"[WARNING] No .metta files found in '{path}'.")
    
    print(f"[FILE] Found {len(files)} .metta files. Starting import...")

    successful_files = 0
    failed_files = 0
    
    with server.work_at("annotation") as scope:  # Use annotation namespace
        for file_path in files:
            # Convert host path to container path (Docker compatible)
            relative_path = file_path.relative_to(data_dir)
            container_file_path = Path("/app/data") / relative_path
            file_uri = f"file://{container_file_path}"
            
            # Show the exact folder path
            folder_path = file_path.parent
            print(f"...Importing {folder_path}/{file_path.name}")
            try:
                cmd = scope.sexpr_import_(file_uri)
                cmd.block()
                print(f"   [SUCCESS] LOADED: {folder_path}/{file_path.name}")
                successful_files += 1
            except Exception as e:
                print(f"   [FAILED] FAILED: {folder_path}/{file_path.name}: {e}")
                failed_files += 1

    # Final summary with visual indicators
    print("\n" + "="*50)
    print("...LOADING SUMMARY:")
    print(f"   [SUCCESS] Successfully loaded: {successful_files} files")
    print(f"   [FAILED] Failed to load: {failed_files} files") 
    print(f"   Total processed: {len(files)} files")
    print("="*50)
    
    return successful_files, failed_files


def show_summary(server):
    """Show a brief summary of data in MORK."""
    with server.work_at("annotation") as scope:  # Use annotation namespace
        try:
            data = scope.download_(max_results=10)
            data.block()
            if data.data:
                print("\n Sample facts from MORK:")
                print(data.data[:500])
                print("[SUCCESS] Data verification successful - facts found in MORK")
            else:
                print("[WARNING] No data found in annotation namespace.")
        except Exception as e:
            print(f"[WARNING] Error fetching summary: {e}")


def main():
    # Update the path where .metta files are stored
    dataset_path = "/mnt/hdd_1/abdu_md/metta_v1"

    print("... Starting MORK Data Loading Process...")
    
    server = connect_to_mork("localhost", 8431)
    successful, failed = load_metta_files(server, dataset_path)
    show_summary(server)
    
    # Final status
    if failed == 0:
        print(f"\n SUCCESS: All {successful} files loaded successfully!")
    else:
        print(f"\n[WARNING]  COMPLETED WITH ISSUES: {successful} loaded, {failed} failed")


if __name__ == "__main__":
    main()