import os
import glob
import subprocess
import sys
from pathlib import Path

# wal_client lives in the same directory as this script
sys.path.insert(0, str(Path(__file__).resolve().parent))
from wal_client import WalMORK
from client import MORK

def connect_to_mork(host="localhost", port=None):
    """Connect to the running MORK instance (WAL-backed for crash-safe writes)."""
    if port is None:
        port = int(os.getenv("HOST_PORT", 8027))

    persist_dir = os.getenv(
        "SNAPSHOT_DIR",
        str(Path(__file__).resolve().parent.parent / "mork_persist")
    )
    wal_path = os.path.join(persist_dir, "wal.metta")

    base_client = MORK(f"http://{host}:{port}")
    server = WalMORK(
        base_client,
        wal_path=wal_path,
        sync_writes=True,
    )

    # Verify connection
    try:
        with server.work_at("annotation") as scope:
            pass
        print("[SUCCESS] Connected to MORK (WAL enabled).")
    except Exception as e:
        raise ConnectionError(f"[FAILED] Connection failed: {e}")

    return server


def load_metta_files(server, data_dir):
    """Load all .metta and .paths files from the given directory into MORK."""
    path = Path(data_dir)
    if not path.exists():
        raise ValueError(f"[FAILED] Data directory '{path}' not found.")
    
    # to look for both .paths and .metta files
    files = list(path.rglob("*.paths")) + list(path.rglob("*.metta"))
    if not files:
        raise ValueError(f"[WARNING] No .paths or .metta files found in '{path}'.")
    
    print(f"[FILE] Found {len(files)} files. Starting import...")

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
                if file_path.suffix == ".paths":
                    cmd = scope.paths_import_(file_uri)
                    file_type = "PATHS"
                else:
                    cmd = scope.sexpr_import_(file_uri)
                    file_type = "METTA"
                
                cmd.block()
                print(f"   [SUCCESS] LOADED: {folder_path}/{file_path.name} ({file_type})")
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
    dataset_path = os.getenv("DATASET_PATH", "./data")    

    print("... Starting MORK Data Loading Process...")
    
    server = connect_to_mork("localhost")
    successful, failed = load_metta_files(server, dataset_path)
    show_summary(server)
    

    # Final status
    if failed == 0:
        print(f"\n SUCCESS: All {successful} files loaded successfully!")
    else:
        print(f"\n[WARNING]  COMPLETED WITH ISSUES: {successful} loaded, {failed} failed")


if __name__ == "__main__":
    main()