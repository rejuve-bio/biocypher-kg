import os
import glob
import time
from pathlib import Path
from mork_client import ManagedMORK


def load_metta_dataset(dataset_path, mork_port, space, clear_before_load=True):
    """
    Load MeTTa dataset into MORK server
    
    Args:
        dataset_path (str): Path to directory containing .metta files
        mork_port (int): MORK server port
        space (str): MORK space to load into
        clear_before_load (bool): Whether to clear before loading
    
    Returns:
        bool: True if successful, False otherwise
    """
    
    # Validate dataset path
    if not os.path.exists(dataset_path):
        print(f"Error: Dataset path '{dataset_path}' does not exist.")
        return False
    
    # Find .metta files
    metta_files = glob.glob(os.path.join(dataset_path, "**/*.metta"), recursive=True)
    if not metta_files:
        print(f"Error: No .metta files found in '{dataset_path}'")
        return False
    
    print(f"Found {len(metta_files)} .metta files in {dataset_path}")
    
    try:
        # Connect to MORK server
        mork_url = f"http://localhost:{mork_port}"
        print(f"Connecting to MORK server at {mork_url}...")
        
        server = ManagedMORK.connect(url=mork_url)
        print("Connected to MORK server successfully")
        
        # Clear existing data if requested
        if clear_before_load:
            print("Clearing existing data...")
            server.clear()
        
        # Load files
        print(f"Loading files into '{space}' space...")
        start_time = time.time()
        
        failed_files = []
        successful_files = 0
        
        with server.work_at(space) as workspace:
            for i, file_path in enumerate(metta_files, 1):
                if "dbsnp" in file_path:
                    print(f"Skiping {file_path}")
                    continue
                path_obj = Path(file_path)
                file_url = path_obj.resolve().as_uri()
                
                # Docker volume mount adjustments
                file_url = file_url.replace("/mnt/hdd_1/abdu/metta_out_v5", "/shared/output")
                file_url = file_url.replace("/mnt/hdd_1/dawit/metta_sample/output", "/shared/output")
                
                print(f"  [{i:3d}/{len(metta_files)}] Loading: {file_path}")
                
                try:
                    workspace.sexpr_import_(file_url).block()
                    successful_files += 1
                except Exception as e:
                    print(f"    Warning: Error loading {file_path}: {e}")
                    failed_files.append((file_path, str(e)))
                    continue
        
        # Calculate loading time
        end_time = time.time()
        loading_time = end_time - start_time
        
        print(f"\nDataset loading completed!")
        print(f"   Files found: {len(metta_files)}")
        print(f"   Files loaded successfully: {successful_files}")
        print(f"   Files failed: {len(failed_files)}")
        print(f"   Loading time: {loading_time:.2f} seconds")
        print(f"   Target space: {space}")
        print(f"   MORK server: {mork_url}")
        
        if failed_files:
            print(f"\nFailed files:")
            for filename, error in failed_files:
                print(f"   - {filename}: {error}")
        
        return successful_files > 0
        
    except Exception as e:
        print(f"Error: Failed to load dataset: {e}")
        return False