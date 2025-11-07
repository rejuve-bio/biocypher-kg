import os
import glob
import time
from pathlib import Path
from mork_client import ManagedMORK


def get_user_input():
    """
    Get input arguments interactively from the user
    """
    print("=== MeTTa Dataset Loader - Interactive Mode ===\n")
    
    # Get dataset path
    while True:
        dataset_path = input("Enter the dataset path (directory containing .metta files): ").strip()
        if dataset_path:
            if os.path.exists(dataset_path):
                break
            else:
                print(f"Error: Path '{dataset_path}' does not exist. Please try again.")
        else:
            print("Dataset path cannot be empty. Please try again.")
    
    # Get MORK port
    while True:
        mork_port_input = input("Enter MORK server port (default: 8080): ").strip()
        if not mork_port_input:
            mork_port = 8080
            break
        try:
            mork_port = int(mork_port_input)
            if mork_port > 0 and mork_port < 65536:
                break
            else:
                print("Port must be between 1 and 65535. Please try again.")
        except ValueError:
            print("Invalid port number. Please enter a valid integer.")
    
    # Get space name
    while True:
        space = input("Enter MORK space name (default: 'default'): ").strip()
        if not space:
            space = "default"
            break
        if space.strip():
            break
        print("Space name cannot be empty. Please try again.")
    
    # Get clear before load option
    while True:
        clear_input = input("Clear existing data before loading? (y/n, default: y): ").strip().lower()
        if not clear_input:
            clear_before_load = True
            break
        if clear_input in ['y', 'yes']:
            clear_before_load = True
            break
        elif clear_input in ['n', 'no']:
            clear_before_load = False
            break
        else:
            print("Please enter 'y' for yes or 'n' for no.")
    
    return dataset_path, mork_port, space, clear_before_load


def confirm_loading(dataset_path, mork_port, space, clear_before_load):
    """
    Confirm the loading parameters with the user
    """
    print("\n=== Loading Configuration ===")
    print(f"Dataset path: {dataset_path}")
    print(f"MORK server port: {mork_port}")
    print(f"Target space: {space}")
    print(f"Clear before load: {'Yes' if clear_before_load else 'No'}")
    
    # Count .metta files for confirmation
    metta_files = glob.glob(os.path.join(dataset_path, "**/*.metta"), recursive=True)
    print(f"Found {len(metta_files)} .metta files")
    
    while True:
        confirm = input("\nProceed with loading? (y/n): ").strip().lower()
        if confirm in ['y', 'yes']:
            return True
        elif confirm in ['n', 'no']:
            return False
        else:
            print("Please enter 'y' to continue or 'n' to cancel.")


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


def main():
    """
    Main interactive function
    """
    try:
        # Get user input
        dataset_path, mork_port, space, clear_before_load = get_user_input()
        
        # Confirm with user
        if not confirm_loading(dataset_path, mork_port, space, clear_before_load):
            print("Loading cancelled by user.")
            return
        
        # Load the dataset
        print("\nStarting dataset loading...")
        success = load_metta_dataset(dataset_path, mork_port, space, clear_before_load)
        
        if success:
            print("\n✅ Dataset loading completed successfully!")
        else:
            print("\n❌ Dataset loading failed!")
            
    except KeyboardInterrupt:
        print("\n\nOperation cancelled by user.")
    except Exception as e:
        print(f"\n❌ An unexpected error occurred: {e}")


if __name__ == "__main__":
    main()