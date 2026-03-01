import sys
import argparse
import subprocess
from pathlib import Path

def run_mork_query(file_path, pattern, template):
    is_metta = file_path.endswith(".metta")
    space_name = file_path
    if space_name.endswith(".act"): space_name = space_name[:-4]
    if space_name.endswith(".metta"): space_name = space_name[:-6]

    if is_metta:
        metta_script = f"(exec 0 (I (BTM {pattern})) (, ({template})))"
        mork_run_cmd = f"/app/MORK/target/release/mork run --aux-path /dev/shm/{file_path} /dev/shm/query.metta"
    else:
        metta_script = f"(exec 0 (I (ACT {space_name} {pattern})) (, ({template})))"
        mork_run_cmd = "/app/MORK/target/release/mork run /dev/shm/query.metta"
    
    sh_cmd = (
        f"mkdir -p /dev/shm/$(dirname '{file_path}') && "
        f"cp /app/data/{file_path} /dev/shm/{file_path} && "
        f"echo '{metta_script}' > /dev/shm/query.metta && "
        f"{mork_run_cmd}"
    )

    cmd = ["docker", "compose", "run", "--rm", "-T", "mork", "sh", "-c", sh_cmd]
    
    try:
        result = subprocess.run(cmd, capture_output=True, text=True)
        if result.returncode == 0:
            return result.stdout
        else:
            return f"Error ({result.returncode}):\n{result.stderr}\n{result.stdout}"
    except Exception as e:
        return f"Execution fail: {e}"

def main():
    parser = argparse.ArgumentParser(description="MORK REPL for BioAtomSpace queries.")
    args = parser.parse_args()

    print("\n--- MORK BioAtomSpace REPL ---")
    print("Type 'quit' or 'exit' to exit.")
    
    while True:
        try:
            print("\n" + "="*40)
            user_input_path = input("File to load (e.g. reactome/nodes.act or .metta)> ").strip()
            if user_input_path.lower() in ('quit', 'exit'): break
            if not user_input_path: continue

            pattern = input("Pattern (e.g. (Gene $g))> ").strip()
            if pattern.lower() in ('quit', 'exit'): break
            if not pattern: continue

            template = input("Return (e.g. $g)> ").strip()
            if template.lower() in ('quit', 'exit'): break
            if not template: template = "(result)"

            normalized_path = user_input_path.replace("output/", "", 1)
            if normalized_path.startswith("/app/data/"): 
                normalized_path = normalized_path.replace("/app/data/", "", 1)
            
            if not normalized_path.endswith(".act") and not normalized_path.endswith(".metta"):
                normalized_path += ".act"

            print(f"Executing query on {normalized_path}...")
            output = run_mork_query(normalized_path, pattern, template)
            
            if output and output.strip():
                 print(f"Result:\n{output.strip()}")
            else:
                 print("Result: (no output returned)")
                     
        except (KeyboardInterrupt, EOFError):
            print("\nExited!")
            break

if __name__ == "__main__":
    main()
