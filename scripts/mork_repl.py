import sys
import argparse
import subprocess
from pathlib import Path

def run_mork_query(act_file_name, pattern, template):
    metta_script = f"(exec 0 (I (ACT {act_file_name} {pattern})) (, ({template})))"
    
    sh_cmd = (
        f"mkdir -p /dev/shm/$(dirname '{act_file_name}') && "
        f"cp /app/data/{act_file_name}.act /dev/shm/{act_file_name}.act && "
        f"echo '{metta_script}' > /dev/shm/query.metta && "
        f"/app/MORK/target/release/mork run /dev/shm/query.metta"
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
            act_file = input("ACT File (e.g. reactome/nodes)> ").strip()
            if act_file.lower() in ('quit', 'exit'): break
            if not act_file: continue

            pattern = input("Pattern (e.g. (Gene $g))> ").strip()
            if pattern.lower() in ('quit', 'exit'): break
            if not pattern: continue

            template = input("Return (e.g. $g)> ").strip()
            if template.lower() in ('quit', 'exit'): break
            if not template: template = "(result)"

            clean_name = act_file.replace("output/", "", 1)
            if clean_name.startswith("/app/data/"): clean_name = clean_name.replace("/app/data/", "", 1)
            if clean_name.endswith(".act"): clean_name = clean_name[:-4]

            print(f"Executing query on {clean_name}...")
            output = run_mork_query(clean_name, pattern, template)
            
            if output and output.strip():
                 print(f"Result:\n{output.strip()}")
            else:
                 print("Result: (no output returned)")
                     
        except (KeyboardInterrupt, EOFError):
            print("\nExited!")
            break

if __name__ == "__main__":
    main()
