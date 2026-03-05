import sys
import argparse
import subprocess
import os
from pathlib import Path

def run_mork_query(target_space, pattern, template):
    metta_script = f'(exec 0 (I (ACT {target_space} {pattern})) (, ({template})))'

    sh_cmd = (
        f"[ -L /dev/shm/{target_space}.act ] || ln -s /app/data/{target_space}.act /dev/shm/{target_space}.act && "
        f"echo '{metta_script}' > /dev/shm/query.metta && "
        f"/app/MORK/target/release/mork run /dev/shm/query.metta"
    )

    cmd = ["docker", "compose", "run", "--rm", "-T", "mork", "sh", "-c", sh_cmd]

    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode == 0:
        output = result.stdout
        if "result:" in output:
            output = output.split("result:")[-1].strip()
        return output
    return f"Error ({result.returncode}):\n{result.stderr}\n{result.stdout}"

def main():
    parser = argparse.ArgumentParser(description="MORK REPL for BioAtomSpace.")
    parser.add_argument("data_dir", nargs="?", default=os.environ.get("MORK_DATA_DIR", "output"))
    args = parser.parse_args()

    data_path = Path(args.data_dir).resolve()
    
    if data_path.is_file():
        data_dir = data_path.parent
        target = data_path.stem
    else:
        data_dir = data_path
        target = "annotation"

    os.environ["MORK_DATA_DIR"] = str(data_dir)
    
    act_file = data_dir / f"{target}.act"
    if not act_file.exists():
        print(f"Error: Could not find '{act_file}'")
        print("Please run 'bash scripts/build_act.sh <dir>' first to generate it.")
        sys.exit(1)

    print("\n" + "="*50)
    print(f"MORK UNIFIED REPL (Space: {target})")
    print("Type 'quit' or 'exit' to exit.")
    print("="*50)
    
    while True:
        try:
            pattern = input("\nQuery Pattern (e.g. (Gene $g))> ").strip()
            if pattern.lower() in ('quit', 'exit'): break
            if not pattern: continue

            template = input("Return Template (e.g. $g)> ").strip()
            if template.lower() in ('quit', 'exit'): break
            if not template: template = "(result)"

            print(f"Searching {target}...")
            output = run_mork_query(target, pattern, template)
            
            if output and output.strip():
                print(f"\nResult:\n{output.strip()}")
            else:
                print("\nResult: (no matches found)")
                     
        except (KeyboardInterrupt, EOFError):
            print("\nExited!")
            break

if __name__ == "__main__":
    main()
