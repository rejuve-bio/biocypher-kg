import sys
import argparse
import subprocess
import os
from pathlib import Path

def run_mork_query(target_space, pattern, template, data_dir, format_type):
    host_data_dir = os.environ.get("MORK_DATA_DIR", "./output")
    
    if format_type == "act":
        setup_cmd = f"ln -sf /app/data/{target_space}.act /dev/shm/{target_space}.act"
        query = f'(exec 0 (I (ACT {target_space} {pattern})) (, ({template})))'
        full_cmd = f"/app/MORK/target/release/mork run /app/data/query.metta"
        
    elif format_type == "metta":
        metta_files = []
        for root, _, files in os.walk(host_data_dir):
            for f in files:
                if f.endswith(".metta") and "query.metta" not in f and not f.startswith("q"):
                    rel_path = os.path.relpath(os.path.join(root, f), host_data_dir)
                    metta_files.append(f"/app/data/{rel_path}")
        aux_flags = " ".join([f'--aux-path "{f}"' for f in metta_files])
        query = f'(exec 0 (, {pattern}) (, {template}))'
        setup_cmd = "true"
        full_cmd = f"/app/MORK/target/release/mork run {aux_flags} /app/data/query.metta"
    
    elif format_type == "paths":
        paths_files = []
        for root, _, files in os.walk(host_data_dir):
            for f in files:
                if f.endswith(".paths") and "query" not in f and not f.startswith("q"):
                    rel_path = os.path.relpath(os.path.join(root, f), host_data_dir)
                    paths_files.append(f"/app/data/{rel_path}")
        
        convert_cmds = " && ".join(
            [f'/app/MORK/target/release/mork convert paths metta "$" "_1" "{f}" "/tmp/p{i}.metta"'
             for i, f in enumerate(paths_files)]
        )
        if not convert_cmds:
            convert_cmds = "true"
            
        aux_flags = " ".join([f'--aux-path "/tmp/p{i}.metta"' for i in range(len(paths_files))])
        query = f'(exec 0 (, {pattern}) (, {template}))'
        setup_cmd = "true"
        full_cmd = f"{convert_cmds} && /app/MORK/target/release/mork run {aux_flags} /app/data/query.metta"

    query_file = os.path.join(host_data_dir, "query.metta")
    with open(query_file, "w") as f:
        f.write(query)

    docker_cmd = [
        "docker", "compose", "run", "--rm", "mork",
        "sh", "-c", f"{setup_cmd} && {full_cmd}"
    ]
    
    result = subprocess.run(docker_cmd, capture_output=True, text=True)

    if result.returncode == 0:
        output = result.stdout
        if "result:\n" in output:
            return output.split("result:\n")[-1].strip()
        return output.strip() or "(no matches found)"

    return f"Error ({result.returncode}):\n{result.stderr}\n{result.stdout}"

def main():
    parser = argparse.ArgumentParser(description="MORK Multi-Format REPL for BioAtomSpace.")
    parser.add_argument("data_dir", nargs="?", default=os.environ.get("MORK_DATA_DIR", "output"))
    parser.add_argument("--format", choices=["act", "paths", "metta"], default="act")
    args = parser.parse_args()

    data_path = Path(args.data_dir).resolve()
    if data_path.is_file():
        data_dir = data_path.parent
        target = data_path.stem
    else:
        data_dir = data_path
        target = "annotation"

    os.environ["MORK_DATA_DIR"] = str(data_dir)
    
    if args.format == "act":
        act_file = data_dir / f"{target}.act"
        if not act_file.exists():
            print(f"Error: Could not find '{act_file}'")
            print("Please run 'bash scripts/build_act.sh <dir>' first to generate it.")
            sys.exit(1)

    print("\n" + "="*40)
    print(f"MORK MULTI-FORMAT REPL")
    print(f"Target: {target}")
    print(f"Format: {args.format.upper()}")
    print("-" * 40)
    print("Type 'quit' or 'exit' to exit.")
    print("="*40)
    
    while True:
        try:
            pattern = input(f"\n[{args.format.upper()}] Query Pattern> ").strip()
            if pattern.lower() in ('quit', 'exit'):
                print("Exited!")
                break
            if not pattern:
                print("Error: Pattern cannot be empty.")
                continue

            template = input("Return Template> ").strip()
            if template.lower() in ('quit', 'exit'):
                print("Exited!")
                break
            if not template:
                print("Warning: Template cannot be empty. Defaulting to '()'.")
                template = "()"
            
            print(f"Searching...")
            output = run_mork_query(target, pattern, template, data_dir, args.format)
            
            if output and output.strip():
                print(f"\nResult:\n{output.strip()}")
            else:
                print("\nResult: (no matches found)")
                     
        except (KeyboardInterrupt, EOFError):
            print("\nExited!")
            break

if __name__ == "__main__":
    main()
