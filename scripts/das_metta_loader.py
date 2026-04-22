"""
das_metta_loader.py — Load MeTTa files into DAS (Distributed AtomSpace) via das-cli.

OVERVIEW
--------
This script recursively walks a directory for all `.metta` files and loads each
one into a running DAS instance using the `das-cli metta load` command.


PREREQUISITES
-------------
1. **das-cli**
   The `das-cli` tool must be installed and accessible on your PATH.
   Setup guide: https://github.com/singnet/das-toolbox/blob/master/das-cli/README.md

2. **DAS (Distributed AtomSpace)**
   DAS must be installed and running before executing this script.
   Setup guide: https://github.com/singnet/das/blob/master/README.md

3. **DAS server must be running**
   Start your local DAS instance before running this script:
       das-cli server start
   Then confirm it is reachable:
       das-cli server ping

USAGE
-----
    python scripts/das_metta_loader.py <directory>

Arguments:
    <directory>   Path to the root directory containing .metta files.
                  The script walks subdirectories recursively.

Examples:
    # Load all .metta files from the default output directory
    python scripts/das_metta_loader.py ./output

"""

import os
import subprocess
import sys
import time


def load_metta_files(root_dir):
    start_time = time.time()

    success = []
    failed = []
    total = 0

    for dirpath, _, filenames in os.walk(root_dir):
        for filename in filenames:
            if filename.endswith(".metta"):
                total += 1
                file_path = os.path.abspath(os.path.join(dirpath, filename))
                print(f"\nProcessing: {file_path}")

                cmd = ["das-cli", "metta", "load", file_path]

                try:
                    result = subprocess.run(
                        cmd,
                        stdout=subprocess.DEVNULL,
                        stderr=subprocess.PIPE,
                        text=True,
                    )

                    if result.returncode == 0:
                        print("✅ SUCCESS")
                        success.append(file_path)
                    else:
                        print("❌ FAILED")
                        print(result.stderr)
                        failed.append(file_path)

                except Exception as e:
                    print(f"❌ EXCEPTION: {e}")
                    failed.append(file_path)

    end_time = time.time()
    total_time = end_time - start_time

    print("\n" + "=" * 60)
    print("SUMMARY")
    print("=" * 60)
    print(f"Total files found:    {total}")
    print(f"Successfully loaded:  {len(success)}")
    print(f"Failed:               {len(failed)}")

    if failed:
        print("\nFailed files:")
        for f in failed:
            print(f"  - {f}")

    print(f"\nTotal execution time: {total_time:.2f} seconds")
    print("=" * 60)


def check_das_cli():
    try:
        subprocess.run(["das-cli", "--version"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    except FileNotFoundError:
        print("Error: 'das-cli' is not installed or not on PATH.")
        print("Install it using the setup guide: https://github.com/singnet/das-toolbox/blob/master/das-cli/README.md")
        sys.exit(1)


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python scripts/das_metta_loader.py <directory>")
        sys.exit(1)

    directory = sys.argv[1]

    if not os.path.isdir(directory):
        print(f"Error: '{directory}' is not a valid directory.")
        sys.exit(1)

    check_das_cli()
    load_metta_files(directory)
