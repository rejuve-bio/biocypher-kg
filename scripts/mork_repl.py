import sys
import readline  # For comfortable typing with history
from mork_client import MORK

import argparse

def main():
    parser = argparse.ArgumentParser(description="MORK REPL for querying.")
    parser.add_argument("--mork-url", type=str, default="http://localhost:8027")
    args = parser.parse_args()

    print(f"Connecting to MORK at {args.mork_url}...")
    try:
        mork = MORK(base_url=args.mork_url)
        # Test connection
        cmd = mork.Status("test")
        cmd.dispatch(mork)
        # cmd.block() # Status is immediate
        if not cmd.response or cmd.response.status_code != 200:
             raise Exception(f"Status check failed: {cmd.response.status_code if cmd.response else 'No response'}")
        print("✅ Connected! Scope: 'annotation'")
    except Exception as e:
        print(f"❌ Connection failed: {e}")
        return

    print("\n--- MORK REPL ---")
    print("Type your pattern and template. 'quit' to exit.")
    print("Syntax: <pattern> <template>")
    print("Example: (transcript ENST00000353224) $x")
    
    with mork.work_at("annotation") as scope:
        while True:
            try:
                print("\n" + "-"*30)
                pattern = input("Pattern>  ").strip()
                if pattern.lower() in ('quit', 'exit'):
                    break
                if not pattern:
                    continue

                template = input("Template> ").strip()
                if not template:
                     template = "$x" # Default template
                     print(f"Using default template: {template}")

                print(f"Executing...")
                try:
                    cmd = scope.download(pattern, template)
                    if cmd.data:
                         print(f"Result:   {cmd.data}")
                    else:
                         print("Result:   (no match found)")
                except Exception as e:
                     print(f"Error: {e}")

            except KeyboardInterrupt:
                print("\nUse 'quit' to exit.")
            except EOFError:
                break

    print("\nExited!")

if __name__ == "__main__":
    main()
