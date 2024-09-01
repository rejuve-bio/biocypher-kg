import subprocess
import os
import re

def get_changed_files():
    try:
        output = subprocess.check_output(['git', 'diff', '--name-only', 'HEAD^', 'HEAD'], text=True)
        return output.splitlines()
    except subprocess.CalledProcessError as e:
        print(f"Error getting changed files: {e}")
        return []

def detect_writer_changes():
    changed_files = get_changed_files()
    
    writer_pattern = re.compile(r'biocypher_metta/(metta|neo4j_csv|prolog)_writer\.py')
    changed_writers = set()

    for file in changed_files:
        match = writer_pattern.search(file)
        if match:
            changed_writers.add(match.group(1))

    all_writers = "metta,neo4j_csv,prolog"
    
    output_file = os.path.join(os.environ.get('GITHUB_WORKSPACE', ''), '.github/changed_writers.txt')
    os.makedirs(os.path.dirname(output_file), exist_ok=True)
    with open(output_file, 'w') as f:
        f.write(','.join(changed_writers))

    print(f"Changed writers: {', '.join(changed_writers)}")
    print(f"All writers: {all_writers}")

    # Set GitHub Actions output
    with open(os.environ['GITHUB_OUTPUT'], 'a') as f:
        f.write(f"writers={','.join(changed_writers)}\n")
        f.write(f"all_writers={all_writers}\n")

if __name__ == "__main__":
    detect_writer_changes()