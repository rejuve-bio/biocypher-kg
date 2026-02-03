import yaml
import subprocess
import os
import sys

def get_file_content(commit, filename):
    try:
        return subprocess.check_output(['git', 'show', f'{commit}:{filename}'], text=True)
    except subprocess.CalledProcessError:
        return None

def detect_config_changes(species='hsa'):
    current_commit = subprocess.check_output(['git', 'rev-parse', 'HEAD'], text=True).strip()
    previous_commit = subprocess.check_output(['git', 'rev-parse', 'HEAD^'], text=True).strip()
    
    filename = f'config/{species}/{species}_adapters_config_sample.yaml'
    
    current_content = get_file_content(current_commit, filename)
    previous_content = get_file_content(previous_commit, filename)
    
    if current_content is None or previous_content is None:
        print(f"Error: Couldn't retrieve content for {filename}")
        return
    
    current_config = yaml.safe_load(current_content)
    previous_config = yaml.safe_load(previous_content)
    
    changed_items = []
    
    for key in current_config:
        if key not in previous_config or current_config[key] != previous_config[key]:
            changed_items.append(key)
    
    for key in previous_config:
        if key not in current_config:
            changed_items.append(key)
    
    output_workspace = os.environ.get('GITHUB_WORKSPACE', os.getcwd())
    output_file = os.path.join(output_workspace, '.github/changed_config_items.txt')
    os.makedirs(os.path.dirname(output_file), exist_ok=True)
    with open(output_file, 'w') as f:
        f.write(','.join(changed_items))
    
    print(f"Changed config items for {species}: {', '.join(changed_items)}")

if __name__ == "__main__":
    species = sys.argv[1] if len(sys.argv) > 1 else 'hsa'
    detect_config_changes(species)
