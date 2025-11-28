import sys
import yaml
import subprocess

def get_file_content(commit, filename):
    try:
        return subprocess.check_output(['git', 'show', f'{commit}:{filename}'], text=True)
    except subprocess.CalledProcessError:
        return None

def get_outdirs_from_config(config_content, changed_config_items):
    if not config_content:
        return set()

    config = yaml.safe_load(config_content)
    outdirs = set()

    changed_items = [item.strip() for item in changed_config_items.split(',') if item.strip()]

    for item in changed_items:
        if item in config and 'outdir' in config[item]:
            outdirs.add(config[item]['outdir'])

    return outdirs

def main():
    if len(sys.argv) != 4:
        print("Usage: python get_config_outdirs.py <base_commit> <head_commit> <changed_config_items>")
        sys.exit(1)

    base_commit = sys.argv[1]
    head_commit = sys.argv[2]
    changed_config_items = sys.argv[3]

    if not changed_config_items:
        print("")
        return

    filename = 'config/adapters_config_sample.yaml'

    old_config_content = get_file_content(base_commit, filename)
    new_config_content = get_file_content(head_commit, filename)

    old_outdirs = get_outdirs_from_config(old_config_content, changed_config_items)
    new_outdirs = get_outdirs_from_config(new_config_content, changed_config_items)

    all_outdirs = old_outdirs | new_outdirs

    if all_outdirs:
        print(f"Old outdirs: {','.join(sorted(old_outdirs))}", file=sys.stderr)
        print(f"New outdirs: {','.join(sorted(new_outdirs))}", file=sys.stderr)
        print(f"All outdirs to update: {','.join(sorted(all_outdirs))}", file=sys.stderr)

    print(','.join(sorted(all_outdirs)))

if __name__ == "__main__":
    main()
