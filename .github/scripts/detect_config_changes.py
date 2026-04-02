import yaml
import subprocess
import os


def get_file_content(commit, filename):
    try:
        return subprocess.check_output(['git', 'show', f'{commit}:{filename}'], text=True)
    except subprocess.CalledProcessError:
        return None


def get_previous_commit(current_commit):
    """
    Returns the parent commit of current_commit, or None if this is the
    first commit (i.e. no parent exists — e.g. initial commit or shallow clone).
    """
    try:
        return subprocess.check_output(
            ['git', 'rev-parse', f'{current_commit}^'],
            text=True,
            stderr=subprocess.DEVNULL
        ).strip()
    except subprocess.CalledProcessError:
        return None


def detect_config_changes():
    current_commit = subprocess.check_output(['git', 'rev-parse', 'HEAD'], text=True).strip()
    previous_commit = get_previous_commit(current_commit)

    filename = 'config/hsa/hsa_adapters_config_sample.yaml'

    current_content = get_file_content(current_commit, filename)

    if previous_commit is None:
        # No parent commit (initial commit or shallow clone) — treat everything as changed
        print("Warning: No parent commit found. Treating all config items as changed.")
        if current_content is None:
            print(f"Error: Couldn't retrieve content for {filename}")
            return
        current_config = yaml.safe_load(current_content)
        changed_items = list(current_config.keys())
    else:
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

    output_file = os.path.join(os.environ['GITHUB_WORKSPACE'], '.github/changed_config_items.txt')
    with open(output_file, 'w') as f:
        f.write(','.join(changed_items))

    print(f"Changed config items: {', '.join(changed_items)}")


if __name__ == "__main__":
    detect_config_changes()
