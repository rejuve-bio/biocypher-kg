import sys
import yaml

def main():
    if len(sys.argv) != 3:
        print("Usage: python get_adapter_outdirs.py <changed_adapters_csv> <config_file>")
        sys.exit(1)

    changed_adapters = sys.argv[1]
    config_file = sys.argv[2]

    if not changed_adapters:
        print("")
        return

    adapter_list = [a.strip() for a in changed_adapters.split(',') if a.strip()]

    with open(config_file, 'r') as f:
        config = yaml.safe_load(f)

    outdirs = set()
    for adapter in adapter_list:
        for config_key, config_value in config.items():
            if adapter.lower() in config_key.lower():
                if 'outdir' in config_value:
                    outdirs.add(config_value['outdir'])

    print(','.join(sorted(outdirs)))

if __name__ == "__main__":
    main()
