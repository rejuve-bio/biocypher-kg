import sys
import yaml

def prepare_config(changed_adapters, config_changed, changed_config_items):
    with open('config/adapters_config_sample.yaml', 'r') as f:
        full_config = yaml.safe_load(f)
    
    test_config = {}
    changed_adapter_list = changed_adapters.split(',') if changed_adapters else []
    changed_config_list = changed_config_items.split(',') if changed_config_items else []
    
    if changed_adapter_list:
        for key, value in full_config.items():
            adapter_name = value['adapter']['module'].split('.')[-1].replace('_adapter', '')
            if adapter_name in changed_adapter_list:
                test_config[key] = value
    
    if config_changed == 'true':
        for key in changed_config_list:
            if key in full_config and key not in test_config:
                test_config[key] = full_config[key]
    
    if not test_config:
        test_config = full_config
    
    with open('config/test_config.yaml', 'w') as f:
        yaml.dump(test_config, f)

if __name__ == "__main__":
    changed_adapters = sys.argv[1] if len(sys.argv) > 1 else ""
    config_changed = sys.argv[2] if len(sys.argv) > 2 else "false"
    changed_config_items = sys.argv[3] if len(sys.argv) > 3 else ""
    prepare_config(changed_adapters, config_changed, changed_config_items)