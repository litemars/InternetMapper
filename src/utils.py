import yaml

def parse_config_yaml(config_path: str = 'config.yaml'):
    with open(config_path, 'r') as f:
        return yaml.safe_load(f)
