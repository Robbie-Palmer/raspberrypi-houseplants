import yaml

_CONFIGS_FILE = './configs/configs.yaml'
CONFIGS = {}
with open(_CONFIGS_FILE, 'r') as config_file:
    CONFIGS = yaml.load(config_file, Loader=yaml.CLoader)
