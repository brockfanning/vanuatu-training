from InputNsdpDatabase import InputNsdpDatabase
from sdg.inputs.InputYamlMeta import InputYamlMeta

def get_inputs():
    nsdp_input = InputNsdpDatabase()
    indicator_config_input = InputYamlMeta(path_pattern='indicators/*.yml', git=False)
    return [
        nsdp_input,
        indicator_config_input,
    ]
