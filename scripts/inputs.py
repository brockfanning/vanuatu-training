from InputNsdpDatabase import InputNsdpDatabase
from sdg.inputs.InputYamlMeta import InputYamlMeta
from sdg.inputs.InputCsvData import InputCsvData

def get_inputs():
    csv_input = InputCsvData(path_pattern='data/*.csv')
    nsdp_input = InputNsdpDatabase()
    indicator_config_input = InputYamlMeta(path_pattern='indicators/*.yml', git=False)
    return [
        nsdp_input,
        csv_input,
        indicator_config_input,
    ]
