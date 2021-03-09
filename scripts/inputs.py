#from InputNsdpDatabase import InputNsdpDatabase
from sdg.inputs.InputYamlMeta import InputYamlMeta
from sdg.inputs.InputCsvData import InputCsvData
from sdg.inputs.InputCsvMeta import InputCsvMeta

def get_inputs():
    data_input = InputCsvData(path_pattern='data/*.csv')
    meta_input = InputCsvMeta(path_pattern='meta/*.csv', git=False)
    indicator_config_input = InputYamlMeta(path_pattern='indicators/*.yml', git=False)
    progress_input = InputYamlMeta(path_pattern='progress/*.yml', git=False)
    return [
        data_input,
        meta_input,
        indicator_config_input,
        progress_input,
    ]

def alter_meta(meta):
    if 'NSDPIndicatorID' in meta:
        # Auto-calculate settings based on 'NSDPIndicatorID'.
        open_sdg_id = meta['NSDPIndicatorID'].replace(' ', '').replace('.', '-')
        meta['indicator_number'] = open_sdg_id
        meta['goal_number'] = open_sdg_id.split('-')[0]
        meta['goal_name'] = 'global_goals.' + meta['goal_number'] + '-title'
        meta['target_number'] = open_sdg_id.split('-')[0] + '-' + open_sdg_id.split('-')[1]
        meta['target_name'] = 'global_targets.' + meta['target_number'] + '-title'
        meta['indicator_name'] = 'indicators.' + open_sdg_id + '-title'
        meta['national_geographical_coverage'] = 'Vanuatu'
    return meta
