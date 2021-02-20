from sdg.open_sdg import open_sdg_check
from inputs import get_inputs

# Validate the indicators.
validation_successful = open_sdg_check(config='config_data.yml', inputs=get_inputs())

# If everything was valid, perform the build.
if not validation_successful:
    raise Exception('There were validation errors. See output above.')
