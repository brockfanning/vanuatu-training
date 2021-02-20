from sdg.open_sdg import open_sdg_check
from InputNsdpDatabase import InputNsdpDatabase

# Validate the indicators.
nsdp_input = InputNsdpDatabase()
validation_successful = open_sdg_check(config='config_data.yml', inputs=[nsdp_input])

# If everything was valid, perform the build.
if not validation_successful:
    raise Exception('There were validation errors. See output above.')
