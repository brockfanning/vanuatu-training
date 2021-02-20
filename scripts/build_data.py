from sdg.open_sdg import open_sdg_build
from InputNsdpDatabase import InputNsdpDatabase

nsdp_input = InputNsdpDatabase()
open_sdg_build(config='config_data.yml', inputs=[nsdp_input])
