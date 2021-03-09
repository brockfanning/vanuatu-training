from sdg.open_sdg import open_sdg_build
from inputs import get_inputs
from inputs import alter_meta

open_sdg_build(config='config_data.yml', inputs=get_inputs(), alter_meta=alter_meta)
