import logging

from pipelines.flows.custom_gsheets import upload_call2action
from pipelines.flows.tx_leg import download_google_sheets
from pipelines.utils.utils import upload_google_sheets

################################################################################
# CONFIGURATION
################################################################################

CONFIG_PATH = "config.yaml"
GSHEETS_CONFIG_PATH = "gsheets_runs.yaml"
DUCKDB_NAME = "texas_bills"
LOG_PATH = "tx-leg.log"
OUT_DATASET_NAME = "tx_leg_raw_bills"
ENV = "prod"

################################################################################
# MAIN
################################################################################

if __name__ == "__main__":
    download_google_sheets(GSHEETS_CONFIG_PATH)
    upload_google_sheets(GSHEETS_CONFIG_PATH, CONFIG_PATH, ENV)

    with open(CONFIG_PATH, "r") as f:
        config = f.read()

    upload_call2action(config["info"]["LegSess"], ENV)
