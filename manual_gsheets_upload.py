import logging

from pipelines.flows.utils import upload_google_sheets

################################################################################
# CONFIGURATION
################################################################################

CONFIG_PATH = 'config.yaml'
GSHEETS_CONFIG_PATH = 'gsheets_runs.yaml'
DUCKDB_NAME = "texas_bills"
LOG_PATH = 'tx-leg.log'
OUT_DATASET_NAME = 'tx_leg_raw_bills'
ENV = 'prod'

################################################################################
# MAIN
################################################################################

if __name__ == "__main__":

    upload_google_sheets(GSHEETS_CONFIG_PATH, CONFIG_PATH, ENV)