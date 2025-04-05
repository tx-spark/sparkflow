import yaml
import duckdb
import pandas as pd
from utils import write_df_to_gsheets

################################################################################
# HELPER FUNCTIONS
################################################################################

def get_current_table_data(duckdb_conn, table_name, dataset_name, leg_id=None):
    curr_df = None
    if duckdb_conn.sql(f"SELECT count(*) FROM information_schema.tables WHERE table_name = '{table_name}' AND table_schema = '{dataset_name}'").fetchone()[0] > 0:
        print(f"Table `{table_name}` exists in dataset `{dataset_name}`")
        curr_df = duckdb_conn.sql(f"""
            SELECT 
                * exclude (leg_id)
            FROM {dataset_name}.{table_name}
            {f"WHERE leg_id = '{leg_id}'" if leg_id else ""}
        """).df()
    else:
        raise ValueError(f"Table `{table_name}` does not exist in dataset `{dataset_name}`")

    if curr_df is not None:
        # Convert Int32 columns to strings for ease of pasting into google sheets 
        for col in curr_df.select_dtypes(include=['Int32']).columns:
            curr_df[col] = curr_df[col].astype('str')
    
    return curr_df


def upload_table_to_gsheets(duckdb_conn,dataset_name, table_name, google_sheets_id, worksheet_name, leg_id=None, replace_headers=True):
    curr_df = get_current_table_data(duckdb_conn, table_name, dataset_name, leg_id)

    if curr_df is not None:
        write_df_to_gsheets(curr_df, google_sheets_id, worksheet_name, minimize_to_rows=True, minimize_to_cols=False, replace_headers=replace_headers)


################################################################################
# MAIN 
################################################################################
def main():
    """
    Main execution function that:
    1. Loads the scraper configuration
    2. Scrapes both house and senate bills
    3. Saves results to TSV files
    """
    # Load config
    CONFIG_PATH = 'config.yaml'

    with open(CONFIG_PATH, 'r') as file:
        scraper_config = yaml.safe_load(file)

    google_sheets_id = scraper_config['config']['google_sheets_id']

    duckdb_conn = duckdb.connect('texas_bills.duckdb')

    # Scrape house bills
    upload_table_to_gsheets(duckdb_conn, 'bills', 'house_tracker', google_sheets_id, 'All House Bills', leg_id=scraper_config['info']['LegSess'], replace_headers=False)
    upload_table_to_gsheets(duckdb_conn, 'bills', 'senate_tracker', google_sheets_id, 'All Senate Bills', leg_id=scraper_config['info']['LegSess'], replace_headers=False)
    upload_table_to_gsheets(duckdb_conn, 'bills', 'house_joint_resolution_tracker', google_sheets_id, 'All HJRs', leg_id=scraper_config['info']['LegSess'], replace_headers=False)
    upload_table_to_gsheets(duckdb_conn, 'bills', 'senate_joint_resolution_tracker', google_sheets_id, 'All SJRs', leg_id=scraper_config['info']['LegSess'], replace_headers=False)
    upload_table_to_gsheets(duckdb_conn, 'bills', 'house_bills_past_committee', google_sheets_id, 'House Bills Past Committee', leg_id=scraper_config['info']['LegSess'], replace_headers=False)
    upload_table_to_gsheets(duckdb_conn, 'bills', 'house_bills_by_hearing_date', google_sheets_id, 'H Bills By H Hearing Date', leg_id=scraper_config['info']['LegSess'], replace_headers=False)
    upload_table_to_gsheets(duckdb_conn, 'bills', 'senate_bills_by_hearing_date', google_sheets_id, 'S Bills By S Hearing Date', leg_id=scraper_config['info']['LegSess'], replace_headers=False)
    upload_table_to_gsheets(duckdb_conn, 'bills', 'house_bills_past_committee', google_sheets_id, 'House Bills Past Committee', leg_id=scraper_config['info']['LegSess'], replace_headers=False)

    # Blue Action Data
    blue_action_master_data_spreadsheet_id = '1Wr0OIVbsquKTxGlnluSteBb1164JEUa1rlC-9C760jQ'
    upload_table_to_gsheets(duckdb_conn, 'bills', 'blue_action_data', blue_action_master_data_spreadsheet_id, 'Bills Data', leg_id=scraper_config['info']['LegSess'], replace_headers=False)
    # upload_table_to_gsheets(duckdb_conn, 'bills', 'committee_membership', blue_action_master_data_spreadsheet_id, 'Committee Members', leg_id=scraper_config['info']['LegSess'], replace_headers=False)

if __name__ == "__main__":
    main()