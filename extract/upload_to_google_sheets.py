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

if __name__ == "__main__":
    main()