import yaml
import duckdb
import pandas as pd
from utils import write_df_to_gsheets

################################################################################
# HELPER FUNCTIONS
################################################################################


# def fill_in_missing_blank_bills(bills_df):
#     """
#     Fill in missing blank bills in the DataFrame.
#     For each legislative session (leg_id), ensure all bills from HB1 to HB{max} and SB1 to SB{max} exist.
#     Missing bills are added with other columns set to NaN.
#     """
#     # Extract bill type (HB or SB) and number
#     bills_df["_bill_type"] = bills_df["bill_id"].str.extract(r"(HB|SB)")
#     bills_df["_bill_num"] = bills_df["bill_id"].str.extract(r"(\d+)").astype(float)

#     # Get max HB and SB numbers for each leg_id
#     bill_ranges = bills_df.groupby(["leg_id", "_bill_type"])["_bill_num"].max().reset_index()

#     # Generate all possible bills for each leg_id
#     all_bills = []
#     for _, row in bill_ranges.iterrows():
#         bill_type = row["_bill_type"]
#         leg_id = row["leg_id"]
#         max_num = int(row["_bill_num"])
#         for num in range(1, max_num + 1):
#             all_bills.append({"bill_id": f"{bill_type}{num}", "leg_id": leg_id})

#     all_bills_df = pd.DataFrame(all_bills)

#     # Merge with original data, keeping all bills and filling missing columns with NaN
#     expanded_bills = all_bills_df.merge(bills_df, on=["bill_id", "leg_id"], how="left")

#     # Keep original column order and fill missing values with NaN
#     expanded_bills = expanded_bills[bills_df.columns].copy()

#     # remove _bill_type and _bill_num columns
#     expanded_bills = expanded_bills.drop(columns=["_bill_type", "_bill_num"])

#     return expanded_bills

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
        write_df_to_gsheets(curr_df, google_sheets_id, worksheet_name, minimize_to_rows=True, minimize_to_cols=True, replace_headers=replace_headers)


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