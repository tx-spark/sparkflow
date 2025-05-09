import os
import requests
import zipfile
import pandas as pd
from utils import dataframe_to_bigquery, get_secret, determine_git_environment

CAMPAIGN_FINANCE_URL = 'https://prd.tecprd.ethicsefile.com/public/cf/public/TEC_CF_CSV.zip'
PROJECT_ID = get_secret(secret_id='GCP_PROJECT_ID')
ENV = determine_git_environment()

def load_campaign_finance_data():

    # Create data directory if it doesn't exist
    os.makedirs('./data', exist_ok=True)

    # Download the campaign finance data
    response = requests.get(CAMPAIGN_FINANCE_URL)
    if response.status_code == 200:
        zip_path = './data/TEC_CF_CSV.zip'
        # Save zip file
        with open(zip_path, 'wb') as f:
            f.write(response.content)
        
        # Extract zip contents
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            zip_ref.extractall('./data')
            
        # Remove zip file after extraction
        os.remove(zip_path)
    else:
        raise Exception(f"Failed to download data: {response.status_code}")

    # Get list of all files in data directory
    data_files = [f for f in os.listdir('./data') if f.endswith('.csv')]

    # Process files
    for filename in data_files:
        base_name = filename.split('.')[0]
        
        try:
            # Check if this is a numbered chunk file
            if base_name.endswith(tuple('0123456789')) and '_' in base_name:
                # Get the base table name without the chunk number
                table_name = base_name.rsplit('_', 1)[0]
                # If file ends in _01, truncate the table, otherwise append
                write_disposition = 'drop' if base_name.endswith('_01') else 'append'
            else:
                # For non-chunked files, use the full base name and truncate
                table_name = base_name
                write_disposition = 'drop'

            print(f"Processing {filename} into table {table_name}")
            
            # Read full CSV since dataframe_to_bigquery handles chunking
            df = pd.read_csv(
                f'./data/{filename}',
                dtype=str
            )
            
            dataframe_to_bigquery(
                df=df,
                project_id=PROJECT_ID,
                dataset_id='tx_leg_raw_bills', 
                table_id=table_name,
                env=ENV,
                write_disposition=write_disposition
            )
                
            print(f"Successfully loaded {filename}")
            
        except Exception as e:
            print(f"Error processing {filename}: {str(e)}")
            continue

if __name__ == '__main__':
    load_campaign_finance_data()