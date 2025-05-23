import os
import re
import requests
import zipfile
import pandas as pd
from utils import dataframe_to_bigquery, get_secret

OUT_DATASET_NAME = 'tx_leg_raw_bills'
PROJECT_ID = get_secret(secret_id='GCP_PROJECT_ID')
ENV = 'prod'

# election_results_location = 'https://data.capitol.texas.gov/dataset/35b16aee-0bb0-4866-b1ec-859f1f044241/resource/e1cd6332-6a7a-4c78-ad2a-852268f6c7a2/download/2024-general-vtds-election-data.zip'

# # Create data directory if it doesn't exist
# os.makedirs('./data/election_results', exist_ok=True)

# # Download the zip file
# response = requests.get(election_results_location)
# if response.status_code == 200:
#     zip_path = './data/election_results/election_data.zip'
#     # Save zip file
#     with open(zip_path, 'wb') as f:
#         f.write(response.content)
    
#     # Extract zip contents
#     with zipfile.ZipFile(zip_path, 'r') as zip_ref:
#         zip_ref.extractall('./data/election_results')
        
#     # Remove zip file after extraction
#     os.remove(zip_path)
# else:
#     raise Exception(f"Failed to download data: {response.status_code}")

# Get list of all files in election_results directory
data_files = [f for f in os.listdir('./data/election_results') if f.endswith('.csv')]

# Process files that match the pattern
election_results_df = pd.DataFrame()
for filename in data_files:
    if re.match('[0-9]+_General_Election_Returns.csv', filename):
        print(f"\nReading {filename}:")
        df = pd.read_csv(f'./data/election_results/{filename}')
        # Extract year from filename
        year = int(filename.split('_')[0])
        # Add year and election_type columns
        df['year'] = year
        df['election_type'] = 'General'
        # Append to main dataframe
        election_results_df = pd.concat([election_results_df, df], ignore_index=True)

dataframe_to_bigquery(election_results_df, PROJECT_ID, OUT_DATASET_NAME, 'historical_precinct_election_results', env=ENV,write_disposition='drop', chunk_size=500000)

