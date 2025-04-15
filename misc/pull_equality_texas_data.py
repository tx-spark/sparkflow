import requests
import pandas as pd
import json
import datetime
import os

pro_lgbt_bills = 'https://www.billtrack50.com/webapi/stakeholder/bills/public/1w6CBm1U5EKW8hQZ7nEA8Q'
anti_lgbt_bills = 'https://www.billtrack50.com/webapi/stakeholder/bills/public/cKC91m_MlkqTBhaj5m7xYg'


headers = {
    'accept': 'application/json, text/plain, */*',
    'accept-encoding': 'gzip, deflate, br, zstd',
    'accept-language': 'en-US,en;q=0.9',
    'content-length': '0',
    'origin': 'https://www.billtrack50.com',
    'referer': 'https://www.billtrack50.com/public/stakeholderpage/1w6CBm1U5EKW8hQZ7nEA8Q/embed',
    'sec-ch-ua': '"Chromium";v="134", "Not:A-Brand";v="24", "Google Chrome";v="134"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"macOS"',
    'sec-fetch-dest': 'empty',
    'sec-fetch-mode': 'cors', 
    'sec-fetch-site': 'same-origin',
    'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36'
}

pro_lgbt_bills_response = requests.post(pro_lgbt_bills, headers=headers)
anti_lgbt_bills_response = requests.post(anti_lgbt_bills, headers=headers)

pro_lgbt_bills_data = pro_lgbt_bills_response.json()
anti_lgbt_bills_data = anti_lgbt_bills_response.json()

print(pro_lgbt_bills_data)
print(anti_lgbt_bills_data)

pro_lgbt_bills_df = pd.DataFrame(pro_lgbt_bills_data)
anti_lgbt_bills_df = pd.DataFrame(anti_lgbt_bills_data)

pro_lgbt_bills_df['stance'] = 'Pro LGBT'
anti_lgbt_bills_df['stance'] = 'Anti LGBT'

all_bills_df = pd.concat([pro_lgbt_bills_df, anti_lgbt_bills_df])

all_bills_df.to_csv('equality_texas_bills.csv', index=False, sep='\t')
