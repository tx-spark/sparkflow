from pipelines.utils.utils import write_df_to_gsheets, get_secret,  bigquery_to_df
from datetime import datetime, timedelta, timezone
import pandas as pd
import gspread
import yaml
import json
import re


def upload_call2action(leg_id, env = 'dev'):

    if env=='dev':
        return

    date_col = 'meeting_datetime'
    curr_date = datetime.now().strftime('%m-%d-%Y')
    gsheets_id = '1XRBXMeOMmmdDr5MdcCHkidjpoh4JKu7kUrQ-MlTfTCw'

    df = bigquery_to_df('txspark','tx_leg_bills', 'call2action', env = 'prod')
    # Filter to specified leg_id and drop leg_id column
    df = df[df['leg_id'] == leg_id].drop('leg_id', axis=1)
    df = df[pd.notnull(df['Position'])]
    # Convert date strings to datetime
    df[date_col] = pd.to_datetime(df[date_col])
    curr_date = datetime.strptime(curr_date, '%m-%d-%Y')

    credentials_str = get_secret(secret_id='GOOGLE_SHEETS_SERVICE_ACCOUNT')
    credentials = json.loads(credentials_str)
    gc = gspread.service_account_from_dict(credentials)

    sh = gc.open_by_key(gsheets_id)
    worksheets = sh.worksheets()
    worksheet_names = [worksheet.title for worksheet in worksheets]

    worksheet_links = []

    # Write each date's data to its own sheet
    for i in range(7):
        date = curr_date + timedelta(days=i)
        worksheet_name = date.strftime('%A (%m/%d/%Y)')
        print(f"Writing data for {worksheet_name}")

        curr_date_worksheet = [worksheet_title for worksheet_title in worksheet_names if re.match(f'^{date.strftime('%A')}', worksheet_title)]
        if len(curr_date_worksheet) <= 0:
            sh.add_worksheet(date.strftime('%A (%m/%d/%Y)'), rows = 1, cols = 1)
        else:
            sh.worksheet(curr_date_worksheet[0]).update_title(worksheet_name)
        # print(curr_date_worksheet)

        date_df = df[df[date_col].dt.date == date.date()]
        print(date_df)

        hide = False
        if len(date_df) <= 0:
            date_df = pd.DataFrame([{col: '—-' for col in df.columns}])
            hide = True

        write_df_to_gsheets(
            date_df,
            gsheets_id,
            date.strftime('%A (%m/%d/%Y)'),
            minimize_to_rows=True,
            minimize_to_cols=True,
            replace_headers=False
        )

        if hide:
            sh.worksheet(worksheet_name).hide()
        else:
            sh.worksheet(worksheet_name).show()

        if not hide:
            worksheet_links.append({'link': f'=HYPERLINK("https://docs.google.com/spreadsheets/d/{gsheets_id}/view?gid={sh.worksheet(worksheet_name).id}#gid={sh.worksheet(worksheet_name).id}", "{date.strftime('%A (%m/%d/%Y)')}")'})
        else:
            worksheet_links.append({'link':f'{date.strftime('%A (%m/%d/%Y)')} - No highlighted bills being discussed'})

    # for i in range(7-len(worksheet_links)):
    #     worksheet_links.append({'links':None})

    # add in last updated timestamp to worksheet_links
    worksheet_links.append({'link':f'Last Updated: {(datetime.now(timezone.utc)  - timedelta(hours=5)).strftime("%B %d, %Y %I:%M %p")}'})
    write_df_to_gsheets(
        pd.DataFrame(worksheet_links),
        gsheets_id,
        'Info',
        replace_headers=False,
        first_cell='A4'
    )

if __name__ == '__main__':
    CONFIG_PATH = 'config.yaml'
    with open(CONFIG_PATH, 'r') as f:
        config = f.read()
    
    upload_call2action(config['info']['LegSess'], env='prod')