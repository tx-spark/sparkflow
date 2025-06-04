import requests
import datetime
import zipfile
import base64
import json
import yaml
import io
import os
import pandas as pd

from utils import get_secret, dataframe_to_bigquery, determine_git_environment, bigquery_to_df, query_bq

CONFIG_PATH = 'config.yaml'
LEGISCAN_API_KEY = get_secret(secret_id='LEGISCAN_API_KEY')
PROJECT_ID = get_secret(secret_id='GCP_PROJECT_ID')
DATASET_ID = 'tx_leg_raw_bills'
ENV = determine_git_environment()

###############################################################################
#                 Parsing Functions
###############################################################################
def parse_vote(votes):
    votes_dict = json.loads(votes)

    roll_call = votes_dict['roll_call']

    votes_list = []
    for vote in roll_call['votes']:
        votes_list.append({
            'roll_call_id': roll_call['roll_call_id'],
            'legiscan_bill_id': roll_call['bill_id'],
            'date': roll_call['date'],
            'desc': roll_call['desc'],
            'legiscan_people_id': vote['people_id'],
            'vote_id':  vote['vote_id'],
            'vote_text': vote['vote_text']
        })

    return votes_list

def parse_bill(bill_json):
    bill_dict = json.loads(bill_json)['bill']

    # get progress as a list of dicts
    progress = bill_dict['progress']
    for p in progress:
        p['legiscan_bill_id'] = bill_dict['bill_id']

    #referrals as list of dicts
    referrals = bill_dict['referrals']
    for r in referrals:
        r['legiscan_bill_id'] = bill_dict['bill_id']

    # calendar as list of dicts
    calendar = bill_dict['calendar']
    for c in calendar:
        c['legiscan_bill_id'] = bill_dict['bill_id']

    # ammendments as list of dicts
    amendments = bill_dict['amendments']
    for a in amendments:
        a['legiscan_bill_id'] = bill_dict['bill_id']

    # supplements as list of dicts
    supplements = bill_dict['supplements']
    for s in supplements:
        s['legiscan_bill_id'] = bill_dict['bill_id']

    # votes as list of dicts
    votes = bill_dict['votes']
    for v in votes:
        v['legiscan_bill_id'] = bill_dict['bill_id']

    # texts as list of dicts
    texts = bill_dict['texts']
    for t in texts:
        t['legiscan_bill_id'] = bill_dict['bill_id']

    # subjects as list of dicts
    subjects = bill_dict['subjects']
    for s in subjects:
        s['legiscan_bill_id'] = bill_dict['bill_id']

    # authors as list of dicts
    authors = bill_dict['sponsors']
    for a in authors:
        a['legiscan_bill_id'] = bill_dict['bill_id']

    # history as list of dicts
    history = bill_dict['history']
    for h in history:
        h['legiscan_bill_id'] = bill_dict['bill_id']

    # Create dict to store bill info
    bill_info = {
        'legiscan_bill_id': bill_dict['bill_id'],  
        'change_hash': bill_dict['change_hash'],
        'session_id': bill_dict['session_id'],
        'session_tag': bill_dict['session']['session_tag'],
        'session_title': bill_dict['session']['session_title'], 
        'session_name': bill_dict['session']['session_name'],
        'url': bill_dict['url'],
        'completed': bill_dict['completed'],
        'status': bill_dict['status'],
        'status_date': bill_dict['status_date'],
        'bill_number': bill_dict['bill_number'],
        'bill_type': bill_dict['bill_type'],
        'bill_type_id': bill_dict['bill_type_id'],
        'body': bill_dict['body'],
        'body_id': bill_dict['body_id'],
        'current_body': bill_dict['current_body'],
        'current_body_id': bill_dict['current_body_id'],
        'title': bill_dict['title'],
        'description': bill_dict['description'],
        'pending_committee_id': bill_dict['pending_committee_id'],
    }

    # Add committee info if exists
    if 'committee' in bill_dict and bill_dict['committee']:
        bill_info.update({
            'committee_id': bill_dict['committee']['committee_id'],
            'committee_chamber': bill_dict['committee']['chamber'],
            'committee_chamber_id': bill_dict['committee']['chamber_id'],
            'committee_name': bill_dict['committee']['name']
        })

    return {
        'bill_info': bill_info,
        'progress': progress,
        'referrals': referrals, 
        'calendar': calendar,
        'amendments': amendments,
        'supplements': supplements,
        'votes': votes,
        'texts': texts,
        'subjects': subjects,
        # 'authors': authors,
        'history': history
    }

def parse_person(person_json):
    person_dict = json.loads(person_json)

    person = person_dict['person']

    return {
        'people_id': person['people_id'],
        'person_hash': person['person_hash'], 
        'party': person['party'],
        'role': person['role'],
        'name': person['name'],
        'first_name': person['first_name'],
        'middle_name': person['middle_name'],
        'last_name': person['last_name'],
        'suffix': person['suffix'],
        'nickname': person['nickname'],
        'district': person['district'],
        'votesmart_id': person['votesmart_id'],
        'ballotpedia': person['ballotpedia']
    }


def parse_dataset(dataset):
    bills = []
    people = []
    bill_votes = []
    progress = []
    referrals = []
    calendar = []
    amendments = []
    supplements = []
    texts = []
    subjects = []
    history = []
    votes = []
    
    for file_path, content in dataset.items():
        if '/bill/' in file_path:
            bill_data = parse_bill(content)
            # Add bill info
            bills.append(bill_data['bill_info'])
            
            # Add all other bill data lists
            progress.extend(bill_data['progress'])
            referrals.extend(bill_data['referrals'])
            calendar.extend(bill_data['calendar'])
            amendments.extend(bill_data['amendments'])
            supplements.extend(bill_data['supplements'])
            bill_votes.extend(bill_data['votes'])
            texts.extend(bill_data['texts'])
            subjects.extend(bill_data['subjects'])
            history.extend(bill_data['history'])
            
        elif '/people/' in file_path:
            person_data = parse_person(content)
            people.append(person_data)

        elif '/vote/' in file_path:
            votes_data = parse_vote(content)
            votes.extend(votes_data)
    
    # Convert all lists to dataframes
    return {
        'bills': pd.DataFrame(bills),
        'people': pd.DataFrame(people),
        'bill_votes': pd.DataFrame(bill_votes),
        'votes': pd.DataFrame(votes),
        'progress': pd.DataFrame(progress),
        'referrals': pd.DataFrame(referrals),
        'calendar': pd.DataFrame(calendar), 
        'amendments': pd.DataFrame(amendments),
        'supplements': pd.DataFrame(supplements),
        'texts': pd.DataFrame(texts),
        'subjects': pd.DataFrame(subjects),
        'history': pd.DataFrame(history)
    }

def get_most_recent_dataset_hash(project_id, dataset_id, table_id='_legiscan_pulls'):
    most_recent_hash = query_bq(f"""
            select
            legiscan_hash
            from `{project_id}.{dataset_id}.{table_id}`
            order by TIMESTAMP(upload_time) desc
            limit 1
             """).iloc[0]['legiscan_hash']
    
    return most_recent_hash

def get_dataset(state, leg_id, most_recent_hash):
    dataset_list_url = f'https://api.legiscan.com/?key={LEGISCAN_API_KEY}&op=getDatasetList&state={state}'

    # Extract number by finding first digit and taking all digits
    leg_number = int(''.join(c for c in leg_id if c.isdigit()))

    # Everything after the number is the session type
    session_type = ''.join(c for c in leg_id if not c.isdigit())
    session_tag = 'Regular Session' if session_type == 'R' else 'Special Session'

    response = requests.get(dataset_list_url)
    result = response.json()
    datasets = result['datasetlist']
    curr_dataset = list(filter(lambda a: a['session_tag'] == session_tag and a['session_name'][:2] == str(leg_number), datasets))

    if len(curr_dataset) > 1:
        raise Exception(f'Found multiple datasets matching {leg_id}')
    elif len(curr_dataset) < 1:
        raise Exception(f'Failed to find a dataset matching {leg_id}')
    else:
        curr_dataset = curr_dataset[0]

    access_key = curr_dataset['access_key']
    session_id = curr_dataset['session_id']
    curr_dataset_hash = curr_dataset['dataset_hash']

    if curr_dataset_hash == most_recent_hash:
        print('Current hash matches most recent data pull. Not downloading data.')
        return None

    weekly_dataset_url = f'https://api.legiscan.com/?key={LEGISCAN_API_KEY}&op=getDataset&id={session_id}&access_key={access_key}'
    weekly_dataset_response = requests.get(weekly_dataset_url,timeout=30)
    weekly_dataset = weekly_dataset_response.json()

    # Check if response is valid
    if weekly_dataset['status'] != 'OK':
        raise Exception(f"API returned error status: {weekly_dataset['status']}")

    # Get the zip data and decode from base64
    zip_data = base64.b64decode(weekly_dataset['dataset']['zip'])

    # Create a ZipFile object from the bytes
    zip_buffer = io.BytesIO(zip_data)
    with zipfile.ZipFile(zip_buffer, 'r') as zip_ref:
        # Extract all files to memory
        dataset = {name: zip_ref.read(name) for name in zip_ref.namelist()}
    return dataset

def legiscan_to_bigquery(leg_session, project_id, dataset_id, env='dev'):
    most_recent_dataset_hash = get_most_recent_dataset_hash(project_id, dataset_id)

    raw_dataset = get_dataset('TX',leg_session, most_recent_hash=most_recent_dataset_hash)
    if raw_dataset == None: # if there's nothing new, do nothing
        return
    
    clean_dataset = parse_dataset(raw_dataset)

    legiscan_hash = raw_dataset['TX/2025-2026_89th_Legislature/hash.md5']
    legiscan_hash = legiscan_hash.decode("utf-8")

    legiscan_pull_info = {
        "upload_time": datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        'legiscan_hash': legiscan_hash
    }
    legiscan_pull_df = pd.DataFrame([legiscan_pull_info])
    dataframe_to_bigquery(legiscan_pull_df, project_id, dataset_id, '_legiscan_pulls', env, 'append')

    for table in clean_dataset.keys():
        table_df = clean_dataset[table]
        dataframe_to_bigquery(table_df, 'lgover', dataset_id, f'legiscan_{table}', env, 'drop')
        print(table_df)


################################################################################
#                 Pull Data
################################################################################

if __name__ == '__main__':
    with open(CONFIG_PATH, "r", encoding="utf-8") as f:
        config = yaml.safe_load(f)

    leg_session = config['info']['LegSess']
    
    # get_most_recent_dataset_hash('lgover','tx_leg_raw_bills')
    # get_dataset('TX','89R')
    legiscan_to_bigquery(leg_session, project_id=PROJECT_ID,dataset_id=DATASET_ID,env=ENV)