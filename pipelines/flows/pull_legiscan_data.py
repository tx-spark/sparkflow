import requests
import zipfile
import base64
import json
import io
import os
import pandas as pd

LEGISCAN_API_KEY = os.getenv('LEGISCAN_API_KEY')

################################################################################
#                 Parsing Functions
################################################################################'
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

def get_dataset(state, leg_id):
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

################################################################################
#                 Pull Data
################################################################################

raw_dataset = get_dataset('TX','89R')
clean_dataset = parse_dataset(raw_dataset)