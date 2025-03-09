import re
import yaml
import requests
import functools

import dlt
import duckdb
import pandas as pd
from bs4 import BeautifulSoup
from utils import FtpConnection


################################################################################
# CONFIGURATION
################################################################################

CONFIG_PATH = 'config.yaml'
PIPELINE_NAME = "texas_bills"
OUT_DATASET_NAME = "raw_bills"

################################################################################
# HELPER FUNCTIONS
################################################################################

def clean_bill_id(bill_id):
    """
    Transform bill ID from format like '89(R) HB 1' into standardized format.
    
    Args:
        bill_id (str): Bill ID in format like '89(R) HB 1'
        
    Returns:
        tuple: (bill_number, session)
            bill_number (str): Bill number in format like 'HB1'
            session (str): Session in format like '89R'
    """
    # Split into session and bill parts
    session_part, bill_part = bill_id.split(') ')
    
    # Clean session (e.g. '89(R' -> '89R')
    session = session_part.replace('(', '')
    
    # Clean bill number (e.g. 'HB 1' -> 'HB1')
    bill_number = bill_part.replace(' ', '')
    
    return bill_number, session

def merge_with_current_data(new_df, curr_df):
    """
    Merge two dataframes and handle first_seen_at and last_seen_at timestamps.
    
    Args:
        new_df: DataFrame containing new records
        curr_df: DataFrame containing existing records
        match_columns: List of column names to match on
        
    Returns:
        DataFrame with first_seen_at and last_seen_at columns properly set
    """

    # Get current timestamp truncated to minute
    curr_time = pd.Timestamp.now().floor('min')
    

    if curr_df is None or len(curr_df) == 0:
        # Add timestamp columns to new_df
        new_df['first_seen_at'] = curr_time
        new_df['last_seen_at'] = curr_time
        return new_df
    
    match_columns = new_df.columns.tolist()

    # Add timestamp columns to new_df
    new_df['first_seen_at'] = curr_time
    new_df['last_seen_at'] = curr_time
        
    # Find matching and non-matching rows
    merged = new_df.merge(curr_df, on=match_columns, how='outer', indicator=True)
    
    # For new data only, keep current timestamps d
    new_only_mask = merged['_merge'] == 'left_only'
    merged.loc[new_only_mask, 'first_seen_at'] = curr_time
    merged.loc[new_only_mask, 'last_seen_at'] = curr_time

    # For matching rows, keep original first_seen_at and update last_seen_at
    matching_mask = merged['_merge'] == 'both'

    merged.loc[matching_mask, 'first_seen_at'] = merged.loc[matching_mask, 'first_seen_at_y']
    merged.loc[matching_mask, 'last_seen_at'] = curr_time
    
    # For rows only in curr_df, keep original timestamps
    curr_only_mask = merged['_merge'] == 'right_only'
    merged.loc[curr_only_mask, 'first_seen_at'] = merged.loc[curr_only_mask, 'first_seen_at_y']
    merged.loc[curr_only_mask, 'last_seen_at'] = merged.loc[curr_only_mask, 'last_seen_at_y']
    
    # Clean up merge artifacts
    merged = merged.drop(['first_seen_at_x', 'first_seen_at_y', 
                         'last_seen_at_x', 'last_seen_at_y', '_merge'], axis=1)
    print(merged)
    return merged

def get_current_table_data(duckdb_conn, table_name, dataset_name):
    curr_df = None
    if duckdb_conn.sql(f"SELECT count(*) FROM information_schema.tables WHERE table_name = '{table_name}' AND table_schema = '{dataset_name}'").fetchone()[0] > 0:
        curr_df = duckdb_conn.table(f"{dataset_name}.{table_name}").df()
    return curr_df

################################################################################
# HTML SCRAPING FUNCTIONS
################################################################################

def extract_committee_meetings_links(committees_page_url, leg_id):
    session = requests.Session()

    # Step 1: GET request to retrieve hidden form fields
    response = session.get(committees_page_url)
    soup = BeautifulSoup(response.text, 'html.parser')

    # Extract necessary hidden fields
    viewstate = soup.find("input", {"name": "__VIEWSTATE"})["value"]
    eventvalidation = soup.find("input", {"name": "__EVENTVALIDATION"})["value"]
    viewstategenerator = soup.find("input", {"name": "__VIEWSTATEGENERATOR"})["value"]

    # Step 2: POST request with hidden fields and selected legislature
    data = {
        "__VIEWSTATE": viewstate,
        "__EVENTVALIDATION": eventvalidation,
        "__VIEWSTATEGENERATOR": viewstategenerator,
        "__EVENTTARGET": "ddlLegislature",  # Mimic dropdown change
        "__EVENTARGUMENT": "",
        "ddlLegislature": leg_id
    }

    response = session.post(committees_page_url, data=data)

    soup = BeautifulSoup(response.text, 'html.parser')

    committees_list = soup.find_all('a', id='CmteList')

    committees = []

    for committee in committees_list:
        committees.append({
            'name': committee.text.strip(),
            'href': committee['href']
        })

    return committees

def get_committee_meetings_data(config):

    committees_list_url = config['sources']['static_html']['committees_list']
    committees_url = config['sources']['static_html']['committees']
    leg_id = config['info']['LegSess']

    for chamber in ['H', 'J', 'S']:
        committees_page_url = f"{committees_list_url}?Chamber={chamber}"
        committees = extract_committee_meetings_links(committees_page_url, leg_id)

        committee_meetings = []
        for committee in committees:
            committee_meetings.append({
                'name': committee['name'],
                'href': committees_url + committee['href'],
                'chamber': chamber,
                'leg_id': leg_id
            })
        return pd.DataFrame(committee_meetings)
    
################################################################################
# FTP SCRAPING FUNCTIONS
################################################################################

def get_bill_urls(base_path, leg_session, ftp_connection):
    """
    Get list of URLs for all bill XML files in house_bills and senate_bills directories.
    
    Args:
        base_path: String containing base path
        leg_session: String containing leg session
        ftp_connection: FtpConnection object
        
    Returns:
        List of URLs for all bill XML files
    """
    # Build base URL from config
    base_url = base_path.format(
        LegSess=leg_session
    )
    
    bill_urls = []
    
    # Process both house and senate bills
    for chamber in ['house_bills', 'senate_bills']:
        # Build URL for this chamber
        chamber_url = f"{base_url}/billhistory/{chamber}"
        
        # Get list of bill range folders (HB00001_HB00099 etc)
        range_folders = ftp_connection.ls(chamber_url)
        
        # Get bill XML files from each range folder
        for folder_url in range_folders:
            bill_xmls = ftp_connection.ls(folder_url)
            bill_urls.extend(bill_xmls)
            
    return bill_urls

def parse_bill_xml(ftp_connection, url):
    """
    Parse bill XML data from a URL into a standardized dictionary format.
    
    Args:
        conn: FtpConnection object
        url: URL to retrieve XML from
        
    Returns:
        Dictionary containing parsed bill data
    """
    # Get XML data from URL
    xml_str = ftp_connection.get_data(url=url)
    if not xml_str:
        print(f"Failed to retrieve data from {url}")
        return None
        
    soup = BeautifulSoup(xml_str, 'xml')
    bill_data = {}
    
    # Basic bill info
    bill_history = soup.find('billhistory')
    if bill_history:
        bill_data['bill_id'] = bill_history.get('bill')
        bill_data['last_update'] = bill_history.get('lastUpdate')
    
    # Last action
    last_action = soup.find('lastaction')
    if last_action:
        bill_data['last_action'] = last_action.text.strip()
    
    # Caption
    caption = soup.find('caption')
    if caption:
        bill_data['caption'] = caption.text.strip()
        bill_data['caption_version'] = caption.get('version')
    
    # Authors and co-authors
    bill_data['authors'] = [a.strip() for a in soup.find('authors').text.strip().split('|') if a.strip()]
    bill_data['coauthors'] = [a.strip() for a in soup.find('coauthors').text.strip().split('|') if a.strip()]
    bill_data['sponsors'] = [a.strip() for a in soup.find('sponsors').text.strip().split('|') if a.strip()]
    bill_data['cosponsors'] = [a.strip() for a in soup.find('cosponsors').text.strip().split('|') if a.strip()]
    
    # Subjects
    bill_data['subjects'] = [s.text.strip() for s in soup.find_all('subject')]

    # Parse companions
    companions = soup.find('companions')
    bill_data['companions'] = []
    if companions and companions.text.strip():
        for companion in companions.text.strip().split('\n'):
            companion = companion.strip()
            if companion:
                # Extract bill ID, author and relationship
                parts = companion.split(' by ')
                if len(parts) == 2:
                    bill_id = parts[0].strip()
                    author_and_type = parts[1].split(',')
                    author = author_and_type[0].strip()
                    relationship = author_and_type[1].strip() if len(author_and_type) > 1 else None
                    
                    companion_data = {
                        'bill_id': bill_id,
                        'author': author,
                        'relationship': relationship
                    }
                    bill_data['companions'].append(companion_data)
    
    # Committee info
    committees = soup.find('committees')
    bill_data['committees'] = []
    if committees:
        for comm in committees.find_all():
            committee_type = comm.name
            committee_data = {
                'type': committee_type,
                'name': comm.get('name'),
                'status': comm.get('status'),
                'votes': {
                    'aye': int(comm.get('ayeVotes', 0)),
                    'nay': int(comm.get('nayVotes', 0)), 
                    'absent': int(comm.get('absentVotes', 0)),
                    'present_not_voting': int(comm.get('presentNotVotingVotes', 0))
                }
            }
            bill_data['committees'].append(committee_data)
    # Actions
    bill_data['actions'] = []
    for action in soup.find_all('action'):
        action_data = {
            'number': action.find('actionNumber').text.strip(),
            'date': action.find('date').text.strip(),
            'description': action.find('description').text.strip(),
            'comment': action.find('comment').text.strip() if action.find('comment') else None,
            'timestamp': action.find('actionTimestamp').text.strip() if action.find('actionTimestamp') else None
        }
        bill_data['actions'].append(action_data)
    
    # Bill text URLs
    bill_data['versions'] = []
    
    # Get versions from bill text
    bill_versions = soup.find('billtext').find('docTypes').find('bill').find('versions').find_all('version')
    for idx, version in enumerate(bill_versions):
        version_data = {
            'type': 'Bill',
            'text_order': idx+1,  # Adding index +1 as text_order
            'description': version.find('versionDescription').text.strip(),
            'urls': {
                'web_html': version.find('WebHTMLURL').text.strip(),
                'web_pdf': version.find('WebPDFURL').text.strip(),
                'ftp_html': version.find('FTPHTMLURL').text.strip(),
                'ftp_pdf': version.find('FTPPDFURL').text.strip()
            }
        }
        bill_data['versions'].append(version_data)

        
    # Get versions from analysis
    analysis_versions = soup.find('billtext').find('docTypes').find('analysis').find('versions').find_all('version')
    for idx, version in enumerate(analysis_versions):
        version_data = {
            'type': 'Analysis',
            'text_order': idx+1,  # Adding index +1 as text_order
            'description': version.find('versionDescription').text.strip(),
            'urls': {
                'web_html': version.find('WebHTMLURL').text.strip(),
                'web_pdf': version.find('WebPDFURL').text.strip(),
                'ftp_html': version.find('FTPHTMLURL').text.strip(),
                'ftp_pdf': version.find('FTPPDFURL').text.strip()
            }
        }
        bill_data['versions'].append(version_data)
        
    # Get versions from fiscal note
    fiscal_versions = soup.find('billtext').find('docTypes').find('fiscalNote').find('versions').find_all('version')
    for version in fiscal_versions:
        version_data = {
            'type': 'Fiscal Note',
            'description': version.find('versionDescription').text.strip(),
            'text_order': idx+1,  # Adding index +1 as text_order
            'urls': {
                'web_html': version.find('WebHTMLURL').text.strip(),
                'web_pdf': version.find('WebPDFURL').text.strip(),
                'ftp_html': version.find('FTPHTMLURL').text.strip(),
                'ftp_pdf': version.find('FTPPDFURL').text.strip()
            }
        }
        bill_data['versions'].append(version_data)
        
    return bill_data

def get_bills_data(raw_bills_df):
    """
    Extract core bill data from raw bills dataframe into standardized format.
    
    Args:
        raw_bills_df (pd.DataFrame): DataFrame containing raw bill data
        
    Returns:
        pd.DataFrame: DataFrame with columns bill_id, leg_id, caption, last_action, caption_version
    """
    bills_data = []
    for _, row in raw_bills_df.iterrows():
        bill_id, leg_id = clean_bill_id(row['bill_id'])
        bills_data.append({
            'bill_id': bill_id,
            'leg_id': leg_id,
            'caption': row['caption'],
            'last_action_date': row['last_action'].split(' ')[0],
            'last_action_chamber': row['last_action'].split(' ')[1],
            'last_action': ' '.join(row['last_action'].split(' ')[2:]),
            'caption_version': row['caption_version']
        })
    return pd.DataFrame(bills_data, columns=['bill_id', 'leg_id', 'caption', 'last_action', 'last_action_date', 'last_action_chamber', 'caption_version'])

def get_authors_data(raw_bills_df):
    authors_data = []
    for _, row in raw_bills_df.iterrows():
        bill_id, leg_id = clean_bill_id(row['bill_id'])
        for author in row['authors']:
            authors_data.append({
                'bill_id': bill_id,
                'leg_id': leg_id,
                'author': author,
                'author_type': 'Author'
            })
        for coauthor in row['coauthors']:
            authors_data.append({
                'bill_id': bill_id,
                'leg_id': leg_id,
                'author': coauthor,
                'author_type': 'Coauthor'
            })
    return pd.DataFrame(authors_data, columns=['bill_id', 'leg_id', 'author', 'author_type'])


def get_sponsors_data(raw_bills_df):
    """
    Extract sponsors data from raw bills dataframe into standardized format.
    
    Args:
        raw_bills_df (pd.DataFrame): DataFrame containing raw bill data
        
    Returns:
        pd.DataFrame: DataFrame with columns bill_id, leg_id, sponsor, sponsor_type
    """
    sponsors_data = []
    for _, row in raw_bills_df.iterrows():
        bill_id, leg_id = clean_bill_id(row['bill_id'])
        for sponsor in row['sponsors']:
            sponsors_data.append({
                'bill_id': bill_id,
                'leg_id': leg_id, 
                'sponsor': sponsor,
                'sponsor_type': 'Sponsor'
            })
        for cosponsor in row['cosponsors']:
            sponsors_data.append({
                'bill_id': bill_id,
                'leg_id': leg_id,
                'sponsor': cosponsor,
                'sponsor_type': 'cosponsor'
            })
    return pd.DataFrame(sponsors_data, columns=['bill_id', 'leg_id', 'sponsor', 'sponsor_type'])

def get_subjects_data(raw_bills_df):
    """
    Extract subjects data from raw bills dataframe into standardized format.
    Splits subject strings into title and ID components.
    
    Args:
        raw_bills_df (pd.DataFrame): DataFrame containing raw bill data
        
    Returns:
        pd.DataFrame: DataFrame with columns bill_id, leg_id, subject_title, subject_id
    """
    subjects_data = []
    for _, row in raw_bills_df.iterrows():
        bill_id, leg_id = clean_bill_id(row['bill_id'])
        for subject in row['subjects']:
            # Split subject into title and ID
            # Example: "City Government--Employees/Officers (I0061)"
            title = subject.split(' (')[0]  # Everything before the ID
            subject_id = subject.split('(')[1].rstrip(')')  # Extract ID without parentheses
            
            subjects_data.append({
                'bill_id': bill_id,
                'leg_id': leg_id,
                'subject_title': title,
                'subject_id': subject_id
            })
    return pd.DataFrame(subjects_data, columns=['bill_id', 'leg_id', 'subject_title', 'subject_id'])

def get_companions_data(raw_bills_df):
    """
    Extract companion bill relationships from raw bills dataframe into standardized format.
    
    Args:
        raw_bills_df (pd.DataFrame): DataFrame containing raw bill data
        
    Returns:
        pd.DataFrame: DataFrame with columns bill_id, leg_id, companion_bill_id
    """
    companions_data = []
    for _, row in raw_bills_df.iterrows():
        bill_id, leg_id = clean_bill_id(row['bill_id'])
        
        # Skip if no companions
        if not row['companions']:
            continue
            
        for companion in row['companions']:
            companions_data.append({
                'bill_id': bill_id,
                'leg_id': leg_id,
                'companion_bill_id': companion['bill_id'],
                'relationship': companion['relationship']
            })
            
    return pd.DataFrame(companions_data, columns=['bill_id', 'leg_id', 'companion_bill_id', 'relationship'])

def get_actions_data(raw_bills_df):
    """
    Extract action data from raw bills dataframe into standardized format.
    
    Args:
        raw_bills_df (pd.DataFrame): DataFrame containing raw bill data
        
    Returns:
        pd.DataFrame: DataFrame with action information
    """
    actions_data = []
    for _, row in raw_bills_df.iterrows():
        bill_id, leg_id = clean_bill_id(row['bill_id'])
        
        # Skip if no actions
        if not row['actions']:
            continue
            
        for action in row['actions']:
            actions_data.append({
                'bill_id': bill_id,
                'leg_id': leg_id,
                'action_number': action['number'],
                'action_date': action['date'],
                'description': action['description'],
                'comment': action['comment'],
                'action_timestamp': action.get('timestamp', None)
            })
            
    return pd.DataFrame(actions_data, columns=['bill_id', 'leg_id', 'action_number', 'action_date', 
                                             'description', 'comment', 'action_timestamp'])


def get_committees_data(raw_bills_df):
    """
    Extract committee data from raw bills dataframe into standardized format.
    
    Args:
        raw_bills_df (pd.DataFrame): DataFrame containing raw bill data
        
    Returns:
        pd.DataFrame: DataFrame with committee information
    """
    committees_data = []
    
    for _, row in raw_bills_df.iterrows():
        bill_id, leg_id = clean_bill_id(row['bill_id'])
        
        # Skip if no committees
        if not row['committees']:
            continue
            
        for committee in row['committees']:
            votes = committee.get('votes', {})
            committees_data.append({
                'bill_id': bill_id,
                'leg_id': leg_id,
                'chamber': committee.get('type'),
                'name': committee.get('name'),
                #'subcommittee_name': None,  # Not in sample data but included for schema
                'status': committee.get('status'),
                #'subcommittee_status': None,  # Not in sample data but included for schema
                'aye_votes': votes.get('aye', 0),
                'nay_votes': votes.get('nay', 0), 
                'present_votes': votes.get('present_not_voting', 0),
                'absent_votes': votes.get('absent', 0)
            })
            
    return pd.DataFrame(committees_data, columns=['bill_id', 'leg_id', 'chamber', 'name',
                                                'status', #'subcommittee_name', 'subcommittee_status',
                                                'aye_votes', 'nay_votes', 'present_votes', 'absent_votes'])

def get_versions_data(raw_bills_df):
    versions_data = []
    
    for _, row in raw_bills_df.iterrows():
        bill_id, leg_id = clean_bill_id(row['bill_id'])
        
        # Skip if no versions
        if not row['versions']:
            continue
            
        for version in row['versions']:
            # Extract URLs, defaulting to None if not present
            urls = version.get('urls', {})
            versions_data.append({
                'bill_id': bill_id,
                'leg_id': leg_id,
                'type': version.get('type'),
                'text_order': version.get('text_order'),
                'description': version.get('description'),
                'html_url': urls.get('web_html'),
                'pdf_url': urls.get('web_pdf'),
                'ftp_html_url': urls.get('ftp_html'),
                'ftp_pdf_url': urls.get('ftp_pdf')
            })
            
    return pd.DataFrame(versions_data, columns=['bill_id', 'leg_id', 'type', 'text_order', 'description',
                                              'html_url', 'pdf_url', 'ftp_html_url', 'ftp_pdf_url'])

def get_links_data(raw_bills_df, config):
    links_data = []
    
    for _, row in raw_bills_df.iterrows():
        bill_id, leg_id = clean_bill_id(row['bill_id'])

        # Build links dictionary
        links = {
            'bill_id': bill_id,
            'leg_id': leg_id,
        }
        for link_type, base_url in config['sources']['html'].items():
            # Format URL with session and bill info
            formatted_url = f"{base_url}?LegSess={leg_id}&Bill={bill_id}"
            links[link_type] = formatted_url
            
        links_data.append(links)

    return pd.DataFrame(links_data, columns=['bill_id', 'leg_id'] + list(config['sources']['html'].keys()))

def get_raw_bills_data(base_path, leg_session, ftp_connection):
    print("Getting raw bills data")
    bill_urls = get_bill_urls(base_path, leg_session, ftp_connection)
    raw_bills = []
    for url in bill_urls:
        print(url)
        bill_data = parse_bill_xml(ftp_connection, url)
        if bill_data:
            raw_bills.append(bill_data)
    return pd.DataFrame(raw_bills)



################################################################################
# DATA PIPELINE
################################################################################
@dlt.resource(write_disposition="replace")
def bills(raw_bills_df, curr_bills_df=None):
    bills_df = get_bills_data(raw_bills_df)
    
    result_df = merge_with_current_data(bills_df, curr_bills_df)
    print(result_df)
    yield result_df

@dlt.resource(write_disposition="replace")
def actions(raw_bills_df, curr_actions_df=None):
    actions_df = get_actions_data(raw_bills_df)
    
    result_df = merge_with_current_data(actions_df, curr_actions_df)
    print(result_df)
    yield result_df

@dlt.resource(write_disposition="replace")
def authors(raw_bills_df, curr_authors_df=None):
    authors_df = get_authors_data(raw_bills_df)
    
    result_df = merge_with_current_data(authors_df, curr_authors_df)
    print(result_df)
    yield result_df

@dlt.resource(write_disposition="replace")
def sponsors(raw_bills_df, curr_sponsors_df=None):
    sponsors_df = get_sponsors_data(raw_bills_df)
    
    result_df = merge_with_current_data(sponsors_df, curr_sponsors_df)
    print(result_df)
    yield result_df

@dlt.resource(write_disposition="replace")
def subjects(raw_bills_df, curr_subjects_df=None):
    subjects_df = get_subjects_data(raw_bills_df)
    
    result_df = merge_with_current_data(subjects_df, curr_subjects_df)
    print(result_df)
    yield result_df

@dlt.resource(write_disposition="replace")
def committees(raw_bills_df, curr_committees_df=None):
    committees_df = get_committees_data(raw_bills_df)
    
    result_df = merge_with_current_data(committees_df, curr_committees_df)
    print(result_df)
    yield result_df

@dlt.resource(write_disposition="replace")
def versions(raw_bills_df, curr_versions_df=None):
    versions_df = get_versions_data(raw_bills_df)
    
    result_df = merge_with_current_data(versions_df, curr_versions_df)
    print(result_df)
    yield result_df

@dlt.resource(write_disposition="replace")
def companions(raw_bills_df, curr_companions_df=None):
    companions_df = get_companions_data(raw_bills_df)
    
    result_df = merge_with_current_data(companions_df, curr_companions_df)
    print(result_df)
    yield result_df

@dlt.resource(write_disposition="replace")
def links(raw_bills_df, config, curr_links_df=None):
    links_df = get_links_data(raw_bills_df,config)
    
    result_df = merge_with_current_data(links_df, curr_links_df)
    print(result_df)
    yield result_df

@dlt.resource(write_disposition="replace")
def committee_meetings(config, curr_committee_meetings_df=None):

    committee_meetings_df = get_committee_meetings_data(config)
    
    result_df = merge_with_current_data(committee_meetings_df, curr_committee_meetings_df)
    print(result_df)
    yield result_df


################################################################################
# MAIN
################################################################################

if __name__ == "__main__":
    with open(CONFIG_PATH, "r", encoding="utf-8") as f:
        config = yaml.safe_load(f)

    pipeline = dlt.pipeline(
        destination="duckdb",
        dataset_name=OUT_DATASET_NAME,
        pipeline_name=PIPELINE_NAME
    )

    conn = FtpConnection(config['sources']['ftp']['host'])

    base_path = config['sources']['ftp']['base_path']
    leg_session = config['info']['LegSess']
    raw_bills_df = get_raw_bills_data(base_path, leg_session, conn)
    
    duckdb_conn = duckdb.connect(f"{PIPELINE_NAME}.duckdb")

    curr_bills_df = get_current_table_data(duckdb_conn, 'bills', OUT_DATASET_NAME)
    curr_authors_df = get_current_table_data(duckdb_conn, 'authors', OUT_DATASET_NAME) 
    curr_subjects_df = get_current_table_data(duckdb_conn, 'subjects', OUT_DATASET_NAME)
    curr_committees_df = get_current_table_data(duckdb_conn, 'committees', OUT_DATASET_NAME)
    curr_versions_df = get_current_table_data(duckdb_conn, 'versions', OUT_DATASET_NAME)
    curr_actions_df = get_current_table_data(duckdb_conn, 'actions', OUT_DATASET_NAME)
    curr_companions_df = get_current_table_data(duckdb_conn, 'companions', OUT_DATASET_NAME)
    curr_links_df = get_current_table_data(duckdb_conn, 'links', OUT_DATASET_NAME)
    curr_committee_meetings_df = get_current_table_data(duckdb_conn, 'committee_meetings', OUT_DATASET_NAME)
    
    pipeline.run([
        bills(raw_bills_df, curr_bills_df),
        authors(raw_bills_df, curr_authors_df),
        subjects(raw_bills_df, curr_subjects_df),
        committees(raw_bills_df, curr_committees_df),
        versions(raw_bills_df, curr_versions_df),
        actions(raw_bills_df, curr_actions_df),
        companions(raw_bills_df, curr_companions_df),
        links(raw_bills_df, config, curr_links_df),
        committee_meetings(config, curr_committee_meetings_df)
    ])

