import dlt
import pandas as pd
import re
from bs4 import BeautifulSoup
from utils import FtpConnection
import yaml
import functools

################################################################################
# CONFIGURATION
################################################################################

CONFIG_PATH = 'scraper_config.yaml'

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

################################################################################
# DATA SCRAPING FUNCTIONS
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
    for version in bill_versions:
        version_data = {
            'type': 'Bill',
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
    for version in analysis_versions:
        version_data = {
            'type': 'Analysis',
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
                'author_type': 'coauthor'
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
                'actionNumber': action['number'],
                'action_date': action['date'],
                'description': action['description'],
                'comment': action['comment'],
                'action_timestamp': action.get('timestamp', None)
            })
            
    return pd.DataFrame(actions_data, columns=['bill_id', 'leg_id', 'actionNumber', 'action_date', 
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
                'subcommittee_name': None,  # Not in sample data but included for schema
                'status': committee.get('status'),
                'subcommittee_status': None,  # Not in sample data but included for schema
                'aye_votes': votes.get('aye', 0),
                'nay_votes': votes.get('nay', 0), 
                'present_votes': votes.get('present_not_voting', 0),
                'absent_votes': votes.get('absent', 0)
            })
            
    return pd.DataFrame(committees_data, columns=['bill_id', 'leg_id', 'chamber', 'name',
                                                'subcommittee_name', 'status', 'subcommittee_status',
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
                'description': version.get('description'),
                'html_url': urls.get('web_html'),
                'pdf_url': urls.get('web_pdf'),
                'ftp_html_url': urls.get('ftp_html'),
                'ftp_pdf_url': urls.get('ftp_pdf')
            })
            
    return pd.DataFrame(versions_data, columns=['bill_id', 'leg_id', 'type', 'description',
                                              'html_url', 'pdf_url', 'ftp_html_url', 'ftp_pdf_url'])

def get_raw_bills_data(base_path, leg_session, ftp_connection):
    print("Getting raw bills data")
    bill_urls = get_bill_urls(base_path, leg_session, ftp_connection)
    raw_bills = []
    for url in bill_urls[:5]:
        bill_data = parse_bill_xml(ftp_connection, url)
        if bill_data:
            raw_bills.append(bill_data)
    return pd.DataFrame(raw_bills)




################################################################################
# DATA PIPELINE
################################################################################

@dlt.resource(write_disposition="replace")
def bills(raw_bills_df):
    bills_df = get_bills_data(raw_bills_df)
    print(bills_df)
    yield bills_df

@dlt.resource(write_disposition="replace")
def actions(raw_bills_df):
    actions_df = get_actions_data(raw_bills_df)
    print(actions_df)
    yield actions_df

@dlt.resource(write_disposition="replace")
def authors(raw_bills_df):
    authors_df = get_authors_data(raw_bills_df)
    print(authors_df)
    yield authors_df

@dlt.resource(write_disposition="replace")
def sponsors(raw_bills_df):
    sponsors_df = get_sponsors_data(raw_bills_df)
    print(sponsors_df)
    yield sponsors_df

@dlt.resource(write_disposition="replace")
def subjects(raw_bills_df):
    subjects_df = get_subjects_data(raw_bills_df)
    print(subjects_df)
    yield subjects_df

@dlt.resource(write_disposition="replace")
def committees(raw_bills_df):
    committees_df = get_committees_data(raw_bills_df)
    print(committees_df)
    yield committees_df 

@dlt.resource(write_disposition="replace")
def versions(raw_bills_df):
    versions_df = get_versions_data(raw_bills_df)
    print(versions_df)
    yield versions_df

@dlt.resource(write_disposition="replace")
def companions(raw_bills_df):
    companions_df = get_companions_data(raw_bills_df)
    print(companions_df)
    yield companions_df


################################################################################
# MAIN
################################################################################


if __name__ == "__main__":
    with open("scraper/scraper_config.yaml", "r", encoding="utf-8") as f:
        config = yaml.safe_load(f)

    pipeline = dlt.pipeline(
        destination="duckdb",
        dataset_name="raw_bills",
        pipeline_name="texas_bills"
    )

    conn = FtpConnection(config['sources']['ftp']['host'])

    base_path = config['sources']['ftp']['base_path']
    leg_session = config['info']['LegSess']
    raw_bills_df = get_raw_bills_data(base_path, leg_session, conn)
    
    pipeline.run([
        bills(raw_bills_df),
        authors(raw_bills_df),
        subjects(raw_bills_df),
        committees(raw_bills_df),
        versions(raw_bills_df),
        actions(raw_bills_df),
        companions(raw_bills_df)
    ])

