import re
import requests
import datetime

import feedparser
import pandas as pd
from bs4 import BeautifulSoup
import json
import logging

from prefect import task
from prefect.cache_policies import NO_CACHE

logger = logging.getLogger(__name__)

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

    if len(new_df) == 0:
        return curr_df

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


    # Get the full set of columns
    all_columns = set(new_df.columns) | set(curr_df.columns)

    # Add missing columns with NaN
    for df in [new_df, curr_df]:
        for col in all_columns:
            if col not in df.columns:
                df[col] = pd.NA

    # Replace null values with a distinct placeholder before merging
    # This prevents merge issues with null values
    placeholder = '___NULL___'
    new_df = new_df.fillna(placeholder)
    curr_df = curr_df.fillna(placeholder)

    # Find matching and non-matching rows
    merged = new_df.merge(curr_df, on=match_columns, how='outer', indicator=True)

    # For new data only, keep current timestamps
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

    print(merged)

    # Clean up merge artifacts
    merged = merged.drop(['first_seen_at_x', 'first_seen_at_y', 
                         'last_seen_at_x', 'last_seen_at_y', '_merge'], axis=1)

    # Drop any remaining columns ending with _x or _y
    x_y_cols = [col for col in merged.columns if col.endswith('_x') or col.endswith('_y')]
    merged = merged.drop(columns=x_y_cols)

    # Replace the placeholder back with null values
    merged = merged.replace(placeholder, pd.NA)
    
    return merged

def get_current_table_data(duckdb_conn, table_name, dataset_name):
    curr_df = None
    if duckdb_conn.sql(f"SELECT count(*) FROM information_schema.tables WHERE table_name = '{table_name}' AND table_schema = '{dataset_name}'").fetchone()[0] > 0:
        curr_df = duckdb_conn.table(f"{dataset_name}.{table_name}").df()
    return curr_df



################################################################################
# RSS SCRAPING FUNCTIONS
################################################################################

def get_rss_data(config):
    timeframes = config['sources']['rss']
    current_date = datetime.datetime.now().strftime('%Y-%m-%d %H:%M')

    for timeframe in timeframes.keys():
        entries = []
        for rss_label, rss_url in zip(timeframes[timeframe].keys(), timeframes[timeframe].values()):
            feed = feedparser.parse(rss_url)
            for entry in feed.entries:
                entry_dict = {
                    'timeframe': timeframe,
                    'rss_label': rss_label,
                    'date': current_date
                }
                for key, value in entry.items():
                    entry_dict[key] = value
                entries.append(entry_dict)
    return pd.DataFrame(entries)

@task(retries=3, retry_delay_seconds=10, log_prints=False, cache_policy=NO_CACHE)
def get_upcoming_from_rss(upcoming_rss_urls:dict):
    """
    Gets RSS feed data from the configured URLs and returns a DataFrame.
    
    Args:
        config (dict): Configuration dictionary containing labelsRSS feed URLs
        
    Returns:
        pandas.DataFrame: DataFrame containing RSS feed entries
    """
    current_date = datetime.datetime.now().strftime('%Y-%m-%d %H:%M')
    entries = []
    
    for rss_label, rss_url in zip(upcoming_rss_urls.keys(), upcoming_rss_urls.values()):
        feed = feedparser.parse(rss_url)
        for entry in feed.entries:
            entry_dict = {
                'rss_label': rss_label,
                'date': current_date,
                'description': entry.get('description', None),
            }
            for key, value in entry.items():
                if type(value) == feedparser.util.FeedParserDict:
                    for k,v in value.items():
                        entry_dict[f'{key}_{k}'] = v
                else:
                    entry_dict[key] = value
            entries.append(entry_dict)
            
    return pd.DataFrame(entries)

@task(retries=3, retry_delay_seconds=10, log_prints=False, cache_policy=NO_CACHE)
def get_rss_committee_meetings(rss_config):
    """
    Gets all committee meetings from RSS feed and returns a standardized DataFrame.
    
    Args:
        rss_config (dict): Dictionary containing RSS feed URLs for upcoming meetings
        
    Returns:
        DataFrame with standardized meeting details including bills
    """
    # Get upcoming meetings from RSS
    upcoming_meetings = get_upcoming_from_rss(rss_config)
    
    # Filter for committee meetings and reset index
    meetings_mask = upcoming_meetings['rss_label'].isin(['meetings_senate', 'meetings_house'])
    filtered_meetings = upcoming_meetings[meetings_mask].reset_index(drop=True)
    meetings_links = filtered_meetings['link'].tolist()
    meetings_labels = filtered_meetings['rss_label'].tolist()
    
    # Get detailed meeting data
    meetings_df = pd.DataFrame(map(read_committee_meeting, meetings_links))
    meetings_df = meetings_df[meetings_df['bills'].notna()]
    
    # Convert bills columns from string to list if needed
    if isinstance(meetings_df['bills'].iloc[0], str):
        meetings_df['bills'] = meetings_df['bills'].apply(eval)
    if isinstance(meetings_df['deleted_bills'].iloc[0], str):
        meetings_df['deleted_bills'] = meetings_df['deleted_bills'].apply(eval)
    if isinstance(meetings_df['added_bills'].iloc[0], str):
        meetings_df['added_bills'] = meetings_df['added_bills'].apply(eval)

    # Extract date and time from filtered_meetings
    meetings_df['date'] = filtered_meetings['title'].str.extract(r'-\s*(\d{1,2}/\d{1,2}/\d{4})')
    meetings_df['time'] = filtered_meetings['description'].str.extract(r'Time:\s*(\d{1,2}:\d{2}\s*[AP]M)')

    # Create standardized output
    result = []
    for i, meeting in meetings_df.iterrows():
        meeting_info = {
            'committee': meeting['committee'],
            'chamber': 'Senate' if meetings_labels[i] == 'meetings_senate' else 'House',
            'date': meeting['date'],
            'time': meeting['time'],
            'location': meeting['place'],
            'chair': meeting['chair'],
            'meeting_url': meeting['meeting_url'],
            'bills': []
        }

        # Add regular bills

        for bill in meeting['bills']:
            bill_info = {
                'bill_id': bill['bill_id'],
                'link': bill['bill_link'],
                'author': bill.get('author', ''),
                'description': bill.get('description', ''),
                'status': 'scheduled'
            }
            meeting_info['bills'].append(bill_info)

        # Add deleted bills
        for bill in meeting['deleted_bills']:
            bill_info = {
                'bill_id': bill['bill_id'],
                'link': bill['bill_link'],
                'status': 'deleted'
            }
            meeting_info['bills'].append(bill_info)

        # Add added bills  
        for bill in meeting['added_bills']:
            bill_info = {
                'bill_id': bill['bill_id'],
                'link': bill['bill_link'],
                'status': 'added'
            }
            meeting_info['bills'].append(bill_info)

        result.append(meeting_info)

    return pd.DataFrame(result)

################################################################################
# HTML SCRAPING FUNCTIONS
################################################################################

def get_committee_meetings(committee_meetings_url):
    """
    Scrapes committee meeting details from a committee's meetings page
    
    Args:
        committee_meetings_url (str): URL of the committee meetings page
        
    Returns:
        dict: Dictionary containing meeting details 
    """
    response = requests.get(committee_meetings_url)
    soup = BeautifulSoup(response.text, 'html.parser')

    # Find the meetings table
    meetings_table = soup.find('table', id='tblMeetings')
    if not meetings_table:
        return []
    
    # Extract meeting details
    meeting_rows = meetings_table.find_all('tr')
    meetings = []
    
    for row in meeting_rows:
        cells = row.find_all('td')
        if len(cells) >= 7 and type(cells[1].text) == str and cells[1].text.lower() != 'time':
            meeting = {
                'date': cells[0].get_text(strip=True),
                'time': cells[1].get_text(strip=True), 
                'subcommittee': cells[2].get_text(strip=True),
                'hearing_notice_html': 'https://capitol.texas.gov/' + cells[3].find('a', href=lambda x: x and 'html' in x)['href'] if cells[3].find('a', href=lambda x: x and 'html' in x) else None,
                'hearing_notice_pdf': 'https://capitol.texas.gov/' + cells[3].find('a', href=lambda x: x and 'pdf' in x.lower())['href'] if cells[3].find('a', href=lambda x: x and 'pdf' in x.lower()) else None,
                'minutes_html': 'https://capitol.texas.gov/' + cells[4].find('a', href=lambda x: x and 'html' in x)['href'] if cells[4].find('a', href=lambda x: x and 'html' in x) else None,
                'minutes_pdf': 'https://capitol.texas.gov/' + cells[4].find('a', href=lambda x: x and 'pdf' in x.lower())['href'] if cells[4].find('a', href=lambda x: x and 'pdf' in x.lower()) else None,
                'witness_list_html': 'https://capitol.texas.gov/' + cells[5].find('a', href=lambda x: x and 'html' in x)['href'] if cells[5].find('a', href=lambda x: x and 'html' in x) else None,
                'witness_list_pdf': 'https://capitol.texas.gov/' + cells[5].find('a', href=lambda x: x and 'pdf' in x.lower())['href'] if cells[5].find('a', href=lambda x: x and 'pdf' in x.lower()) else None,
                'comments': cells[6].get_text(strip=True)
            }

            meetings.append(meeting)
            
    return meetings


def get_html_committee_meetings(config):
    """
    Gets all committee meetings from committee pages and returns a standardized DataFrame.
    
    Args:
        config (dict): Dictionary containing configuration including URLs and session info
        
    Returns:
        DataFrame with standardized meeting details including bills
    """
    # Get committee links with chamber info
    committee_links = get_committee_meetings_links(config)
    
    # Get meetings for each committee
    meetings = []
    for _, committee in committee_links.iterrows():
        committee_meetings = get_committee_meetings(committee['link'])
        # Add chamber info to each meeting
        for meeting in committee_meetings:
            meeting['chamber'] = 'Joint' if committee['chamber'] == 'J' else (
                'Senate' if committee['chamber'] == 'S' else 'House'
            )
            meeting['committee_link'] = committee['link']   
            meeting['leg_id'] = committee['leg_id']
        meetings.extend(committee_meetings)
    committee_meetings_df = pd.DataFrame(meetings)
    
    # Get detailed bill info for each meeting
    committee_bills = []
    for committee in committee_meetings_df['hearing_notice_html']:
        committee_bills.append(read_committee_meeting(committee))

    committee_bills_df = pd.DataFrame(committee_bills)
    committee_bills_df = committee_bills_df[committee_bills_df['bills'].notna()]
    
    # Create standardized output
    result = []
    for i, meeting in committee_bills_df.iterrows():
        # Extract time from original meetings dataframe
        meeting_time = committee_meetings_df.iloc[i]['time'].split(' ')[0] + ' ' + committee_meetings_df.iloc[i]['time'].split(' ')[1]
        
        meeting_info = {
            'committee': meeting['committee'],
            'chamber': committee_meetings_df.iloc[i]['chamber'],
            'committee_meetings_link': committee_meetings_df.iloc[i]['committee_link'],
            'leg_id': committee_meetings_df.iloc[i]['leg_id'],
            'date': committee_meetings_df.iloc[i]['date'],
            'time': meeting_time,
            'location': meeting['place'],
            'chair': meeting['chair'],
            'meeting_url': meeting['meeting_url'],
            'bills': [],
            'subcommittee': committee_meetings_df.iloc[i]['subcommittee'],
            'hearing_notice_html': committee_meetings_df.iloc[i]['hearing_notice_html'],
            'hearing_notice_pdf': committee_meetings_df.iloc[i]['hearing_notice_pdf'],
            'minutes_html': committee_meetings_df.iloc[i]['minutes_html'],
            'minutes_pdf' : committee_meetings_df.iloc[i]['minutes_pdf'],
            'witness_list_html': committee_meetings_df.iloc[i]['witness_list_html'],
            'witness_list_pdf': committee_meetings_df.iloc[i]['witness_list_pdf'],
            'comments' : committee_meetings_df.iloc[i]['comments']
        }
        
        if type(meeting['bills']) != list or len(meeting['bills']) < 1:
            result.append(meeting_info)

        # Add regular bills
        for bill in meeting['bills']:
            bill_info = {
                'bill_id': bill['bill_id'],
                'link': bill['bill_link'],
                'author': bill.get('author', ''),
                'description': bill.get('description', ''),
                'status': 'scheduled'
            }
            meeting_info['bills'].append(bill_info)

        # Add deleted bills
        for bill in meeting['deleted_bills']:
            bill_info = {
                'bill_id': bill['bill_id'],
                'link': bill['bill_link'],
                'status': 'deleted'
            }
            meeting_info['bills'].append(bill_info)

        # Add added bills  
        for bill in meeting['added_bills']:
            bill_info = {
                'bill_id': bill['bill_id'],
                'link': bill['bill_link'],
                'status': 'added'
            }
            meeting_info['bills'].append(bill_info)

        result.append(meeting_info)

    return pd.DataFrame(result)

@task(retries=3, retry_delay_seconds=10, log_prints=False, cache_policy=NO_CACHE)
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

@task(retries=0, retry_delay_seconds=10, log_prints=False, cache_policy=NO_CACHE)
def get_committee_meetings_links(config, max_errors=5):

    committees_list_url = config['sources']['static_html']['committees_list']
    committees_url = config['sources']['static_html']['committees']
    leg_id = ''.join(filter(lambda i: i.isdigit(), config['info']['LegSess']))

    committee_meetings = []
    error_count = 0
    for chamber in ['H', 'J', 'S']:
        try:
            committees_page_url = f"{committees_list_url}?Chamber={chamber}"
            committees = extract_committee_meetings_links(committees_page_url, leg_id)
            
            for committee in committees:
                committee_meetings.append({
                    'name': committee['name'],
                    'link': committees_url + committee['href'],
                    'chamber': chamber,
                    'leg_id': config['info']['LegSess']
                    })
        except Exception as e:
            logger.debug(f"Failed to get committee meetings links for {chamber}: {e}")
            error_count += 1
        if error_count > max_errors:
            logger.error(f"{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')} -- Failed to get committee meetings links for {error_count} chambers")
            raise Exception(f"Failed to get committee meetings links for {error_count} chambers")
    return pd.DataFrame(committee_meetings)

@task(retries=3, retry_delay_seconds=10, log_prints=False)
def get_house_hearing_videos_data(house_videos_url, leg_id):
    leg_num = leg_id[:-1]  # Get all but last character
    leg_letter = leg_id[-1]
    house_videos_url = house_videos_url.replace("{leg_id}", f"{leg_num}/{leg_letter}") 
    headers = {}#'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:123.0) Gecko/20100101 Firefox/123.0'}
    response = requests.get(house_videos_url, headers=headers)

    videos_list = json.loads(response.text)
    videos_df = pd.DataFrame(videos_list)
    return videos_df

@task(retries=3, retry_delay_seconds=10, log_prints=False, cache_policy=NO_CACHE)
def get_senate_hearing_videos_data(senate_videos_url, leg_id):
    leg_num = leg_id[:-1]  # Get all but last character
    senate_videos_url = senate_videos_url.replace("{leg_id}", f"{leg_num}") 
    response = requests.get(senate_videos_url)
    soup = BeautifulSoup(response.text, 'html.parser')
    videos_table = soup.find('table')
    videos_rows = videos_table.find_all('tr')
    videos_list = []
    for row in videos_rows:
        # Skip header row
        if row.find('th'):
            continue
            
        cells = row.find_all('td')
        if cells:
            date = cells[0].text
            program = cells[1].text
            
            # Get video link if it exists
            video_link = None
            play_cell = cells[2].find('a')
            if play_cell:
                video_link = play_cell['href']
                
            video_data = {
                'date': date,
                'program': program,
                'video_link': 'https://senate.texas.gov/' + video_link
            }
            
            # Add to list
            videos_list.append(video_data)
    return pd.DataFrame(videos_list)


def get_committee_hearing_videos_data(config):
    house_videos = config['sources']['videos']['house']
    senate_videos = config['sources']['videos']['senate']

    house_videos_df = get_house_hearing_videos_data(house_videos, config['info']['LegSess']) ## has lots of columns: id	date	time	name	type	status_id	status	liveUrl	channel	noTime	textTime	room	notes	sponsors	publicNote	url	EventUrl
    senate_videos_df = get_senate_hearing_videos_data(senate_videos, config['info']['LegSess'])

    house_videos_df = house_videos_df[['date', 'time', 'name', 'EventUrl']]

    house_videos_df.rename(columns={'EventUrl': 'video_link', 'name': 'program'}, inplace=True)

    house_videos_df['chamber'] = 'House'
    senate_videos_df['chamber'] = 'Senate'
    house_videos_df['leg_id'] = config['info']['LegSess']
    senate_videos_df['leg_id'] = config['info']['LegSess']

    # date, program, video_link

    return pd.concat([house_videos_df, senate_videos_df])

def get_indv_bill_stages(bill_stages_url, bill_id, leg_id):
    """
    TO DO: WRITE DESCRIPTION
    """
    bill_text_url = f'{bill_stages_url}?LegSess={leg_id}&Bill={bill_id}'
    site_html = requests.get(bill_text_url,timeout=30).text
    soup = BeautifulSoup(site_html, 'html.parser')

    stages_div = soup.find('div', id='usrBillStages_pnlBillStages')
    stages_div = soup.find('div', class_='bill-status')
    # Initialize empty list to store stages
    stages = []

    stage = {'bill_id': bill_id, 'leg_id': leg_id}
    if stages_div:
        # Find all stage boxes and continuations
        stage_boxes = stages_div.find_all('div', recursive=False)

        # Process each stage
        for box in stage_boxes:
            text = box.text.strip()
            div_class = box.get('class')[0]
            
            # Get image filename from continuation div
            img = box.find('img')
            img_src = img['src'].split('/')[-1] if img else None

            if len(text.split('\n')) > 2:
                stage.update({
                    'stage': text.split('\n')[0],
                    'stage_title': text.split('\n')[1],
                    'stage_date': ''.join(text.split('\n')[2:]),
                    'div_class': div_class.split('-')[-1]
                })
            elif len(text.split('\n')) == 2:
                stage.update({
                    'stage': text.split('\n')[0],
                    'stage_title': text.split('\n')[1],
                    'div_class': div_class.split('-')[-1],
                    'stage_date': None
                })
            elif img_src is not None:
                stage.update({
                    'after_status': img_src.split('.')[0]
                })
                if 'Not reached' not in stage['stage_title']:
                    stages.append(stage)
                stage = {'bill_id': bill_id, 'leg_id': leg_id}
            else:
                continue
        if 'stage_title' in stage.keys() and 'Not reached' not in stage['stage_title']:
            stages.append(stage)
    else:
        # If no stages div found, return empty list
        stages = []

    #########################################################

    stage_labels = soup.find_all('div', class_='stageLabel')
    stage_texts = soup.find_all('div', class_='stageText')


    stage_details = []
    for label, text in zip(stage_labels, stage_texts):
        stage_detail = {
            'stage': label.text.strip(),
            'stage_text': text.text.strip()
        }
        stage_details.append(stage_detail)

    for i in range(len(stages)):
        stages[i].update(stage_details[i]) 

    return stages

def get_bill_stages(bill_stages_url, raw_bills_df, max_errors=5):
    bill_stages = []
    error_count = 0
    for _, row in raw_bills_df.iterrows():
        bill_id, leg_id = clean_bill_id(row['bill_id'])
        print(bill_id)
        try:
            bill_stages.extend(get_indv_bill_stages(bill_stages_url, bill_id, leg_id))
        except Exception as e:
            print(f"Error getting bill stages for {bill_id}: {e}")
            error_count += 1
    if error_count > max_errors:
        logger.error(f"{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')} -- Failed to get bill stages for {error_count} bills")
        raise Exception(f"Failed to get bill stages for {error_count} bills")
    return pd.DataFrame(bill_stages)

    
################################################################################
# FTP SCRAPING FUNCTIONS
################################################################################

@task(retries=3, retry_delay_seconds=10, log_prints=False, cache_policy=NO_CACHE)
def get_bill_urls(base_path, leg_session, ftp_connection, max_errors=5):
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
    error_count = 0
    for chamber in ['house_bills', 'senate_bills', 
                    'house_joint_resolutions', 'senate_joint_resolutions', 
                    'house_concurrent_resolutions', 'senate_concurrent_resolutions', 
                    'house_resolutions', 'senate_resolutions']: 
        # Build URL for this chamber
        chamber_url = f"{base_url}/billhistory/{chamber}"
        
        # Get list of bill range folders (HB00001_HB00099 etc)
        try:
            range_folders = ftp_connection.ls(chamber_url)
        except Exception as e:
            logger.debug(f"Error getting folders for {chamber_url}: {e}")
            continue
        
        # Get bill XML files from each range folder
        for folder_url in range_folders:
            try:
                bill_xmls = ftp_connection.ls(folder_url)
                bill_urls.extend(bill_xmls)
            except Exception as e:
                logger.debug(f"Error getting bill XML files list for {folder_url}: {e}")
                error_count += 1
                continue

        if error_count > max_errors:
            logger.error(f"{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')} -- Failed to get bill URLs for {error_count} chambers")
            raise Exception(f"Failed to get bill URLs for {error_count} chambers. Stopping process.")
        
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
    try:
        xml_str = ftp_connection.get_data(url=url)
    except Exception as e:
        logger.debug(f"Failed to retrieve data from {url}: {e}")
        return None
        
    if not xml_str:
        logger.debug(f"Recieved no data from {url}")
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

def read_committee_meeting(meeting_url):
    """
    Reads a committee meeting URL and returns a dictionary of the meeting data.
    
    Args:
        meeting_url (str): URL of the committee meeting page
        
    Returns:
        dict: Dictionary containing committee info and list of bills to be discussed
    """
    response = requests.get(meeting_url)
    soup = BeautifulSoup(response.text, 'html.parser')
    
    # Find the first table with class MsoNormalTable
    tables = soup.find_all('table', class_='MsoNormalTable')
    if len(tables) < 2:
        return {}
    
    table = tables[1]

    # Extract data using string parsing
    meeting_data = {
        'bills': []
    }

    # Get committee info
    paragraphs = table.find_all('p', class_='MsoNormal')
    for p in paragraphs:
        text = p.get_text(' ', strip=True)
        text = re.sub(r'\s+', ' ', text)
        
        if text.startswith('COMMITTEE:'):
            meeting_data['committee'] = text.replace('COMMITTEE:', '').strip()
        elif text.startswith('TIME & DATE:'):
            meeting_data['time_date'] = text.replace('TIME & DATE:', '').strip()
        elif text.startswith('PLACE:'):
            place_chair = text.replace('PLACE:', '').strip()
            if 'CHAIR:' in place_chair:
                place, chair = place_chair.split('CHAIR:')
                meeting_data['place'] = place.strip()
                meeting_data['chair'] = chair.strip()
            else:
                meeting_data['place'] = place_chair
    
    # Find all bill rows
    bill_rows = soup.find_all('tr', style='page-break-inside:avoid')
    
    for row in bill_rows:
        bill_p = row.find('p', class_='MsoNormal')
        if not bill_p:
            continue
            
        # Get bill link and ID
        bill_link = bill_p.find('a')
        if not bill_link:
            continue
            
        bill_href = 'https://capitol.texas.gov' + bill_link['href']
        bill_id = bill_link.get_text(strip=True)
        
        # Get author text after </a> tag but before first <br> tag
        author = ''
        for content in bill_link.next_siblings:
            if content.name == 'br':
                break
            author += str(content)
        author = re.sub(r'[\r\n]', '', author.strip())
        # Get description text between first and second <br> tags
        description = ''
        br_tags = bill_p.find_all('br')
        if len(br_tags) >= 2:
            description_content = br_tags[0].next_sibling
            if description_content and description_content.string:
                description = re.sub(r'[\r\n]', '', description_content.string.strip())

        
        meeting_data['bills'].append({
            'bill_id': bill_id,
            'bill_link': bill_href,
            'author': author,
            'description': description,
        })

    # Check for deleted and added bills sections
    deleted_bills = []
    added_bills = []
    
    # Find headers for deleted and added bills
    deleted_header = soup.find('p', class_='MsoNormal', string=lambda x: x and 'Bills deleted' in x)
    added_header = soup.find('p', class_='MsoNormal', string=lambda x: x and 'Bills added' in x)
    
    # Get deleted bills if they exist
    if deleted_header:
        # Get all bill links between deleted header and added header (if it exists)
        next_header = added_header if added_header else None
        current = deleted_header.find_next('a')
        while current and current != next_header:
            deleted_bill_id = current.get_text(strip=True)
            deleted_bill_href = 'https://capitol.texas.gov' + current['href']
            deleted_bills.append({
                'bill_id': deleted_bill_id,
                'bill_link': deleted_bill_href
            })
            current = current.find_next('a')
    
    # Get added bills if they exist        
    if added_header:
        # Get all bill links after added header
        current = added_header.find_next('a')
        while current:
            added_bill_id = current.get_text(strip=True)
            added_bill_href = 'https://capitol.texas.gov' + current['href']
            added_bills.append({
                'bill_id': added_bill_id,
                'bill_link': added_bill_href
            })
            current = current.find_next('a')
            
    meeting_data['deleted_bills'] = deleted_bills
    meeting_data['added_bills'] = added_bills

    meeting_data['meeting_url'] = meeting_url
    return meeting_data

########################################################
# Standardized Data Extraction Functions
########################################################

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
        try:
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
        except Exception as e:
            logger.debug(f"Failed to get clean bill data for {row['bill_id']}: {e}")
            continue

    return pd.DataFrame(bills_data, columns=['bill_id', 'leg_id', 'caption', 'last_action', 'last_action_date', 'last_action_chamber', 'caption_version'])

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
        try:
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
        except Exception as e:
            logger.debug(f"Failed to get clean action data for {row['bill_id']}: {e}")
            continue
            
    return pd.DataFrame(actions_data, columns=['bill_id', 'leg_id', 'action_number', 'action_date', 
                                             'description', 'comment', 'action_timestamp'])


def get_authors_data(raw_bills_df):
    authors_data = []
    for _, row in raw_bills_df.iterrows():
        try:
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
        except Exception as e:
            logger.debug(f"Failed to get clean author data for {row['bill_id']}: {e}")
            continue

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
        try: 
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
        except Exception as e:
            logger.debug(f"Failed to get clean sponsor data for {row['bill_id']}: {e}")
            continue

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
        try:
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
        except Exception as e:
            logger.debug(f"Failed to get clean subject data for {row['bill_id']}: {e}")
            continue

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
        try:
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
        except Exception as e: 
            logger.debug(f"Failed to get clean companion data for {row['bill_id']}: {e}")
            continue
            
    return pd.DataFrame(companions_data, columns=['bill_id', 'leg_id', 'companion_bill_id', 'relationship'])

def get_committee_status_data(raw_bills_df):
    """
    Extract committee status data from raw bills dataframe into standardized format.
    
    Args:
        raw_bills_df (pd.DataFrame): DataFrame containing raw bill data
        
    Returns:
        pd.DataFrame: DataFrame with committee information
    """
    committees_data = []
    
    for _, row in raw_bills_df.iterrows():
        try:
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
                });
        
        except Exception as e:
            logger.debug(f"Failed to get clean committee votes data for {row['bill_id']}: {e}")
            continue
                
    return pd.DataFrame(committees_data, columns=['bill_id', 'leg_id', 'chamber', 'name',
                                                'status', #'subcommittee_name', 'subcommittee_status',
                                                'aye_votes', 'nay_votes', 'present_votes', 'absent_votes'])

def get_versions_data(raw_bills_df):
    versions_data = []
    
    for _, row in raw_bills_df.iterrows():
        try:
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
        except Exception as e:
            logger.debug(f"Failed to get clean version data for {row['bill_id']}: {e}")
            continue
            
    return pd.DataFrame(versions_data, columns=['bill_id', 'leg_id', 'type', 'text_order', 'description',
                                              'html_url', 'pdf_url', 'ftp_html_url', 'ftp_pdf_url'])

def get_links_data(raw_bills_df, config):
    links_data = []
    
    for _, row in raw_bills_df.iterrows():
        try:
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
        except Exception as e:
            logger.debug(f"Failed to create clean links data for {row['bill_id']}: {e}")
            continue

    return pd.DataFrame(links_data, columns=['bill_id', 'leg_id'] + list(config['sources']['html'].keys()))

def get_complete_bills_list(raw_bills_df):
    new_rows = []
    
    # Extract and clean bill_id and leg_id
    cleaned_data = []
    for _, row in raw_bills_df.iterrows():
        bill_id, leg_id = clean_bill_id(row['bill_id'])
        cleaned_data.append((bill_id, leg_id))
    
    cleaned_df = pd.DataFrame(cleaned_data, columns=["bill_id", "leg_id"])
    
    # Group by legislative session
    for leg_id, group in cleaned_df.groupby("leg_id"):
        bill_types = group["bill_id"].str.extract(r"([A-Z]+)(\d+)")  # Extract prefix and number
        group["prefix"] = bill_types[0]
        group["number"] = bill_types[1].astype(int)
        
        # Ensure each prefix has a consecutive sequence
        for prefix, sub_group in group.groupby("prefix"):
            min_bill = sub_group["number"].min()
            max_bill = sub_group["number"].max()
            print(f"Prefix: {prefix}, Min: {min_bill}, Max: {max_bill}")
            full_range = pd.DataFrame({
                "bill_id": [f"{prefix}{i}" for i in range(min_bill, max_bill + 1)],
                "leg_id": leg_id
            })
            new_rows.append(full_range)
    
    return pd.concat(new_rows, ignore_index=True)

def get_upcoming_committee_meetings(config):
    try:
        upcoming_meetings_df = get_rss_committee_meetings(config['sources']['rss']['upcoming'])
        return upcoming_meetings_df[['committee', 'chamber', 'date', 'time', 'location', 'chair', 'meeting_url']]
    except Exception as e:
        logger.error(f"{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')} -- Failed to get upcoming committee meetings data: {e}")
        return pd.DataFrame()

def get_upcoming_committee_meeting_bills(config):
    upcoming_meetings_df = get_rss_committee_meetings(config['sources']['rss']['upcoming'])

    # Create list to store flattened bill records
    bills_list = []
    
    # Iterate through meetings and their bills
    for _, meeting in upcoming_meetings_df.iterrows():
        try:
            # Get meeting details
            meeting_details = {
                'committee': meeting['committee'],
                'chamber': meeting['chamber'], 
                'date': meeting['date'],
                'time': meeting['time'],
                'meeting_url': meeting['meeting_url']
            }
            
            # Add each bill with meeting details
            for bill in meeting['bills']:
                try:
                    bill_record = meeting_details.copy()
                    # Extract leg_id from link using regex
                    leg_id = re.search(r'/tlodocs/(\w+)/', meeting['meeting_url']).group(1)
                    bill_record.update({
                        'bill_id': bill['bill_id'],
                        'leg_id': leg_id,
                        'link': bill['link'], 
                        'author': bill.get('author', None),
                        'description': bill.get('description', None),
                        'status': bill.get('status', None)
                    })
                    bills_list.append(bill_record)
                except Exception as e:
                    print(f"Error processing upcoming meeting bill {bill['bill_id']}: {e}")
        except Exception as e:
            logger.debug(f"Failed to get clean upcoming committee meeting bills data from RSS for {meeting['meeting_url']}: {e}")
            continue
            
    # Convert to DataFrame
    bills_df = pd.DataFrame(bills_list)
    return bills_df


def get_committee_meetings_data(config):
    upcoming_meetings_df = get_html_committee_meetings(config)
    return upcoming_meetings_df[['committee', 'chamber', 'committee_meetings_link', 'leg_id', 'date',
       'time', 'location', 'chair', 'meeting_url', 'subcommittee',
       'hearing_notice_html', 'hearing_notice_pdf', 'minutes_html',
       'minutes_pdf', 'witness_list_html', 'witness_list_pdf', 'comments']]

def get_committee_meeting_bills_data(config):
    upcoming_meetings_df = get_html_committee_meetings(config)

    # Create list to store flattened bill records 
    bills_list = []
    
    # Iterate through meetings and their bills
    for _, meeting in upcoming_meetings_df.iterrows():
        # Get meeting details
        meeting_details = {
            'committee': meeting['committee'],
            'chamber': meeting['chamber'],
            'date': meeting['date'], 
            'time': meeting['time'],
            'meeting_url': meeting['meeting_url']
        }
        
        # Add each bill with meeting details
        for bill in meeting['bills']:
            try:
                bill_record = meeting_details.copy()
                # Extract leg_id from link using regex
                leg_id = re.search(r'/tlodocs/(\w+)/', meeting['meeting_url']).group(1)
                bill_record.update({
                    'bill_id': bill['bill_id'],
                    'leg_id': leg_id,
                    'link': bill['link'],
                    'author': bill.get('author', None), 
                    'description': bill.get('description', None),
                    'status': bill.get('status', None)
                })
                bills_list.append(bill_record)
            except Exception as e:
                logger.debug(f"Failed to get upcoming committee meeting bills data from HTML for {meeting['meeting_url']}: {e}")
            
    # Convert to DataFrame
    bills_df = pd.DataFrame(bills_list)
    return bills_df

@task(retries=0, retry_delay_seconds=10, log_prints=False, cache_policy=NO_CACHE)
def get_bill_texts(duckdb_conn, ftp_conn, max_errors=5):
    # Check if curr_bill_texts table exists
    meta_tables = duckdb_conn.execute("SELECT table_name FROM information_schema.tables WHERE table_schema='bills'").fetchall()
    raw_tables = duckdb_conn.execute("SELECT table_name FROM information_schema.tables WHERE table_schema='raw_bills'").fetchall()
    meta_table_names = [t[0] for t in meta_tables]
    raw_table_names = [t[0] for t in raw_tables]

    if 'bill_texts' in raw_table_names and 'curr_versions' in meta_table_names:
        # Get PDF URLs that are in curr_bill_texts but not in curr_versions
        query = """
        SELECT DISTINCT versions.ftp_pdf_url 
        FROM bills.curr_versions versions 
        LEFT JOIN raw_bills.bill_texts bill_texts 
            ON bill_texts.ftp_pdf_url = versions.ftp_pdf_url
        WHERE bill_texts.ftp_pdf_url IS NULL
        """
        
    elif 'curr_versions' in meta_table_names:
        # Get all PDF URLs from curr_versions
        query = """
        SELECT DISTINCT ftp_pdf_url 
        FROM bills.curr_versions
        """
    else:
        logger.error("No table found in raw_bills.bill_texts or bills.curr_versions")
        raise ValueError("No table found in raw_bills.bill_texts or bills.curr_versions")

    pdf_urls = duckdb_conn.execute(query).fetchall()
    pdf_urls = [url[0] for url in pdf_urls]

    pdf_texts = []
    error_count = 0
    for url in pdf_urls:
        try:
            pdf_text = ftp_conn.get_pdf_text(url)
        except Exception as e:
            logger.debug(f"Failed to get PDF text for {url}: {e}")
            error_count += 1
            continue

        pdf_texts.append({
            'ftp_pdf_url': url,
            'text': pdf_text
        })
    if error_count > max_errors:
        logger.error(f"{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')} -- Failed to get PDF text for {error_count} bills")
        raise Exception(f"Failed to get PDF text for {error_count} bills")
    return pd.DataFrame(pdf_texts)

@task(retries=0, retry_delay_seconds=10, log_prints=False, cache_policy=NO_CACHE)
def get_raw_bills_data(base_path, leg_session, ftp_connection, max_errors=5):
    try:
        bill_urls = get_bill_urls(base_path, leg_session, ftp_connection)
    except Exception as e:
        logger.error(f"{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')} -- Failed to get bill URLs: {e}")
        raise Exception(f"Failed to get bill URLs: {e}")
    raw_bills = []
    error_count = 0
    for url in bill_urls[:10]:
        try:
            bill_data = parse_bill_xml(ftp_connection, url)
            if bill_data:
                raw_bills.append(bill_data)
        except Exception as e:
            logger.debug(f"Error getting bill data for {url}: {e}")
            error_count += 1
    if error_count > max_errors:
        logger.error(f"{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')} -- Failed to get bill data for {error_count} bills")
        raise Exception(f"Failed to get bill data for {error_count} bills")
    return pd.DataFrame(raw_bills)

