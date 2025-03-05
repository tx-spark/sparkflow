
import yaml
import pandas as pd
import re
from bs4 import BeautifulSoup
import requests
import gspread

################################################################################
# MISC UTILITY FUNCTIONS
################################################################################

def bill_text_table_to_df(soup_html):
    """
    Convert an HTML table containing bill text information into a pandas DataFrame.
    
    Args:
        soup_html: BeautifulSoup object containing the HTML table
        
    Returns:
        pandas DataFrame with the table data, where each row represents a bill version
        and columns contain metadata like version type and links to bill text
    """
    rows = soup_html.find_all('tr')
    
    if not rows:
        return []
    
    # Extract column names from the first row
    headers = [header.get_text(strip=True).replace('\n', ' ') for header in rows[0].find_all('td')]
    
    data_list = []
    
    for row in rows[1:]:  # Skip header row
        cells = row.find_all('td')
        row_dict = {}
        
        for i, cell in enumerate(cells):
            images = cell.find_all('img')
            links = cell.find_all('a')
            
            if images and links:
                # Store multiple images in a list of dictionaries
                image_data = []
                for img, link in zip(images, links):
                    img_dict = {
                        "name": img.get("alt", ""),
                        "URL": link.get("href", "")
                    }
                    image_data.append(img_dict)
                row_dict[headers[i]] = image_data
            else:
                row_dict[headers[i]] = cell.get_text(strip=True)
        
        data_list.append(row_dict)
    
    return pd.DataFrame(data_list)


def write_df_to_gsheets(df, google_sheets_id, worksheet_name):
    """
    Write a pandas DataFrame to a Google Sheets worksheet.
    
    Args:
        df: pandas DataFrame to write
        google_sheets_id: ID of the target Google Sheet 
        worksheet_name: Name of the worksheet to write to
        
    The function will resize the worksheet to match the DataFrame dimensions
    and write all data starting from cell A1.
    """
    google_sheets_df = df.copy()
    google_sheets_df.fillna('',inplace=True)

    gc = gspread.service_account()

    sh = gc.open_by_key(google_sheets_id)

    # Select the first worksheet
    worksheet = sh.worksheet(worksheet_name)

    # Convert DataFrame to list of lists (including column headers)
    data = [google_sheets_df.columns.tolist()] + google_sheets_df.values.tolist()

    # Minimize to just the data
    num_rows = len(data)
    num_cols = len(data[0])
    worksheet.resize(rows=num_rows,cols=num_cols)

    # Write data to the sheet, starting from A1
    worksheet.update(data,value_input_option="USER_ENTERED")


################################################################################
# UTILITY FUNCTIONS FOR SCRAPING DIFFERENT BILL ASPECTS
################################################################################

def get_bill_history(scraper_config, bill_id, session_id):
    """
    Retrieve the history information for a specific bill.
    
    Args:
        scraper_config: Dictionary containing scraping configuration
        bill_id: ID of the bill (e.g. 'HB1')
        session_id: Legislative session ID
        
    Returns:
        Dictionary containing bill history information including status,
        last action, and other metadata from the history page
    """
    bill_url = f'{scraper_config['sources']['html']['history']}?LegSess={session_id}&Bill={bill_id}'
    site_html = requests.get(bill_url,timeout=30).text
    soup = BeautifulSoup(site_html, 'html.parser')

    bill_history_dict = {}

    validation_summary = soup.find('div', id=lambda x: x and 'validationSummary' in x)
    if validation_summary is not None:
        # This means the bill does not yet exist
        bill_history_dict['status_value'] = 'Unassigned'
        return bill_history_dict

    bill_content = soup.find_all('div', id=lambda x: x and 'content' in x.lower())

    if not bill_content or len(bill_content) < 2:
        # This means the bill does not have the required information
        bill_history_dict['status_value'] = 'Unassigned'
        return bill_history_dict

    # Ignoring the first div, because it just contains information we already know
    bill_content = bill_content[1]
    bill_info_tables = bill_content.find_all('table')

    for table_html in bill_info_tables:
        
        for row in table_html.find_all('tr'):
            cells = row.find_all('td')
            if len(cells) == 2:
                key = cells[0].get_text(strip=True).replace(':', '')
                value = cells[1].get_text(strip=True)
                bill_history_dict[key] = value
    
    # TO DO: Define Status Logic
    bill_history_dict['status_value'] = 'Alive'
    return bill_history_dict

def get_bill_text(scraper_config, bill_id, session_id):
    """
    Retrieve the text versions available for a specific bill.
    
    Args:
        scraper_config: Dictionary containing scraping configuration
        bill_id: ID of the bill (e.g. 'HB1')
        session_id: Legislative session ID
        
    Returns:
        Dictionary containing URLs to introduced and most recent bill text PDFs,
        as well as a DataFrame of all available text versions
    """
    bill_text_url = f'{scraper_config['sources']['html']['text']}?LegSess={session_id}&Bill={bill_id}'
    bill_text_dict = {'bill_id': bill_id, 'session_id':session_id} # gets filled in with information

    site_html = requests.get(bill_text_url,timeout=30).text
    soup = BeautifulSoup(site_html, 'html.parser')

    validation_summary = soup.find('div', id=lambda x: x and 'validationSummary' in x)
    if validation_summary is not None:
        return {}

    bill_text_info_container = soup.find('form', id='Form1') # Why is it just called "form1"??? That's not descriptive at all??
    bill_info_tables = bill_text_info_container.find_all('table')

    versions_table = bill_info_tables[0]
    if 'not yet available' in versions_table.text.strip().lower():
        return {}
    
    versions_df = bill_text_table_to_df(versions_table)

    introduced_only_versions_df = versions_df[versions_df['Version'] == 'Introduced']
    bill_text_dict = {
        'introduced_text_pdf': 'https://capitol.texas.gov' + list(filter(lambda a: a['name'] == 'Adobe PDF', introduced_only_versions_df.iloc[0]['Bill']))[0]['URL'], # gross, sorry!
        'recent_text_pdf': 'https://capitol.texas.gov' + list(filter(lambda a: a['name'] == 'Adobe PDF', versions_df[versions_df['Bill'] != ''].iloc[-1]['Bill']))[0]['URL'],
        'text_versions_df' : versions_df
    }
    return bill_text_dict

def get_bill_stages(scraper_config, bill_id, session_id):
    """
    Retrieve the progression stages of a specific bill.
    
    Args:
        scraper_config: Dictionary containing scraping configuration
        bill_id: ID of the bill (e.g. 'HB1')
        session_id: Legislative session ID
        
    Returns:
        Dictionary containing a list of stages the bill has gone through,
        with each stage including number, status, date and continuation status
    """
    bill_text_url = f'{scraper_config['sources']['html']['bill_stages']}?LegSess={session_id}&Bill={bill_id}'
    bill_text_dict = {} # gets filled in with information

    site_html = requests.get(bill_text_url,timeout=30).text
    soup = BeautifulSoup(site_html, 'html.parser')

    stages_div = soup.find('div', id='usrBillStages_pnlBillStages')
    # Initialize empty list to store stages
    bill_text_dict['stages'] = []

    if stages_div:
        # Find all stage boxes and continuations
        stage_boxes = stages_div.find_all('div', class_='bill-status-box-complete')
        stage_continuations = stages_div.find_all('div', class_='bill-status-continuation')

        # Process each stage
        for box, continuation in zip(stage_boxes, stage_continuations):
            stage_num = box.find('div', class_='stage').text.strip().replace('Stage ', '')
            status = box.find('div', class_='bill-status-status').text.strip()
            date = box.find('div', class_='bill-status-date').text.strip()
            
            # Get image filename from continuation div
            img = continuation.find('img')
            img_src = img['src'].split('/')[-1] if img else None

            stage = {
                'stage_number': stage_num,
                'status': status,
                'date': date,
                'continuation_image': img_src,
                'after_status': img_src.replace('.gif','')
            }
            bill_text_dict['stages'].append(stage)
    else:
        # If no stages div found, return empty list
        bill_text_dict['stages'] = []

    return bill_text_dict

def get_bill_actions(scraper_config, bill_id, session_id):
    """
    Retrieve all actions taken on a specific bill.
    
    Args:
        scraper_config: Dictionary containing scraping configuration
        bill_id: ID of the bill (e.g. 'HB1')
        session_id: Legislative session ID
        
    Returns:
        Dictionary containing a list of all actions taken on the bill,
        with each action including chamber, description, date, time and other metadata
    """
    bill_actions_url = f'{scraper_config['sources']['html']['actions']}?LegSess={session_id}&Bill={bill_id}'
    bill_actions_dict = {} # gets filled in with information

    site_html = requests.get(bill_actions_url,timeout=30).text
    soup = BeautifulSoup(site_html, 'html.parser')
    
    validation_summary = soup.find('div', id=lambda x: x and 'validationSummary' in x)
    if validation_summary is not None:
        return {}
    
    actions_form = soup.find('form', id='Form1')
    actions_table = actions_form.find_all('table')[1]

    # Initialize empty list to store actions
    bill_actions_dict['actions'] = []

    # Get all rows except header
    action_rows = actions_table.find_all('tr')[1:]
    
    for row in action_rows:
        cells = row.find_all('td')
        if len(cells) >= 6:
            # Get link if it exists
            description_cell = cells[1]
            description_link = description_cell.find('a')
            link_url = description_link['href'] if description_link else None
            
            action = {
                'chamber': cells[0].text.strip(),
                'description': cells[1].text.strip(),
                'description_link': link_url,
                'comment': cells[2].text.strip(),
                'date': cells[3].text.strip(),
                'time': cells[4].text.strip(),
                'journal_page': cells[5].text.strip()
            }
            bill_actions_dict['actions'].append(action)

    return bill_actions_dict


def get_bill_companions(scraper_config, bill_id, session_id):
    """
    Retrieve companion bills for a specific bill.
    
    Args:
        scraper_config: Dictionary containing scraping configuration
        bill_id: ID of the bill (e.g. 'HB1')
        session_id: Legislative session ID
        
    Returns:
        Dictionary containing a list of companion bills,
        with each companion including bill number and link
    """
    bill_companions_url = f'{scraper_config['sources']['html']['companions']}?LegSess={session_id}&Bill={bill_id}'
    bill_companions_dict = {'companions': []} # gets filled in with information

    site_html = requests.get(bill_companions_url,timeout=30).text
    soup = BeautifulSoup(site_html, 'html.parser')
    companions_form = soup.find('form', id='Form1')

    if len( companions_form.find_all('table')) < 2:
        return {}
    
    companions_table = companions_form.find_all('table')[1]

    # Find all companion bill rows (they have links in first td)
    companion_rows = companions_table.find_all('tr')
    
    for i in range(0, len(companion_rows), 5):  # Each companion takes 5 rows
        if i + 4 < len(companion_rows):
            bill_link = companion_rows[i].find('a')
            if bill_link:
                companion = {
                    'bill_number': bill_link.text.strip(),
                    'bill_link': 'https://capitol.texas.gov/BillLookup/' + bill_link['href']
                }
                bill_companions_dict['companions'].append(companion)

    return bill_companions_dict

def get_bill_authors(scraper_config, bill_id, session_id):
    """
    Retrieve authors and coauthors for a specific bill.
    
    Args:
        scraper_config: Dictionary containing scraping configuration
        bill_id: ID of the bill (e.g. 'HB1')
        session_id: Legislative session ID
        
    Returns:
        Dictionary containing lists of primary authors and coauthors,
        with each author including name and date added
    """
    bill_authors_url = f'{scraper_config['sources']['html']['authors']}?LegSess={session_id}&Bill={bill_id}'
    bill_authors_dict = {'primary_authors': [], 'coauthors': []}

    # Get the HTML content
    site_html = requests.get(bill_authors_url, timeout=30).text
    soup = BeautifulSoup(site_html, 'html.parser')

    # Get primary authors
    primary_authors_table = soup.find('table', id='tblPrimaryAuthors')
    if primary_authors_table:
        for row in primary_authors_table.find_all('tr')[1:]:  # Skip header row
            cells = row.find_all('td')
            if len(cells) >= 2:
                author = {
                    'name': cells[0].text.strip(),
                    'date': cells[1].text.strip()
                }
                bill_authors_dict['primary_authors'].append(author)

    # Get coauthors
    coauthors_table = soup.find('table', id='tblCoauthors')
    if coauthors_table:
        for row in coauthors_table.find_all('tr')[1:]:  # Skip header row
            cells = row.find_all('td')
            if len(cells) >= 2:
                author = {
                    'name': cells[0].text.strip(),
                    'date': cells[1].text.strip()
                }
                bill_authors_dict['coauthors'].append(author)

    return bill_authors_dict

def get_bill_amendments(scraper_config, bill_id, session_id):
    """
    Retrieve amendments for a specific bill.
    
    Args:
        scraper_config: Dictionary containing scraping configuration
        bill_id: ID of the bill (e.g. 'HB1')
        session_id: Legislative session ID
        
    Returns:
        Dictionary containing lists of amendments with reading, number, author,
        coauthor, type, action, date and text links
    """
    bill_amendments_url = f'{scraper_config['sources']['html']['amendments']}?LegSess={session_id}&Bill={bill_id}'
    bill_amendments_dict = {'amendments': []} # gets filled in with information

    # Get the HTML content
    site_html = requests.get(bill_amendments_url, timeout=30).text
    soup = BeautifulSoup(site_html, 'html.parser')
    
    # Find the amendments table
    table = soup.find('table', {'border': '1'})
    if not table:
        return bill_amendments_dict
        
    # Get all rows except header
    amendment_rows = table.find_all('tr')[1:]
    
    for row in amendment_rows:
        cells = row.find_all('td')
        if len(cells) >= 8:
            # Get text links if they exist
            text_cell = cells[7]
            html_link = text_cell.find('a', href=lambda x: x and 'html' in x.lower())
            pdf_link = text_cell.find('a', href=lambda x: x and 'pdf' in x.lower())
            
            amendment = {
                'reading': cells[0].text.strip(),
                'number': cells[1].text.strip(),
                'author': cells[2].text.strip(),
                'coauthor': cells[3].text.strip(),
                'type': cells[4].text.strip(),
                'action': cells[5].text.strip(),
                'date': cells[6].text.strip(),
                'html_link': 'https://capitol.texas.gov/' + html_link['href'] if html_link else None,
                'pdf_link': 'https://capitol.texas.gov/' + pdf_link['href'] if pdf_link else None
            }
            bill_amendments_dict['amendments'].append(amendment)

    return bill_amendments_dict

################################################################################
# CORE BILL PROCESSING FUNCTIONS
################################################################################

def get_bill_dict(scraper_config, bill_id, session_id):
    """
    Get all available information for a bill and return as dictionary.
    
    Args:
        scraper_config: Dictionary containing scraping configuration
        bill_id: ID of the bill (e.g. 'HB1')
        session_id: Legislative session ID
        
    Returns:
        Dictionary containing all available information about the bill,
        including history, text, stages, actions, companions, and authors
    """
    bill_info_dict = {'bill_id': bill_id, 'session_id': session_id}
    
    # Get data from all available functions
    bill_history_dict = get_bill_history(scraper_config, bill_id, session_id)
    bill_text_dict = get_bill_text(scraper_config, bill_id, session_id)
    bill_stages_dict = get_bill_stages(scraper_config, bill_id, session_id)
    bill_actions_dict = get_bill_actions(scraper_config, bill_id, session_id)
    bill_companions_dict = get_bill_companions(scraper_config, bill_id, session_id)
    bill_authors_dict = get_bill_authors(scraper_config, bill_id, session_id)
    bill_amendments_dict = get_bill_amendments(scraper_config, bill_id, session_id)

    # Update main dictionary with all the data
    bill_info_dict.update(bill_history_dict)
    bill_info_dict.update(bill_text_dict)
    bill_info_dict.update(bill_stages_dict)
    bill_info_dict.update(bill_actions_dict)
    bill_info_dict.update(bill_companions_dict)
    bill_info_dict.update(bill_authors_dict)
    bill_info_dict.update(bill_amendments_dict)
    # Get URLS
    info_urls = list(scraper_config['sources']['html'].keys())
    for key in info_urls:
        url = f"{scraper_config['sources']['html'][key]}?LegSess={session_id}&Bill={bill_id}"
        bill_info_dict[f'{key}_url'] = url

    return bill_info_dict

################################################################################
# DATA CLEANING FUNCTIONS
################################################################################

def clean_bill_dict(bill_info_dict):
    """
    Clean and format the raw bill information dictionary.
    
    Args:
        bill_info_dict: Dictionary containing raw bill information
        
    Returns:
        Dictionary with cleaned and formatted bill information including:
        - Parsed last action into date, chamber and description
        - Parsed subjects into list with codes
        - Formatted hyperlinks for authors and actions
        - Parsed vote counts
    """
    if 'Last Action' in bill_info_dict.keys():
        last_action_string = bill_info_dict['Last Action']
        match = re.match(r"(\d{2}/\d{2}/\d{4})\s+(\S)\s+(.*)", last_action_string)
        if match:
            date, chamber, description = match.groups()
            bill_info_dict['last_action_date'] = date
            bill_info_dict['last_action_chamber'] = chamber
            bill_info_dict['last_action_description'] = description

    # Subjects
    if 'Subjects' in bill_info_dict.keys():
        subject_matches = re.findall(r"([^()]+)\s+\((I\d+)\)", bill_info_dict['Subjects'])
        subjects_list = [{"subject": subject.strip(), "code": code} for subject, code in subject_matches]
        bill_info_dict['subjects_list'] = subjects_list

    if 'Author' in bill_info_dict.keys():
        bill_info_dict['authors_hyperlink'] = f'=HYPERLINK("{bill_info_dict['authors_url']}", "{bill_info_dict['Author'].replace('"','""')}")'
    if 'last_action_description' in bill_info_dict.keys():
        bill_info_dict['last_action_hyperlink'] = f'=HYPERLINK("{bill_info_dict['actions_url']}", "{bill_info_dict['last_action_description'].replace('"','""')}")'
    if 'companions' in bill_info_dict.keys() and len(bill_info_dict['companions']) > 0:
        bill_info_dict['companion_bill_hyperlink'] = f'=HYPERLINK("{bill_info_dict['companions'][0]['bill_link']}", "{bill_info_dict['companions'][0]['bill_number']}")'
    if 'amendments' in bill_info_dict.keys() and len(bill_info_dict['amendments']) > 0:
        bill_info_dict['amendments_hyperlink'] = f'=HYPERLINK("{bill_info_dict['amendments_url']}", "{len(bill_info_dict['amendments'])}")'
    else:
        bill_info_dict['amendments_hyperlink'] = None
    if bill_info_dict['status_value'] == 'Unassigned':
        bill_info_dict['text_url'] = None

    # Votes
    if 'Vote' in bill_info_dict.keys():
        vote_matches = re.findall(r'(\w+(?: \w+)*)=(\d+)', bill_info_dict['Vote'])
        bill_info_dict['vote_counts'] = {key: int(value) for key, value in vote_matches}

    return bill_info_dict

def clean_bills_df(raw_bills_df):
    final_col_names = {
        'bill_id':'Bill Number',
        'Caption Text': 'Caption',
        'history_url': 'Bill History/Status',
        'authors_hyperlink': 'Authors',
        'captions_url': 'Captions',
        'status_value': 'Dead|Alive|Unassigned|Law',
        'last_action_date': 'Latest Action Date',
        'last_action_chamber':'Latest Action Chamber',
        'last_action_hyperlink': 'Latest Action',
        'text_url': 'Link | All Texts',
        'recent_text_pdf': 'Recent Bill Text',
        'introduced_text_pdf': 'Introduced Text',
        'companion_bill_hyperlink': 'Companions',
        'amendments_hyperlink': 'Amendments',
    }
    renamed_bills_df = raw_bills_df.rename(columns=final_col_names)
    cols_order = ['Bill Number','Caption','Bill History/Status','Authors',
                  'Captions','Dead|Alive|Unassigned|Law', 'Latest Action Date',
                  'Latest Action Chamber','Latest Action', 'Link | All Texts',
                  'Recent Bill Text', 'Introduced Text', 'Companions', 'Amendments'
                  ]
    reduced_bills_df = renamed_bills_df[cols_order]
    return reduced_bills_df.reindex(columns = cols_order)

################################################################################
# MAIN EXECUTION
################################################################################

def scrape_bills(scraper_config, session_id, consecutive_misses_allowed, bill_prefix):
    """
    Scrape bills with the given prefix and return cleaned dataframe.
    
    Args:
        scraper_config: Dictionary containing scraping configuration
        session_id: Legislative session ID
        consecutive_misses_allowed: Number of consecutive non-existent bills before stopping
        bill_prefix: Bill prefix to scrape ('HB' or 'SB')
        
    Returns:
        Cleaned pandas DataFrame containing information for all found bills
    """
    bills_list = []
    misses = 0
    curr_bill_num = 1

    miss_backlog = []

    while misses < consecutive_misses_allowed:
        raw_bill = get_bill_dict(scraper_config, f'{bill_prefix}{curr_bill_num}', session_id)
        curr_bill = clean_bill_dict(raw_bill)
        print(f'{bill_prefix}{curr_bill_num}')

        if curr_bill['status_value'] == 'Unassigned':
            misses += 1
        else:
            misses = 0
        
        bills_list.append(curr_bill)
        curr_bill_num += 1

    raw_bills_df = pd.DataFrame(bills_list)
    return clean_bills_df(raw_bills_df)

def main():
    """
    Main execution function that:
    1. Loads the scraper configuration
    2. Scrapes both house and senate bills
    3. Saves results to TSV files
    """
    # Load config
    CONFIG_PATH = 'scraper/scraper_config.yaml'

    with open(CONFIG_PATH, 'r') as file:
        scraper_config = yaml.safe_load(file)

    session_id = scraper_config['info']['LegSess']
    consecutive_misses_allowed = scraper_config['config']['misses_allowed']
    google_sheets_id = scraper_config['config']['google_sheets_id']

    # Scrape house bills
    house_bills_df = scrape_bills(scraper_config, session_id, consecutive_misses_allowed, 'HB')
    house_bills_df.to_csv('house_bills.tsv', sep='\t', index=False)
    worksheet_name = 'All House Bills'
    write_df_to_gsheets(house_bills_df, google_sheets_id, worksheet_name)

    # Scrape senate bills  
    senate_bills_df = scrape_bills(scraper_config, session_id, consecutive_misses_allowed, 'SB')
    senate_bills_df.to_csv('senate_bills.tsv', sep='\t', index=False)
    worksheet_name = 'All Senate Bills'
    write_df_to_gsheets(senate_bills_df, google_sheets_id, worksheet_name)

if __name__ == "__main__":
    main()


