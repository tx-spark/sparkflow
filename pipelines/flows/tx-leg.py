import yaml
import datetime
import dlt
import duckdb
import logging
import pandas as pd

from prefect import flow
from utils import FtpConnection, dataframe_to_bigquery, dataframe_to_duckdb
from extract_functions import *

logger = logging.getLogger(__name__)

################################################################################
# CONFIGURATION
################################################################################

CONFIG_PATH = 'config.yaml'
DUCKDB_NAME = "texas_bills"


################################################################################
# DATA PIPELINE
################################################################################


def bills(raw_bills_df, curr_bills_df=None, duckdb_conn = None):
    bills_df = get_bills_data(raw_bills_df)
    
    result_df = merge_with_current_data(bills_df, curr_bills_df)
    dataframe_to_bigquery(result_df, 'lgover', 'tx_leg_raw_bills', 'bills', 'dev', 'drop')
    dataframe_to_duckdb(result_df, duckdb_conn, 'tx_leg_raw_bills', 'bills', 'dev', 'drop')

def actions(raw_bills_df, curr_actions_df=None, duckdb_conn = None):
    actions_df = get_actions_data(raw_bills_df)
    result_df = merge_with_current_data(actions_df, curr_actions_df)

    result_df = result_df.drop_duplicates()
    dataframe_to_bigquery(result_df, 'lgover', 'tx_leg_raw_bills', 'actions', 'dev', 'drop')
    dataframe_to_duckdb(result_df, duckdb_conn, 'tx_leg_raw_bills', 'actions', 'dev', 'drop')
        

def authors(raw_bills_df, curr_authors_df=None, duckdb_conn = None):
    authors_df = get_authors_data(raw_bills_df)
    
    result_df = merge_with_current_data(authors_df, curr_authors_df)
    dataframe_to_bigquery(result_df, 'lgover', 'tx_leg_raw_bills', 'authors', 'dev', 'drop')
    dataframe_to_duckdb(result_df, duckdb_conn, 'tx_leg_raw_bills', 'authors', 'dev', 'drop')

def bill_stages(raw_bills_df, config, curr_bill_stages_df=None, duckdb_conn = None):
    bill_stages_df = get_bill_stages(config['sources']['html']['bill_stages'], raw_bills_df)
    
    result_df = merge_with_current_data(bill_stages_df, curr_bill_stages_df)
    dataframe_to_bigquery(result_df, 'lgover', 'tx_leg_raw_bills', 'bill_stages', 'dev', 'drop')
    dataframe_to_duckdb(result_df, duckdb_conn, 'tx_leg_raw_bills', 'bill_stages', 'dev', 'drop')

def bill_texts(duckdb_conn, ftp_conn):
    first_seen_at = datetime.datetime.now().strftime('%Y-%m-%d %H:%M')
    bill_texts_df = get_bill_texts(duckdb_conn, ftp_conn)
    bill_texts_df['seen_at'] = first_seen_at
    dataframe_to_bigquery(bill_texts_df, 'lgover', 'tx_leg_raw_bills', 'bill_texts', 'dev', 'append')
    dataframe_to_duckdb(bill_texts_df, duckdb_conn, 'tx_leg_raw_bills', 'bill_texts', 'dev', 'append')


def committees(raw_bills_df, curr_committees_df=None, duckdb_conn = None):
    committees_df = get_committees_data(raw_bills_df)
    
    result_df = merge_with_current_data(committees_df, curr_committees_df)
    dataframe_to_bigquery(result_df, 'lgover', 'tx_leg_raw_bills', 'committees', 'dev', 'drop')
    dataframe_to_duckdb(result_df, duckdb_conn, 'tx_leg_raw_bills', 'committees', 'dev', 'drop')
    yield result_df

def committee_hearing_videos(config, curr_committee_hearing_videos_df=None, duckdb_conn = None):
    committee_hearing_videos_df = get_committee_hearing_videos_data(config)
    
    result_df = merge_with_current_data(committee_hearing_videos_df, curr_committee_hearing_videos_df)
    dataframe_to_bigquery(result_df, 'lgover', 'tx_leg_raw_bills', 'committee_hearing_videos', 'dev', 'drop')
    dataframe_to_duckdb(result_df, duckdb_conn, 'tx_leg_raw_bills', 'committee_hearing_videos', 'dev', 'drop')
    yield result_df

def committee_meetings(config, curr_committee_meetings_df=None, duckdb_conn = None):
    committee_meetings_df = get_committee_meetings_data(config)
    result_df = merge_with_current_data(committee_meetings_df, curr_committee_meetings_df)
    dataframe_to_bigquery(result_df, 'lgover', 'tx_leg_raw_bills', 'committee_meetings', 'dev', 'drop')
    dataframe_to_duckdb(result_df, duckdb_conn, 'tx_leg_raw_bills', 'committee_meetings', 'dev', 'drop')
    yield result_df

def committee_meeting_bills(config, curr_committee_meeting_bills_df=None, duckdb_conn = None):
    committee_meeting_bills_df = get_committee_meeting_bills_data(config)
    
    result_df = merge_with_current_data(committee_meeting_bills_df, curr_committee_meeting_bills_df)
    dataframe_to_bigquery(result_df, 'lgover', 'tx_leg_raw_bills', 'committee_meeting_bills', 'dev', 'drop')
    dataframe_to_duckdb(result_df, duckdb_conn, 'tx_leg_raw_bills', 'committee_meeting_bills', 'dev', 'drop')
    yield result_df

def committee_meetings_links(config, curr_committee_meetings_df=None, duckdb_conn = None):

    committee_meetings_df = get_committee_meetings_data(config)
    
    result_df = merge_with_current_data(committee_meetings_df, curr_committee_meetings_df)
    dataframe_to_bigquery(result_df, 'lgover', 'tx_leg_raw_bills', 'committee_meetings_links', 'dev', 'drop')
    dataframe_to_duckdb(result_df, duckdb_conn, 'tx_leg_raw_bills', 'committee_meetings_links', 'dev', 'drop')
    yield result_df

def companions(raw_bills_df, curr_companions_df=None, duckdb_conn = None):
    companions_df = get_companions_data(raw_bills_df)
    result_df = merge_with_current_data(companions_df, curr_companions_df)
    dataframe_to_bigquery(result_df, 'lgover', 'tx_leg_raw_bills', 'companions', 'dev', 'drop')
    dataframe_to_duckdb(result_df, duckdb_conn, 'tx_leg_raw_bills', 'companions', 'dev', 'drop')
    
def complete_bills_list(raw_bills_df, curr_complete_bills_list_df=None, duckdb_conn = None):

    complete_bills_list_df = get_complete_bills_list(raw_bills_df)
    
    result_df = merge_with_current_data(complete_bills_list_df, curr_complete_bills_list_df)
    dataframe_to_bigquery(result_df, 'lgover', 'tx_leg_raw_bills', 'complete_bills_list', 'dev', 'drop')
    dataframe_to_duckdb(result_df, duckdb_conn, 'tx_leg_raw_bills', 'complete_bills_list', 'dev', 'drop')
    yield result_df

def links(raw_bills_df, config, curr_links_df=None, duckdb_conn = None):
    links_df = get_links_data(raw_bills_df,config)
    
    result_df = merge_with_current_data(links_df, curr_links_df)
    dataframe_to_bigquery(result_df, 'lgover', 'tx_leg_raw_bills', 'links', 'dev', 'drop')
    dataframe_to_duckdb(result_df, duckdb_conn, 'tx_leg_raw_bills', 'links', 'dev', 'drop')
    yield result_df


def sponsors(raw_bills_df, curr_sponsors_df=None, duckdb_conn = None):
    sponsors_df = get_sponsors_data(raw_bills_df)
    
    result_df = merge_with_current_data(sponsors_df, curr_sponsors_df)
    dataframe_to_bigquery(result_df, 'lgover', 'tx_leg_raw_bills', 'sponsors', 'dev', 'drop')
    dataframe_to_duckdb(result_df, duckdb_conn, 'tx_leg_raw_bills', 'sponsors', 'dev', 'drop')

def subjects(raw_bills_df, curr_subjects_df=None, duckdb_conn = None):
    subjects_df = get_subjects_data(raw_bills_df)
    
    result_df = merge_with_current_data(subjects_df, curr_subjects_df)
    dataframe_to_bigquery(result_df, 'lgover', 'tx_leg_raw_bills', 'subjects', 'dev', 'drop')
    dataframe_to_duckdb(result_df, duckdb_conn, 'tx_leg_raw_bills', 'subjects', 'dev', 'drop')


def rss_feeds(config, curr_rss_df=None, duckdb_conn = None):
    rss_df = get_rss_data(config)
    
    result_df = merge_with_current_data(rss_df, curr_rss_df)
    dataframe_to_bigquery(result_df, 'lgover', 'tx_leg_raw_bills', 'rss_feeds', 'dev', 'drop')
    dataframe_to_duckdb(result_df, duckdb_conn, 'tx_leg_raw_bills', 'rss_feeds', 'dev', 'drop')

def upcoming_committee_meetings(config, duckdb_conn):
    upcoming_meetings_df = get_upcoming_committee_meetings(config)
    upcoming_meetings_df['seen_at'] = datetime.datetime.now().strftime('%Y-%m-%d %H:%M')
    dataframe_to_bigquery(upcoming_meetings_df, 'lgover', 'tx_leg_raw_bills', 'upcoming_committee_meetings', 'dev', 'append')
    dataframe_to_duckdb(upcoming_meetings_df, duckdb_conn, 'tx_leg_raw_bills', 'upcoming_committee_meetings', 'dev', 'append')
    yield upcoming_meetings_df

def upcoming_committee_meeting_bills(config, duckdb_conn):
    upcoming_meeting_bills_df = get_upcoming_committee_meeting_bills(config)
    upcoming_meeting_bills_df['seen_at'] = datetime.datetime.now().strftime('%Y-%m-%d %H:%M')
    dataframe_to_bigquery(upcoming_meeting_bills_df, 'lgover', 'tx_leg_raw_bills', 'upcoming_committee_meeting_bills', 'dev', 'append')
    dataframe_to_duckdb(upcoming_meeting_bills_df, duckdb_conn, 'tx_leg_raw_bills', 'upcoming_committee_meeting_bills', 'dev', 'append')

def versions(raw_bills_df, curr_versions_df=None, duckdb_conn = None):
    versions_df = get_versions_data(raw_bills_df)
    
    result_df = merge_with_current_data(versions_df, curr_versions_df)
    dataframe_to_bigquery(result_df, 'lgover', 'tx_leg_raw_bills', 'versions', 'dev', 'drop')
    dataframe_to_duckdb(result_df, duckdb_conn, 'tx_leg_raw_bills', 'versions', 'dev', 'drop')

def run_logs(start_time, end_time, notes, duckdb_conn = None):
    run_logs_df = pd.DataFrame({
        "start_time": [start_time],
        "end_time": [end_time],
        "notes": [notes]
    })
    dataframe_to_bigquery(run_logs_df, 'lgover', 'tx_leg_raw_bills', 'run_logs', 'dev', 'append')
    dataframe_to_duckdb(run_logs_df, duckdb_conn, 'tx_leg_raw_bills', 'run_logs', 'dev', 'append')

################################################################################
# MAIN
################################################################################

@flow(name="Texas Leg Pipeline", log_prints=True)
def tx_leg_pipeline():

    logger = logging.getLogger(__name__)

    start_time = datetime.datetime.now()
    with open(CONFIG_PATH, "r", encoding="utf-8") as f:
        config = yaml.safe_load(f)


    try:
        conn = FtpConnection(config['sources']['ftp']['host'])
    except Exception as e:
        logger.error(f"ERROR -- Failed to connect to FTP: {e}")
        raise e

    base_path = config['sources']['ftp']['base_path']
    leg_session = config['info']['LegSess']

    try:
        logger.info("INFO -- Starting raw bills data extraction")
        raw_bills_df = get_raw_bills_data(base_path, leg_session, conn)
        logger.info("INFO -- Raw bills data extraction complete")
    except Exception as e:
        logger.error(f"ERROR -- Failed to get raw bills data: {e}")
        raise e
    
    try:
        duckdb_conn = duckdb.connect(f"{DUCKDB_NAME}.duckdb")
    except Exception as e:
        logger.error(f"ERROR -- Failed to connect to DuckDB: {e}")
        raise e

    curr_bills_df = get_current_table_data(duckdb_conn, 'bills', OUT_DATASET_NAME)
    curr_authors_df = get_current_table_data(duckdb_conn, 'authors', OUT_DATASET_NAME) 
    curr_subjects_df = get_current_table_data(duckdb_conn, 'subjects', OUT_DATASET_NAME)
    curr_committees_df = get_current_table_data(duckdb_conn, 'committees', OUT_DATASET_NAME)
    curr_versions_df = get_current_table_data(duckdb_conn, 'versions', OUT_DATASET_NAME)
    curr_actions_df = get_current_table_data(duckdb_conn, 'actions', OUT_DATASET_NAME)
    curr_companions_df = get_current_table_data(duckdb_conn, 'companions', OUT_DATASET_NAME)
    curr_links_df = get_current_table_data(duckdb_conn, 'links', OUT_DATASET_NAME)
    curr_committee_meetings_df = get_current_table_data(duckdb_conn, 'committee_meetings', OUT_DATASET_NAME)
    curr_committee_meeting_bills_df = get_current_table_data(duckdb_conn, 'committee_meeting_bills', OUT_DATASET_NAME)
    curr_bill_stages_df = get_current_table_data(duckdb_conn, 'bill_stages', OUT_DATASET_NAME)
    curr_complete_bills_list_df = get_current_table_data(duckdb_conn, 'complete_bills_list', OUT_DATASET_NAME)
    curr_rss_df = get_current_table_data(duckdb_conn, 'rss_feeds', OUT_DATASET_NAME)
    curr_committee_hearing_videos_df = get_current_table_data(duckdb_conn, 'committee_hearing_videos', OUT_DATASET_NAME)

    bills(raw_bills_df, curr_bills_df, duckdb_conn),
    authors(raw_bills_df, curr_authors_df, duckdb_conn),
    subjects(raw_bills_df, curr_subjects_df, duckdb_conn),
    committees(raw_bills_df, curr_committees_df, duckdb_conn),
    versions(raw_bills_df, curr_versions_df, duckdb_conn),
    actions(raw_bills_df, curr_actions_df, duckdb_conn),
    companions(raw_bills_df, curr_companions_df, duckdb_conn),
    links(raw_bills_df, config, curr_links_df, duckdb_conn),
    committee_meetings(config, curr_committee_meetings_df, duckdb_conn),
    committee_meeting_bills(config, curr_committee_meeting_bills_df, duckdb_conn),
    bill_stages(raw_bills_df, config, curr_bill_stages_df, duckdb_conn),
    complete_bills_list(raw_bills_df, curr_complete_bills_list_df, duckdb_conn),
    upcoming_committee_meetings(config, duckdb_conn),
    upcoming_committee_meeting_bills(config, duckdb_conn),
    committee_hearing_videos(config, curr_committee_hearing_videos_df, duckdb_conn),
    bill_texts(duckdb_conn, conn)
    # rss_feeds(config, curr_rss_df),
    run_logs(start_time, datetime.datetime.now(), "")

    duckdb_conn.close()

if __name__ == "__main__":
    tx_leg_pipeline()