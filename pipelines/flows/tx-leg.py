import yaml
import datetime
import duckdb
import logging
import pandas as pd

from prefect import flow
from utils import FtpConnection, dataframe_to_bigquery, dataframe_to_duckdb, log_bq_load, get_current_table_data
from extract_functions import *

################################################################################
# CONFIGURATION
################################################################################

CONFIG_PATH = 'config.yaml'
DUCKDB_NAME = "texas_bills"
LOG_PATH = 'tx-leg.log'
OUT_DATASET_NAME = 'tx_leg_raw_bills'
ENV = 'dev'

logger = logging.getLogger(__name__)
logging.basicConfig(filename=LOG_PATH, level=logging.DEBUG)

################################################################################
# DATA PIPELINE
################################################################################


def bills(raw_bills_df, curr_bills_df=None, duckdb_conn = None):
    bills_df = get_bills_data(raw_bills_df)
    
    result_df = merge_with_current_data(bills_df, curr_bills_df)
    dataframe_to_bigquery(result_df, 'lgover', OUT_DATASET_NAME, 'bills', ENV, 'drop')
    dataframe_to_duckdb(result_df, duckdb_conn, OUT_DATASET_NAME, 'bills', ENV, 'drop')
    log_bq_load('lgover', OUT_DATASET_NAME, 'bills', ENV, 'drop', duckdb_conn)
    logger.info("Bills data pipeline complete")

def actions(raw_bills_df, curr_actions_df=None, duckdb_conn = None):
    actions_df = get_actions_data(raw_bills_df)
    result_df = merge_with_current_data(actions_df, curr_actions_df)

    result_df = result_df.drop_duplicates()
    dataframe_to_bigquery(result_df, 'lgover', OUT_DATASET_NAME, 'actions', ENV, 'drop')
    dataframe_to_duckdb(result_df, duckdb_conn, OUT_DATASET_NAME, 'actions', ENV, 'drop')
    log_bq_load('lgover', OUT_DATASET_NAME, 'actions', ENV, 'drop', duckdb_conn)
    logger.info("Actions data pipeline complete")

def authors(raw_bills_df, curr_authors_df=None, duckdb_conn = None):
    authors_df = get_authors_data(raw_bills_df)
    
    result_df = merge_with_current_data(authors_df, curr_authors_df)
    dataframe_to_bigquery(result_df, 'lgover', OUT_DATASET_NAME, 'authors', ENV, 'drop')
    dataframe_to_duckdb(result_df, duckdb_conn, OUT_DATASET_NAME, 'authors', ENV, 'drop')
    log_bq_load('lgover', OUT_DATASET_NAME, 'authors', ENV, 'drop', duckdb_conn)
    logger.info("Authors data pipeline complete")

def bill_stages(raw_bills_df, config, curr_bill_stages_df=None, duckdb_conn = None):
    bill_stages_df = get_bill_stages(config['sources']['html']['bill_stages'], raw_bills_df)
    
    result_df = merge_with_current_data(bill_stages_df, curr_bill_stages_df)
    dataframe_to_bigquery(result_df, 'lgover', OUT_DATASET_NAME, 'bill_stages', ENV, 'drop')
    dataframe_to_duckdb(result_df, duckdb_conn, OUT_DATASET_NAME, 'bill_stages', ENV, 'drop')
    log_bq_load('lgover', OUT_DATASET_NAME, 'bill_stages', ENV, 'drop', duckdb_conn)
    logger.info("Bill stages data pipeline complete")

def bill_texts(duckdb_conn, ftp_conn):
    first_seen_at = datetime.datetime.now().strftime('%Y-%m-%d %H:%M')
    bill_texts_df = get_bill_texts(duckdb_conn, ftp_conn, OUT_DATASET_NAME, ENV)
    bill_texts_df['seen_at'] = first_seen_at
    dataframe_to_bigquery(bill_texts_df, 'lgover', OUT_DATASET_NAME, 'bill_texts', ENV, 'append')
    dataframe_to_duckdb(bill_texts_df, duckdb_conn, OUT_DATASET_NAME, 'bill_texts', ENV, 'append')
    log_bq_load('lgover', OUT_DATASET_NAME, 'bill_texts', ENV, 'append', duckdb_conn)
    logger.info("Bill texts data pipeline complete")

def committee_status(raw_bills_df, curr_committees_df=None, duckdb_conn = None):
    committees_df = get_committee_status_data(raw_bills_df)
    
    result_df = merge_with_current_data(committees_df, curr_committees_df)
    dataframe_to_bigquery(result_df, 'lgover', OUT_DATASET_NAME, 'committee_status', ENV, 'drop')
    dataframe_to_duckdb(result_df, duckdb_conn, OUT_DATASET_NAME, 'committee_status', ENV, 'drop')
    log_bq_load('lgover', OUT_DATASET_NAME, 'committees', ENV, 'drop', duckdb_conn)
    logger.info("Committees data pipeline complete")

def committee_hearing_videos(config, curr_committee_hearing_videos_df=None, duckdb_conn = None):
    committee_hearing_videos_df = get_committee_hearing_videos_data(config)
    
    result_df = merge_with_current_data(committee_hearing_videos_df, curr_committee_hearing_videos_df)
    dataframe_to_bigquery(result_df, 'lgover', OUT_DATASET_NAME, 'committee_hearing_videos', ENV, 'drop')
    dataframe_to_duckdb(result_df, duckdb_conn, OUT_DATASET_NAME, 'committee_hearing_videos', ENV, 'drop')
    log_bq_load('lgover', OUT_DATASET_NAME, 'committee_hearing_videos', ENV, 'drop', duckdb_conn)
    logger.info("Committee hearing videos data pipeline complete")

def committee_meetings(config, curr_committee_meetings_df=None, duckdb_conn = None):
    committee_meetings_df = get_committee_meetings_data(config)
    result_df = merge_with_current_data(committee_meetings_df, curr_committee_meetings_df)
    dataframe_to_bigquery(result_df, 'lgover', OUT_DATASET_NAME, 'committee_meetings', ENV, 'drop')
    dataframe_to_duckdb(result_df, duckdb_conn, OUT_DATASET_NAME, 'committee_meetings', ENV, 'drop')
    log_bq_load('lgover', OUT_DATASET_NAME, 'committee_meetings', ENV, 'drop', duckdb_conn)
    logger.info("Committee meetings data pipeline complete")

def committee_meeting_bills(config, curr_committee_meeting_bills_df=None, duckdb_conn = None):
    committee_meeting_bills_df = get_committee_meeting_bills_data(config)
    
    result_df = merge_with_current_data(committee_meeting_bills_df, curr_committee_meeting_bills_df)
    dataframe_to_bigquery(result_df, 'lgover', OUT_DATASET_NAME, 'committee_meeting_bills', ENV, 'drop')
    dataframe_to_duckdb(result_df, duckdb_conn, OUT_DATASET_NAME, 'committee_meeting_bills', ENV, 'drop')
    log_bq_load('lgover', OUT_DATASET_NAME, 'committee_meeting_bills', ENV, 'drop', duckdb_conn)
    logger.info("Committee meeting bills data pipeline complete")

def committee_meetings_links(config, curr_committee_meetings_df=None, duckdb_conn = None):

    committee_meetings_df = get_committee_meetings_data(config)
    
    result_df = merge_with_current_data(committee_meetings_df, curr_committee_meetings_df)
    dataframe_to_bigquery(result_df, 'lgover', OUT_DATASET_NAME, 'committee_meetings_links', ENV, 'drop')
    dataframe_to_duckdb(result_df, duckdb_conn, OUT_DATASET_NAME, 'committee_meetings_links', ENV, 'drop')
    log_bq_load('lgover', OUT_DATASET_NAME, 'committee_meetings_links', ENV, 'drop', duckdb_conn)
    logger.info("Committee meetings links data pipeline complete")

def companions(raw_bills_df, curr_companions_df=None, duckdb_conn = None):
    companions_df = get_companions_data(raw_bills_df)
    result_df = merge_with_current_data(companions_df, curr_companions_df)
    dataframe_to_bigquery(result_df, 'lgover', OUT_DATASET_NAME, 'companions', ENV, 'drop')
    dataframe_to_duckdb(result_df, duckdb_conn, OUT_DATASET_NAME, 'companions', ENV, 'drop')
    log_bq_load('lgover', OUT_DATASET_NAME, 'companions', ENV, 'drop', duckdb_conn)
    logger.info("Companions data pipeline complete")

def complete_bills_list(raw_bills_df, curr_complete_bills_list_df=None, duckdb_conn = None):

    complete_bills_list_df = get_complete_bills_list(raw_bills_df)
    
    result_df = merge_with_current_data(complete_bills_list_df, curr_complete_bills_list_df)
    dataframe_to_bigquery(result_df, 'lgover', OUT_DATASET_NAME, 'complete_bills_list', ENV, 'drop')
    dataframe_to_duckdb(result_df, duckdb_conn, OUT_DATASET_NAME, 'complete_bills_list', ENV, 'drop')
    log_bq_load('lgover', OUT_DATASET_NAME, 'complete_bills_list', ENV, 'drop', duckdb_conn)
    logger.info("Complete bills list data pipeline complete")

def links(raw_bills_df, config, curr_links_df=None, duckdb_conn = None):
    links_df = get_links_data(raw_bills_df,config)
    
    result_df = merge_with_current_data(links_df, curr_links_df)
    dataframe_to_bigquery(result_df, 'lgover', OUT_DATASET_NAME, 'links', ENV, 'drop')
    dataframe_to_duckdb(result_df, duckdb_conn, OUT_DATASET_NAME, 'links', ENV, 'drop')
    log_bq_load('lgover', OUT_DATASET_NAME, 'links', ENV, 'drop', duckdb_conn)
    logger.info("Links data pipeline complete")

def sponsors(raw_bills_df, curr_sponsors_df=None, duckdb_conn = None):
    sponsors_df = get_sponsors_data(raw_bills_df)
    
    result_df = merge_with_current_data(sponsors_df, curr_sponsors_df)
    dataframe_to_bigquery(result_df, 'lgover', OUT_DATASET_NAME, 'sponsors', ENV, 'drop')
    dataframe_to_duckdb(result_df, duckdb_conn, OUT_DATASET_NAME, 'sponsors', ENV, 'drop')
    log_bq_load('lgover', OUT_DATASET_NAME, 'sponsors', ENV, 'drop', duckdb_conn)
    logger.info("Sponsors data pipeline complete")

def subjects(raw_bills_df, curr_subjects_df=None, duckdb_conn = None):
    subjects_df = get_subjects_data(raw_bills_df)
    
    result_df = merge_with_current_data(subjects_df, curr_subjects_df)
    dataframe_to_bigquery(result_df, 'lgover', OUT_DATASET_NAME, 'subjects', ENV, 'drop')
    dataframe_to_duckdb(result_df, duckdb_conn, OUT_DATASET_NAME, 'subjects', ENV, 'drop')
    log_bq_load('lgover', OUT_DATASET_NAME, 'subjects', ENV, 'drop', duckdb_conn)
    logger.info("Subjects data pipeline complete")


def rss_feeds(config, curr_rss_df=None, duckdb_conn = None):
    rss_df = get_rss_data(config)
    
    result_df = merge_with_current_data(rss_df, curr_rss_df)
    dataframe_to_bigquery(result_df, 'lgover', OUT_DATASET_NAME, 'rss_feeds', ENV, 'drop')
    dataframe_to_duckdb(result_df, duckdb_conn, OUT_DATASET_NAME, 'rss_feeds', ENV, 'drop')
    log_bq_load('lgover', OUT_DATASET_NAME, 'rss_feeds', ENV, 'drop', duckdb_conn)
    logger.info("RSS feeds data pipeline complete")

def upcoming_committee_meetings(config, duckdb_conn):
    upcoming_meetings_df = get_upcoming_committee_meetings(config)
    upcoming_meetings_df['seen_at'] = datetime.datetime.now().strftime('%Y-%m-%d %H:%M')
    dataframe_to_bigquery(upcoming_meetings_df, 'lgover', OUT_DATASET_NAME, 'upcoming_committee_meetings', ENV, 'append')
    dataframe_to_duckdb(upcoming_meetings_df, duckdb_conn, OUT_DATASET_NAME, 'upcoming_committee_meetings', ENV, 'append')
    log_bq_load('lgover', OUT_DATASET_NAME, 'upcoming_committee_meetings', ENV, 'append', duckdb_conn)
    logger.info("Upcoming committee meetings data pipeline complete")

def upcoming_committee_meeting_bills(config, duckdb_conn):
    upcoming_meeting_bills_df = get_upcoming_committee_meeting_bills(config)
    upcoming_meeting_bills_df['seen_at'] = datetime.datetime.now().strftime('%Y-%m-%d %H:%M')
    dataframe_to_bigquery(upcoming_meeting_bills_df, 'lgover', OUT_DATASET_NAME, 'upcoming_committee_meeting_bills', ENV, 'append')
    dataframe_to_duckdb(upcoming_meeting_bills_df, duckdb_conn, OUT_DATASET_NAME, 'upcoming_committee_meeting_bills', ENV, 'append')
    log_bq_load('lgover', OUT_DATASET_NAME, 'upcoming_committee_meeting_bills', ENV, 'append', duckdb_conn)
    logger.info("Upcoming committee meeting bills data pipeline complete")

def versions(raw_bills_df, curr_versions_df=None, duckdb_conn = None):
    versions_df = get_versions_data(raw_bills_df)
    
    result_df = merge_with_current_data(versions_df, curr_versions_df)
    dataframe_to_bigquery(result_df, 'lgover', OUT_DATASET_NAME, 'versions', ENV, 'drop')
    dataframe_to_duckdb(result_df, duckdb_conn, OUT_DATASET_NAME, 'versions', ENV, 'drop')
    log_bq_load('lgover', OUT_DATASET_NAME, 'versions', ENV, 'drop', duckdb_conn)
    logger.info("Versions data pipeline complete")

def run_logs(start_time, end_time, notes, duckdb_conn = None):
    run_logs_df = pd.DataFrame({
        "start_time": [start_time],
        "end_time": [end_time],
        "notes": [notes]
    })
    dataframe_to_bigquery(run_logs_df, 'lgover', OUT_DATASET_NAME, 'run_logs', ENV, 'append')
    dataframe_to_duckdb(run_logs_df, duckdb_conn, OUT_DATASET_NAME, 'run_logs', ENV, 'append')
    log_bq_load('lgover', OUT_DATASET_NAME, 'run_logs', ENV, 'append', duckdb_conn)
    logger.info("Run logs data pipeline complete")

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
        logger.error(f"{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')} -- Failed to connect to FTP: {e}")
        raise e

    base_path = config['sources']['ftp']['base_path']
    leg_session = config['info']['LegSess']

    try:
        logger.info("datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')} -- Starting raw bills data extraction")
        raw_bills_df = get_raw_bills_data(base_path, leg_session, conn)
        logger.info("Raw bills data extraction complete")
    except Exception as e:
        logger.error(f"{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')} -- Failed to get raw bills data: {e}")
        raise e
    
    try:
        duckdb_conn = duckdb.connect(f"{DUCKDB_NAME}.duckdb")
    except Exception as e:
        logger.error(f"{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')} -- Failed to connect to DuckDB: {e}")
        raise e

    # curr_bills_df = get_current_table_data(duckdb_conn, 'lgover', OUT_DATASET_NAME, 'bills', ENV)
    # curr_authors_df = get_current_table_data(duckdb_conn, 'lgover', OUT_DATASET_NAME, 'authors', ENV) 
    # curr_subjects_df = get_current_table_data(duckdb_conn, 'lgover', OUT_DATASET_NAME, 'subjects', ENV)
    # curr_committee_status_df = get_current_table_data(duckdb_conn, 'lgover', OUT_DATASET_NAME, 'committee_status', ENV)
    # curr_versions_df = get_current_table_data(duckdb_conn, 'lgover', OUT_DATASET_NAME, 'versions', ENV)
    # curr_actions_df = get_current_table_data(duckdb_conn, 'lgover', OUT_DATASET_NAME, 'actions', ENV)
    # curr_companions_df = get_current_table_data(duckdb_conn, 'lgover', OUT_DATASET_NAME, 'companions', ENV)
    # curr_links_df = get_current_table_data(duckdb_conn, 'lgover', OUT_DATASET_NAME, 'links', ENV)
    # curr_committee_meetings_df = get_current_table_data(duckdb_conn, 'lgover', OUT_DATASET_NAME, 'committee_meetings', ENV)
    # curr_committee_meeting_bills_df = get_current_table_data(duckdb_conn, 'lgover', OUT_DATASET_NAME, 'committee_meeting_bills', ENV)
    # curr_bill_stages_df = get_current_table_data(duckdb_conn, 'lgover', OUT_DATASET_NAME, 'bill_stages', ENV)
    # curr_complete_bills_list_df = get_current_table_data(duckdb_conn, 'lgover', OUT_DATASET_NAME, 'complete_bills_list', ENV)
    # curr_rss_df = get_current_table_data(duckdb_conn, 'lgover', OUT_DATASET_NAME, 'rss_feeds', ENV)
    # curr_committee_hearing_videos_df = get_current_table_data(duckdb_conn, 'lgover', OUT_DATASET_NAME, 'committee_hearing_videos', ENV)

    # bills(raw_bills_df, curr_bills_df, duckdb_conn),
    # authors(raw_bills_df, curr_authors_df, duckdb_conn),
    # subjects(raw_bills_df, curr_subjects_df, duckdb_conn),
    # committee_status(raw_bills_df, curr_committee_status_df, duckdb_conn),
    # versions(raw_bills_df, curr_versions_df, duckdb_conn),
    # actions(raw_bills_df, curr_actions_df, duckdb_conn),
    # companions(raw_bills_df, curr_companions_df, duckdb_conn),
    # links(raw_bills_df, config, curr_links_df, duckdb_conn),
    # committee_meetings(config, curr_committee_meetings_df, duckdb_conn),
    # committee_meeting_bills(config, curr_committee_meeting_bills_df, duckdb_conn),
    # bill_stages(raw_bills_df, config, curr_bill_stages_df, duckdb_conn),
    # complete_bills_list(raw_bills_df, curr_complete_bills_list_df, duckdb_conn),
    # upcoming_committee_meetings(config, duckdb_conn),
    # upcoming_committee_meeting_bills(config, duckdb_conn),
    # committee_hearing_videos(config, curr_committee_hearing_videos_df, duckdb_conn),
    bill_texts(duckdb_conn, conn)
    # rss_feeds(config, curr_rss_df),
    run_logs(start_time, datetime.datetime.now(), "")

    duckdb_conn.close()

if __name__ == "__main__":
    tx_leg_pipeline()