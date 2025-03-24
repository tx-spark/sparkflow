import yaml
import datetime
import dlt
import duckdb

from utils import FtpConnection
from extract_functions import *

################################################################################
# CONFIGURATION
################################################################################

CONFIG_PATH = 'config.yaml'
PIPELINE_NAME = "texas_bills"
OUT_DATASET_NAME = "raw_bills"

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

    print(result_df.drop_duplicates())
    yield result_df.drop_duplicates()
        

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
    print('Getting companions!!!')


    result_df = merge_with_current_data(companions_df, curr_companions_df)
    print(result_df)
    yield result_df

@dlt.resource(write_disposition="replace")
def links(raw_bills_df, config, curr_links_df=None):
    links_df = get_links_data(raw_bills_df,config)
    
    result_df = merge_with_current_data(links_df, curr_links_df)
    print(result_df)
    yield result_df

# @dlt.resource(write_disposition="replace")
# def committee_meetings_links(config, curr_committee_meetings_df=None):

#     committee_meetings_df = get_committee_meetings_data(config)
    
#     result_df = merge_with_current_data(committee_meetings_df, curr_committee_meetings_df)
#     print(result_df)
#     yield result_df

@dlt.resource(write_disposition="replace")
def bill_stages(raw_bills_df, config,curr_bill_stages_df=None):
    bill_stages_df = get_bill_stages(config['sources']['html']['bill_stages'], raw_bills_df)
    
    result_df = merge_with_current_data(bill_stages_df, curr_bill_stages_df)
    print(result_df)
    yield result_df

@dlt.resource(write_disposition="replace")
def complete_bills_list(raw_bills_df,curr_complete_bills_list_df=None):

    complete_bills_list_df = get_complete_bills_list(raw_bills_df)
    
    result_df = merge_with_current_data(complete_bills_list_df, curr_complete_bills_list_df)
    print(result_df)
    yield result_df

@dlt.resource(write_disposition="replace")
def rss_feeds(config, curr_rss_df=None):
    rss_df = get_rss_data(config)
    
    result_df = merge_with_current_data(rss_df, curr_rss_df)
    print(result_df)
    yield result_df

@dlt.resource(write_disposition="append")
def upcoming_committee_meetings(config,):
    upcoming_meetings_df = get_upcoming_committee_meetings(config)
    upcoming_meetings_df['seen_at'] = datetime.datetime.now().strftime('%Y-%m-%d %H:%M')
    print(upcoming_meetings_df)
    yield upcoming_meetings_df

@dlt.resource(write_disposition="append")
def upcoming_committee_meeting_bills(config):
    upcoming_meeting_bills_df = get_upcoming_committee_meeting_bills(config)
    upcoming_meeting_bills_df['seen_at'] = datetime.datetime.now().strftime('%Y-%m-%d %H:%M')
    print(upcoming_meeting_bills_df)
    yield upcoming_meeting_bills_df

@dlt.resource(write_disposition="replace")
def committee_meetings(config, curr_committee_meetings_df=None):
    committee_meetings_df = get_committee_meetings_data(config)
    result_df = merge_with_current_data(committee_meetings_df, curr_committee_meetings_df)
    print(result_df)
    yield result_df

@dlt.resource(write_disposition="replace") 
def committee_meeting_bills(config, curr_committee_meeting_bills_df=None):
    committee_meeting_bills_df = get_committee_meeting_bills_data(config)
    
    result_df = merge_with_current_data(committee_meeting_bills_df, curr_committee_meeting_bills_df)
    print(result_df)
    yield result_df

@dlt.resource(write_disposition="replace") 
def committee_hearing_videos(config, curr_committee_hearing_videos_df=None):
    committee_hearing_videos_df = get_committee_hearing_videos_data(config)
    
    result_df = merge_with_current_data(committee_hearing_videos_df, curr_committee_hearing_videos_df)
    print(result_df)
    yield result_df

@dlt.resource(write_disposition="append")
def run_logs(start_time, end_time, notes):
    yield {
        "start_time": start_time,
        "end_time": end_time,
        "notes": notes
    }

################################################################################
# MAIN
################################################################################

if __name__ == "__main__":

    start_time = datetime.datetime.now()
    with open(CONFIG_PATH, "r", encoding="utf-8") as f:
        config = yaml.safe_load(f)

    pipeline = dlt.pipeline(
        destination="duckdb",
        dataset_name=OUT_DATASET_NAME,
        pipeline_name=PIPELINE_NAME,
        dev_mode=True,
    )

    conn = FtpConnection(config['sources']['ftp']['host'])

    base_path = config['sources']['ftp']['base_path']
    leg_session = config['info']['LegSess']
    #raw_bills_df = get_raw_bills_data(base_path, leg_session, conn)
    
    duckdb_conn = duckdb.connect(f"texas_bills.duckdb")

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

    duckdb_conn.close()

    pipeline.run([
        bills(raw_bills_df, curr_bills_df),
        authors(raw_bills_df, curr_authors_df),
        subjects(raw_bills_df, curr_subjects_df),
        committees(raw_bills_df, curr_committees_df),
        versions(raw_bills_df, curr_versions_df),
        actions(raw_bills_df, curr_actions_df),
        companions(raw_bills_df, curr_companions_df),
        links(raw_bills_df, config, curr_links_df),
        committee_meetings(config, curr_committee_meetings_df),
        committee_meeting_bills(config, curr_committee_meeting_bills_df),
        bill_stages(raw_bills_df, config, curr_bill_stages_df),
        complete_bills_list(raw_bills_df, curr_complete_bills_list_df),
        upcoming_committee_meetings(config),
        upcoming_committee_meeting_bills(config),
        committee_hearing_videos(config, curr_committee_hearing_videos_df),
        #rss_feeds(config, curr_rss_df),
        run_logs(start_time, datetime.datetime.now(), "")
    ])

