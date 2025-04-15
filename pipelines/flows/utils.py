from ftplib import FTP
from urllib.parse import urlparse
import io
import atexit
import gspread
import pandas as pd
import pdfplumber
import warnings
import logging
import datetime
from parsons import GoogleBigQuery, Table

import os
import subprocess
from google.cloud import secretmanager
import dotenv

from prefect import task
from prefect.cache_policies import NO_CACHE
logger = logging.getLogger(__name__)

################################################################################
# UTILITY CLASSES
################################################################################

class FtpConnection:
    def __init__(self, host, username=None, password=None, timeout=120):
        """
        Initialize FTP connection to specified host.
        
        Args:
            host: FTP server hostname
            username: Optional username for FTP login
            password: Optional password for FTP login
            timeout: Connection timeout in seconds (default 30)
        """
        self.host = host
        self.username = username # Is saving this secure? Does it matter?
        self.password = password
        self.timeout = timeout
        self.ftp = None
        self.connect()
        atexit.register(self.close) # Close the FTP connection when the program exits

    def connect(self):
        """Establish FTP connection and login"""
        self.ftp = FTP(self.host, timeout=self.timeout)
        if self.username and self.password:
            self.ftp.login(user=self.username, passwd=self.password)
        else:
            self.ftp.login() # Anonymous login

    def _retry_on_disconnect(self, operation):
        """
        Try an FTP operation and retry once with reconnect if it fails.
        
        Args:
            operation: Function that performs the FTP operation
            
        Returns:
            Result of the operation, or None/[] if it fails
        """
        try:
            return operation()
        except Exception: # TO DO: Add specific exceptions
            try:
                self.connect()
                return operation()
            except Exception as e:
                print(f"Error after relogin attempt: {e}")
                return None
            
    def get_data(self, url):
        """
        Retrieve data from a URL on the FTP server.
        
        Args:
            url: FTP URL to retrieve data from
            
        Returns:
            Retrieved data as a string, or None if retrieval failed
        """
        parsed = urlparse(url)
        if parsed.netloc != self.host:
            print(f"Warning: URL {url} is for different host than connection")
            return None
            
        def retrieve():
            buffer = io.BytesIO()
            self.ftp.retrbinary(f'RETR {parsed.path}', buffer.write)
            buffer.seek(0)
            return buffer.read().decode('utf-8')
            
        return self._retry_on_disconnect(retrieve)
        
    def ls(self, url):
        """
        List all files and folders at the given FTP URL path.
        
        Args:
            url: FTP URL path to list contents from
            
        Returns:
            List of URLs for all files/folders in the directory
        """
        parsed = urlparse(url)
        if parsed.netloc != self.host:
            print(f"Warning: URL {url} is for different host than connection")
            return []
            
        def list_dir():
            file_list = []
            self.ftp.retrlines(f'LIST {parsed.path}', file_list.append)

            urls = []
            for item in file_list:
                # Split into parts but preserve filename with spaces
                parts = item.split()
                    
                filename = ' '.join(parts[3:])
                if parsed.path.endswith('/'):
                    path = parsed.path + filename
                else:
                    path = parsed.path + '/' + filename
                url = f"ftp://{self.host}{path}"
                urls.append(url)
                
            return urls
            
        result = self._retry_on_disconnect(list_dir)
        return result if result is not None else []

    def get_pdf_text(self, pdf_url):
        """
        Download and extract text from a PDF on the FTP server.
        
        Args:
            pdf_url: URL of the PDF on the FTP server
            
        Returns:
            Extracted text from the PDF as a string, or None if extraction fails
        """
        
        # Suppress all warnings and logging
        warnings.filterwarnings('ignore')
        logging.getLogger('pdfminer').setLevel(logging.ERROR)
        
        parsed = urlparse(pdf_url)
        
        def retrieve():
            buffer = io.BytesIO()
            try:
                # Get raw bytes instead of trying to decode as text
                self.ftp.retrbinary(f'RETR {parsed.path}', buffer.write, rest=0)
                
                # Check if we actually got any data
                if buffer.getbuffer().nbytes == 0:
                    print(f"Error: No data received from {pdf_url}")
                    return None
                
                buffer.seek(0)
                
                # Read PDF with pdfplumber
                with pdfplumber.open(buffer) as pdf:
                    text = []
                    for page in pdf.pages:
                        try:
                            page_text = page.extract_text()
                            if page_text:
                                text.append(page_text)
                        except Exception as e:
                            print(f"Warning: Could not extract text from page: {e}")
                            continue
                
                return '\n'.join(text) if text else None
                
            except (TimeoutError, EOFError) as e:
                print(f"Connection timeout while downloading PDF: {e}")
                return None
            except Exception as e:
                print(f"Error extracting PDF text: {e}")
                return None
            finally:
                buffer.close()
                
        return self._retry_on_disconnect(retrieve)
    
    def close(self):
        """Close the FTP connection"""

        try:    
            if self.ftp:
                self.ftp.quit()
                self.ftp = None
        except BrokenPipeError:
            pass

################################################################################
# UTILITY FUNCTIONS
################################################################################

@task(retries=3, retry_delay_seconds=10, log_prints=True, cache_policy=NO_CACHE)
def write_df_to_gsheets(df, google_sheets_id, worksheet_name, minimize_to_rows=False, minimize_to_cols=False, replace_headers=True):
    """
    Write a pandas DataFrame to a Google Sheets worksheet.

    Args:
        df: pandas DataFrame to write
        google_sheets_id: ID of the target Google Sheet
        worksheet_name: Name of the worksheet to write to
        minimize_to_rows: If True, resize the worksheet to match the DataFrame rows.
        minimize_to_cols: If True, resize the worksheet to match the DataFrame columns.
        replace_headers: If True, include the DataFrame headers in the worksheet. If False, only write the values.

    The function will resize the worksheet to match the DataFrame dimensions (optionally)
    and write all data starting from cell A1.
    """
    google_sheets_df = df.copy()
    google_sheets_df.fillna('', inplace=True)

    gc = gspread.service_account()

    sh = gc.open_by_key(google_sheets_id)

    worksheet = sh.worksheet(worksheet_name)

    if replace_headers:
        data = [google_sheets_df.columns.tolist()] + google_sheets_df.values.tolist()
    else:
        data = google_sheets_df.values.tolist()

    if minimize_to_rows:
        num_rows = len(data)
        worksheet.resize(rows=num_rows)

    if minimize_to_cols:
        num_cols = len(data[0]) if data else 0 #handle empty dataframe case
        worksheet.resize(cols=num_cols)

    if replace_headers:
        worksheet.update('A1', data, value_input_option="USER_ENTERED")
    else:
        worksheet.update('A2', data, value_input_option="USER_ENTERED")

@task(retries=3, retry_delay_seconds=10, log_prints=True, cache_policy=NO_CACHE)
def read_gsheets_to_df(google_sheets_id, worksheet_name, header=0):
    """
    Reads a Google Sheets worksheet into a pandas DataFrame.

    Args:
        google_sheets_id: ID of the Google Sheet.
        worksheet_name: Name of the worksheet to read.
        header: Row number(s) to use as the column names, and the start of the data. 
                Defaults to 0 (first row).

    Returns:
        pandas DataFrame containing the data from the Google Sheet.
        Returns None if an error occurs.
    """
    try:
        gc = gspread.service_account()
        sh = gc.open_by_key(google_sheets_id)
        worksheet = sh.worksheet(worksheet_name)

        # Get all values from the worksheet
        data = worksheet.get_all_values()

        if not data:
            return pd.DataFrame()  # Return an empty DataFrame if the sheet is empty

        if header is not None:
            # Use the specified row(s) as headers
            headers = data[header]
            values = data[header + 1:]
            df = pd.DataFrame(values, columns=headers)
        else:
            # No headers, use default integer index
            df = pd.DataFrame(data)

        return df

    except Exception as e:
        print(f"An error occurred: {e}")
        return None

@task(retries=3, retry_delay_seconds=10, log_prints=True, cache_policy=NO_CACHE)
def dataframe_to_bigquery(df, project_id, dataset_id, table_id, env, write_disposition):
    """
    Load data to destination using Parsons BigQuery connector.
    Replace with your actual data loading logic.
    """

    if df is None:
        logger.error(f"{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')} -- DataFrame is None")
        raise ValueError("DataFrame is None")

    # Add environment prefix for dev
    table_name = table_id
    dataset_name = dataset_id
    if env == "dev":
        dataset_name = f"dev_{dataset_name}"

    destination = f"{dataset_name}.{table_name}"
    table_name = f"{project_id}.{dataset_name}.{table_name}"
    logger.info(f"{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')} -- Loading data to {destination} using Parsons")

    # Initialize Parsons BigQuery connector

    gcp_creds = get_secret(secret_id="google_application_credentials")
    bq = GoogleBigQuery(app_creds=gcp_creds)

    # Create dataset if it doesn't exist
    bq.client.create_dataset(dataset=dataset_name, exists_ok=True)

    # convert datetime columns to ISO format strings for BigQuery compatibility -- annoying, but I can't figure out how to get Parsons to read it in properly
    # TO DO: Fix code so this doesn't happen once more familiar with Parsons
    for col in df.columns:
        if df[col].dtype == 'datetime64[us]' or df[col].dtype == 'datetime64[ns]':
            df[col] = df[col].dt.strftime('%Y-%m-%d %H:%M:%S')

    tbl = Table.from_dataframe(df)

    # Load data to BigQuery using Parsons
    bq.copy(
        tbl,
        table_name=table_name,
        if_exists=write_disposition,  # Options: "fail", "append", "drop", or "truncate"
        tmp_gcs_bucket=os.getenv("GCS_TEMP_BUCKET"),  # Replace with your GCS bucket
    )

    logger.info(f"{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')} -- Loaded {tbl.num_rows} rows to {destination}")

def bigquery_to_df(project_id, dataset_id, table_id, env):
    gcp_creds = get_secret(secret_id="google_application_credentials")
    bq = GoogleBigQuery(app_creds=gcp_creds)

    if env == "dev":
        dataset_id = f"dev_{dataset_id}"

    query = f"""
    SELECT * FROM `{project_id}.{dataset_id}.{table_id}`
    """

    # Check if table exists
    try:
        bq_df = pd.DataFrame(bq.query(query))
        return bq_df
    except Exception as e:
        if "Not found: Table" in str(e):
            raise ValueError(f"Table {project_id}.{dataset_id}.{table_id} does not exist")
        else:
            raise e
        
def query_bq(query):
    gcp_creds = get_secret(secret_id="google_application_credentials")
    bq = GoogleBigQuery(app_creds=gcp_creds)
    # Check if table exists
    try:
        bq_df = pd.DataFrame(bq.query(query))
        return bq_df
    except Exception as e:
        if "Not found: Table" in str(e):
            raise ValueError(f"One or more of the tables queried do not exist.")
        else:
            raise e

@task(retries=1, retry_delay_seconds=10, log_prints=True, cache_policy=NO_CACHE)
def dataframe_to_duckdb(df, duckdb_conn, dataset_id, table_id, env,write_disposition):
    """
    Load data to destination using Parsons BigQuery connector.
    Replace with your actual data loading logic.
    """

    # Add environment prefix for dev
    table_name = table_id
    dataset_name = dataset_id
    if env == "dev":
        dataset_name = f"dev_{dataset_name}"

    destination = f"{dataset_name}.{table_name}"
    logger.info(f"{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')} -- Loading data to {destination} using DuckDB")

    # convert datetime columns to ISO format strings for BigQuery compatibility -- annoying, but I can't figure out how to get Parsons to read it in properly
    # Doing this in DuckDB too, so the tables are consistent
    # TO DO: Fix code so this doesn't happen once more familiar with Parsons
    for col in df.columns:
        if df[col].dtype == 'datetime64[us]' or df[col].dtype == 'datetime64[ns]':
            df[col] = df[col].dt.strftime('%Y-%m-%d %H:%M:%S')


    # Load data to DuckDB
    duckdb_conn.sql(f"CREATE SCHEMA IF NOT EXISTS {dataset_name}")
    if write_disposition.lower() == "drop":
        duckdb_conn.sql(f"DROP TABLE IF EXISTS {destination}")
        duckdb_conn.sql(f"CREATE TABLE {destination} AS SELECT * FROM df")
    elif write_disposition.lower() == "append":
        # check if table exists
        if duckdb_conn.sql(f"SELECT * FROM information_schema.tables WHERE table_schema = '{dataset_name}' AND table_name = '{table_name}'").df().empty:
            duckdb_conn.sql(f"CREATE TABLE {destination} AS SELECT * FROM df")
        else:
            duckdb_conn.sql(f"INSERT INTO {destination} SELECT * FROM df")
    elif write_disposition.lower() == "fail":
        raise ValueError(f"Table {destination} already exists")
    else:
        raise ValueError(f"Invalid write disposition: {write_disposition}")
    logger.info(f"{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')} -- Loaded {len(df)} rows to {destination}")

def log_bq_load(project_id, dataset_id, table_id, env, write_disposition, duckdb_conn, log_table_id = '_log_bq_load'):
    """
    Log the BigQuery load to a table in DuckDB and BigQuery.
    """
    current_time = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    upload_desc = [
        {
            'project_id':project_id,
            'dataset_id':dataset_id,
            'table_id':table_id,
            'write_disposition':write_disposition
        }
    ]


    upload_desc_df = pd.DataFrame(upload_desc)
    upload_desc_df['upload_time'] = current_time

    dataframe_to_duckdb(upload_desc_df, duckdb_conn, dataset_id, log_table_id, env, 'append')
    dataframe_to_bigquery(upload_desc_df, project_id, dataset_id, log_table_id, env, 'append')

def get_current_table_data(duckdb_conn, project_id, dataset_id, table_id, env, log_table_id = '_log_bq_load', use_cache = True):
    """
    Intution here is that I'm caching the data in DuckDB. First check the logs to see if the most recent loads match up.
    If they do, return the data from DuckDB. If they don't, return the table from BigQuery.

    If the table doesn't exist in either, return None
    """

    if env == "dev":
        dataset_id = f"dev_{dataset_id}"

    if not use_cache:
        try:
            bq_df = bigquery_to_df(project_id, dataset_id, table_id, env)
        except Exception as e:
            logger.error(f"{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')} -- Error querying BigQuery table {project_id}.{dataset_id}.{table_id}: {e}")
            return None
        return bq_df

    # check if logs for this table match duckdb table
    log_table_id = f"{dataset_id}.{log_table_id}"
    try:
        bq_query = f"""
        SELECT * FROM `{project_id}.{dataset_id}.{log_table_id}`
        where project_id = '{project_id}' and dataset_id = '{dataset_id}' and table_id = '{table_id}'
        order by PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S', upload_time) desc
        limit 1
        """
        bq_log_df = query_bq(bq_query)
    except Exception as e:
        logger.error(f"{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')} -- Error querying BigQuery logs: {e}")
        bq_log_df = None

    try:
        duckdb_query = f"""
        select * from {dataset_id}.{table_id}
        where project_id = '{project_id}' and dataset_id = '{dataset_id}' and table_id = '{table_id}'
        order by strftime('%Y-%m-%d %H:%M:%S', upload_time) desc
        """
        duckdb_log_df = duckdb_conn.sql(duckdb_query).df()

    except Exception as e:
        logger.error(f"{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')} -- Error querying DuckDB logs: {e}")
        duckdb_df = None

    if bq_log_df is None and duckdb_df is None:
        return None
    elif bq_log_df == duckdb_log_df:
        ## If they match, get the data from DuckDB
        try:
            duckdb_df = duckdb_conn.sql(f"SELECT * FROM {dataset_id}.{table_id}").df()
        except Exception as e:
            logger.error(f"{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')} -- Error querying DuckDB table: {e}")
            return None
        return duckdb_df
    else:
        ## If they don't match, get the data from BigQuery
        try:
            bq_df = bigquery_to_df(project_id, dataset_id, table_id, env)
        except Exception as e:
            logger.error(f"{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')} -- Error querying BigQuery table {project_id}.{dataset_id}.{table_id}: {e}")
            return None
        return bq_df
        
################################################################################
# FROM https://github.com/matthewkrausse/parsons-prefect-dbt-cloud-tutorial
################################################################################

def determine_git_environment():
    """
    Determine environment based on multiple signals.
    Returns 'prod' for main branch, 'dev' otherwise.
    """
    # 1. Check for explicit environment variable
    env_var = os.environ.get("ENVIRONMENT")
    if env_var:
        print(f"Using environment from ENVIRONMENT variable: {env_var}")
        return env_var.lower()

    # 2. Check for GitHub Actions environment
    if os.environ.get("GITHUB_REF") == "refs/heads/main":
        print("Detected GitHub main branch, using prod environment")
        return "prod"

    # 3. Try git branch as fallback for local development
    try:
        result = subprocess.run(
            ["git", "rev-parse", "--abbrev-ref", "HEAD"],
            capture_output=True,
            text=True,
            check=True,
            timeout=2,
        )
        branch = result.stdout.strip()
        print(f"Git branch detected: {branch}")

        if branch == "main":
            return "prod"
        else:
            return "dev"
    except Exception as e:
        print(f"Git detection failed: {e}")

    # 4. Default fallback
    print("Using default environment: dev")
    return "dev"


def get_secret(project_id="your-gcp-project-id", secret_id=None, version_id="latest"):
    """
    Access the secret value, first checking environment variables,
    then falling back to Google Secret Manager.

    Args:
        project_id: Google Cloud project ID
        secret_id: Name of the secret (For local env, use UPPERCASE in the .env file)
        version_id: Secret version, defaults to "latest"

    Returns:
        The secret value as a string
    """
    # Load environment variables from .env file
    dotenv.load_dotenv()

    # Check for project_id in environment variables
    env_project_id = os.environ.get("GCP_PROJECT_ID")
    if env_project_id:
        print("Using project_id from environment variables")
        project_id = env_project_id

    # Check if the secret exists as an environment variable
    env_value = os.environ.get(secret_id.upper())
    if env_value:
        print(f"Using {secret_id} from environment variables")
        return env_value

    print(
        f"Secret {secret_id} not found in environment, fetching from Google Secret Manager"
    )

    # Create the Secret Manager client
    client = secretmanager.SecretManagerServiceClient()

    # Build the resource name of the secret version
    name = f"projects/{project_id}/secrets/{secret_id}/versions/{version_id}"

    # Access the secret version
    response = client.access_secret_version(request={"name": name})

    # Return the secret payload
    payload = response.payload.data.decode("UTF-8")
    return payload