from prefect import flow, task
from parsons import GoogleBigQuery, Table
import os
import dotenv

from utilities import get_secret

# Load environment variables from .env file
dotenv.load_dotenv()

# Update with your project details or set as environment variables
PROJECT_ID = os.getenv("GCP_PROJECT_ID")
DATASET_ID = "tx_leg"  # Change
TABLE_ID = "parsons_test"  # Change


# @task(retries=3, retry_delay_seconds=10, log_prints=True)
def extract_data_with_parsons():
    """
    Extract data using Parsons connectors.
    Replace with your actual data extraction logic.
    """
    print("Extracting data using Parsons")

    # Example: Create sample data with Parsons Table
    # In a real scenario, you would use a Parsons connector like:
    # van = VAN(api_key=get_secret("van_api_key"))
    # activist_data = van.get_activists()

    # For demonstration, we'll create sample data
    data = [
        {"name": "John Smith", "party": "Democrat", "age": 42},
        {"name": "Sarah Johnson", "party": "Republican", "age": 35},
        {"name": "Miguel Rodriguez", "party": "Independent", "age": 29},
    ]

    # Convert to Parsons Table
    tbl = Table(data)
    print(f"Created Parsons table with {tbl.num_rows} rows")

    return tbl


# @task(retries=3, retry_delay_seconds=10, log_prints=True)
def transform_data(tbl: Table):
    """
    Transform the extracted data using Parsons Table methods.
    Replace with your actual transformation logic.
    """
    print("Transforming data with Parsons")

    # Example transformations using Parsons Table methods
    tbl.add_column("name_upper", lambda row: row["name"].upper())
    print(tbl)

    return tbl


# @task(retries=3, retry_delay_seconds=10, log_prints=True)
def load_data_with_parsons(tbl, env):
    """
    Load data to destination using Parsons BigQuery connector.
    Replace with your actual data loading logic.
    """

    # Add environment prefix for dev
    table_name = TABLE_ID
    dataset_name = DATASET_ID
    if env == "dev":
        dataset_name = f"dev_{dataset_name}"

    destination = f"{dataset_name}.{table_name}"
    table_name = f"{PROJECT_ID}.{dataset_name}.{table_name}"
    print(f"Loading data to {destination} using Parsons")

    # Initialize Parsons BigQuery connector

    gcp_creds = get_secret(secret_id="google_application_credentials")
    bq = GoogleBigQuery(app_creds=gcp_creds)

    # Create dataset if it doesn't exist
    bq.client.create_dataset(dataset=dataset_name, exists_ok=True)

    # Load data to BigQuery using Parsons
    bq.copy(
        tbl,
        table_name=table_name,
        if_exists="drop",  # Options: "fail", "append", "drop", or "truncate"
        tmp_gcs_bucket=os.getenv("GCS_TEMP_BUCKET"),  # Replace with your GCS bucket
    )

    print(f"Loaded {tbl.num_rows} rows to {destination}")


# @flow(name="Parsons Data Pipeline Example", log_prints=True)
def example_pipeline(env=None):
    """
    Example flow that extracts, transforms, and loads data using Parsons.

    Args:
        env: Optional environment override ('dev' or 'prod')
    """

    # If no parameter is provided, use a default
    if env is None:
        print("No environment specified, defaulting to dev")
        env = "dev"

    print(f"Running in environment: {env}")

    # Extract data using Parsons
    data = extract_data_with_parsons()

    # Transform data using Parsons Table methods
    transformed_data = transform_data(data)

    # Load data using Parsons BigQuery connector
    load_data_with_parsons(transformed_data, env)

    print(f"Pipeline completed successfully for {env} environment")


if __name__ == "__main__":
    example_pipeline()