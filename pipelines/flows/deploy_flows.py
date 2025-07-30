"""
Script to deploy Parsons + Prefect flows to a work pool.
"""

from tx_leg import tx_leg_pipeline
# Import additional flows here

from prefect.docker import DockerImage
import os
import dotenv

from pipelines.utils.utils import determine_git_environment

dotenv.load_dotenv()

# Determine environment (prod or dev)
environment = os.environ.get("ENVIRONMENT")
if environment:
    print(f"Using environment from ENVIRONMENT variable: {environment}")
else:
    # Fall back to git-based detection if environment variable is not set
    environment = determine_git_environment()
    print(f"Determined environment from git: {environment}")

# Get Docker image configuration from environment variables
GCP_PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
GAR_LOCATION = os.environ.get("GAR_LOCATION", "us-central1")
GAR_REPOSITORY = os.environ.get("GAR_REPOSITORY", "prefect-images")
IMAGE_NAME = os.environ.get("IMAGE_NAME", "parsons-prefect-pipeline")

branch_name = os.environ.get("BRANCH_NAME", "local")
is_prod = environment == "prod"

print(
    f"Deploying with environment: {environment}, is_prod: {is_prod}, branch: {branch_name}"
)

# Set up environment-specific configurations
image_tag = os.environ.get("TAG", "latest")
full_image_name = f"{GAR_LOCATION}-docker.pkg.dev/{GCP_PROJECT_ID}/{GAR_REPOSITORY}/{IMAGE_NAME}-{environment}:{image_tag}"

print(f"Using Docker image: {full_image_name}")

# Configure work pool based on environment
work_pool_name = "prod-cloud-work-pool" if is_prod else "dev-cloud-work-pool"

# Define a list of flows to deploy
flows_to_deploy = [
    {
        "flow": tx_leg_pipeline,
        "name": "Pipeline to pull Texas Legislature Bills",
        "schedule": "0 2,11 * * *",  # Daily at 10 PM and 11 AM
    }
    # Add additional flows to deploy here
    # Example:
    # {
    #     "flow": voter_sync_pipeline,
    #     "name": "VAN Voter Sync Pipeline",
    #     "schedule": "0 12 * * *",  # Daily at noon
    # },
]

if __name__ == "__main__":
    for flow_config in flows_to_deploy:
        flow = flow_config["flow"]
        base_deployment_name = flow_config["name"]
        # Configure the schedule for the flow
        schedule = flow_config["schedule"] if is_prod else None

        # Create deployment name with environment prefix
        deployment_name = base_deployment_name
        if not is_prod:
            deployment_name = f"DEV - {deployment_name}"

        # Environment-specific parameters
        flow_parameters = {"env": environment}

        print(f"Deploying {deployment_name} with parameters: {flow_parameters}")

        flow.deploy(
            name=deployment_name,
            work_pool_name=work_pool_name,
            image=DockerImage(
                name=full_image_name,
                platform="linux/amd64",
            ),
            build=False,
            push=False,
            cron=schedule,
            parameters={"env": environment},
            tags=[environment, branch_name],
        )