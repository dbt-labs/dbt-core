import time
import uuid
import zipfile

import requests

import dbt.tracking
from dbt._pydantic_shim import BaseSettings  # type: ignore
from dbt.config.runtime import UnsetProfile, load_project
from dbt.constants import MANIFEST_FILE_NAME, RUN_RESULTS_FILE_NAME
from dbt.events.types import ArtifactUploadSkipped, ArtifactUploadSuccess
from dbt.exceptions import DbtProjectError
from dbt_common.events.functions import fire_event
from dbt_common.exceptions import DbtBaseException as DbtException

MAX_RETRIES = 3

EXECUTION_ARTIFACTS = [MANIFEST_FILE_NAME, RUN_RESULTS_FILE_NAME]

PRODUCED_ARTIFACTS_PATHS: set[str] = set()


# artifact paths calling this will be uploaded to dbt Cloud
def add_artifact_produced(artifact_path: str):
    PRODUCED_ARTIFACTS_PATHS.add(artifact_path)


class ArtifactUploadConfig(BaseSettings):
    tenant_hostname: str
    DBT_CLOUD_TOKEN: str
    DBT_CLOUD_ACCOUNT_ID: str
    DBT_CLOUD_ENVIRONMENT_ID: str

    def get_ingest_url(self):
        return f"https://{self.tenant_hostname}/api/private/accounts/{self.DBT_CLOUD_ACCOUNT_ID}/environments/{self.DBT_CLOUD_ENVIRONMENT_ID}/ingests/"

    def get_complete_url(self, ingest_id):
        return f"{self.get_ingest_url()}{ingest_id}/"

    def get_headers(self, invocation_id=None):
        if invocation_id is None:
            invocation_id = str(uuid.uuid4())
        return {
            "Accept": "application/json",
            "X-Invocation-Id": invocation_id,
            "Authorization": f"Token {self.DBT_CLOUD_TOKEN}",
        }


def _retry_with_backoff(operation_name, func, max_retries=MAX_RETRIES, retry_codes=None):
    """Execute a function with exponential backoff retry logic.

    Args:
        operation_name: Name of the operation for error messages
        func: Function to execute that returns (success, result)
        max_retries: Maximum number of retry attempts

    Returns:
        The result from the function if successful

    Raises:
        DbtException: If all retry attempts fail
    """
    if retry_codes is None:
        retry_codes = [500, 502, 503, 504]
    retry_delay = 1
    for attempt in range(max_retries + 1):
        try:
            success, result = func()
            if success:
                return result

            if result.status_code not in retry_codes:
                raise DbtException(f"Error {operation_name}: {result}")
            if attempt == max_retries:  # Last attempt
                raise DbtException(f"Error {operation_name}: {result}")
        except requests.RequestException as e:
            if attempt == max_retries:  # Last attempt
                raise DbtException(f"Error {operation_name}: {str(e)}")

        time.sleep(retry_delay)
        retry_delay *= 2  # exponential backoff


def upload_artifacts(project_dir, target_path, command):
    # Check if there are artifacts to upload for this command
    if not PRODUCED_ARTIFACTS_PATHS:
        fire_event(ArtifactUploadSkipped(msg="No artifacts to upload for current command"))
        return

    # read configurations
    try:
        project = load_project(
            project_dir, version_check=False, profile=UnsetProfile(), cli_vars=None
        )
        if not project.dbt_cloud or "tenant_hostname" not in project.dbt_cloud:
            raise DbtProjectError("dbt_cloud.tenant_hostname not found in dbt_project.yml")
        tenant_hostname = project.dbt_cloud["tenant_hostname"]
        if not tenant_hostname:
            raise DbtProjectError("dbt_cloud.tenant_hostname is empty in dbt_project.yml")
    except Exception as e:
        raise DbtProjectError(
            f"Error reading dbt_cloud.tenant_hostname from dbt_project.yml: {str(e)}"
        )

    config = ArtifactUploadConfig(tenant_hostname=tenant_hostname)

    if not target_path:
        target_path = "target"

    # Create zip file with artifacts
    zip_file_name = "target.zip"
    with zipfile.ZipFile(zip_file_name, "w") as z:
        for artifact_path in PRODUCED_ARTIFACTS_PATHS:
            z.write(artifact_path, artifact_path.split("/")[-1])

    # Step 1: Create ingest request with retry
    def create_ingest():
        response = requests.post(url=config.get_ingest_url(), headers=config.get_headers())
        return response.status_code == 200, response

    response = _retry_with_backoff("creating ingest request", create_ingest)
    response_data = response.json()
    ingest_id = response_data["data"]["id"]
    upload_url = response_data["data"]["upload_url"]

    # Step 2: Upload the zip file to the provided URL with retry
    with open(zip_file_name, "rb") as f:
        file_data = f.read()

        def upload_file():
            upload_response = requests.put(url=upload_url, data=file_data)
            return upload_response.status_code in (200, 204), upload_response

        _retry_with_backoff("uploading artifacts", upload_file)

    # Step 3: Mark the ingest as successful with retry
    def complete_ingest():
        complete_response = requests.patch(
            url=config.get_complete_url(ingest_id),
            headers=config.get_headers(),
            json={"upload_status": "SUCCESS"},
        )
        return complete_response.status_code == 204, complete_response

    _retry_with_backoff("completing ingest", complete_ingest)

    fire_event(ArtifactUploadSuccess(msg=f"command {command} completed successfully"))
    if dbt.tracking.active_user is not None:
        dbt.tracking.track_artifact_upload({"command": command})
    PRODUCED_ARTIFACTS_PATHS.clear()
