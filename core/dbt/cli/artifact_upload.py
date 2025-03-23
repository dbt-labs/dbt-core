import os
import uuid
import zipfile

import requests
from pydantic import BaseSettings

from dbt.config.runtime import UnsetProfile, load_project
from dbt.constants import (
    CATALOG_FILENAME,
    MANIFEST_FILE_NAME,
    RUN_RESULTS_FILE_NAME,
    SOURCE_RESULT_FILE_NAME,
)
from dbt.events.types import ArtifactUploadSkipped, ArtifactUploadSuccess
from dbt.exceptions import DbtProjectError
from dbt_common.events.functions import fire_event
from dbt_common.exceptions import DbtBaseException as DbtException

EXECUTION_ARTIFACTS = [MANIFEST_FILE_NAME, RUN_RESULTS_FILE_NAME]

ARTIFACTS_TO_UPLOAD = {
    "retry": EXECUTION_ARTIFACTS,
    "clone": EXECUTION_ARTIFACTS,
    "build": EXECUTION_ARTIFACTS,
    "run": EXECUTION_ARTIFACTS,
    "run-operation": EXECUTION_ARTIFACTS,
    "seed": EXECUTION_ARTIFACTS,
    "snapshot": EXECUTION_ARTIFACTS,
    "test": EXECUTION_ARTIFACTS,
    "freshness": [MANIFEST_FILE_NAME, SOURCE_RESULT_FILE_NAME],
    "generate": [MANIFEST_FILE_NAME, CATALOG_FILENAME],
}


class ArtifactUploadConfig(BaseSettings):
    tenant: str
    DBT_CLOUD_TOKEN: str
    DBT_CLOUD_ACCOUNT_ID: str
    DBT_CLOUD_ENVIRONMENT_ID: str

    def get_ingest_url(self):
        return f"https://{self.tenant}.dbt.com/api/private/accounts/{self.DBT_CLOUD_ACCOUNT_ID}/environments/{self.DBT_CLOUD_ENVIRONMENT_ID}/ingests/"

    def get_complete_url(self, ingest_id):
        return f"{self.get_ingest_url()}{ingest_id}/complete/"

    def get_headers(self, invocation_id=None):
        if invocation_id is None:
            invocation_id = str(uuid.uuid4())
        return {
            "Accept": "application/json",
            "X-Invocation-Id": invocation_id,
            "Authorization": f"Token {self.DBT_CLOUD_TOKEN}",
        }


def upload_artifacts(project_dir, target_path, command):
    # Check if there are artifacts to upload for this command
    if command not in ARTIFACTS_TO_UPLOAD:
        fire_event(ArtifactUploadSkipped(msg=f"No artifacts to upload for command {command}"))
        return

    # read configurations
    try:
        project = load_project(
            project_dir, version_check=False, profile=UnsetProfile(), cli_vars=None
        )
        if not project.dbt_cloud or "tenant" not in project.dbt_cloud:
            raise DbtProjectError("dbt_cloud.tenant not found in dbt_project.yml")
        tenant = project.dbt_cloud["tenant"]
    except Exception as e:
        raise DbtProjectError(f"Error reading dbt_cloud.tenant from dbt_project.yml: {str(e)}")

    config = ArtifactUploadConfig(tenant=tenant)

    if not target_path:
        target_path = "target"

    # Create zip file with artifacts
    zip_file_name = "target.zip"
    with zipfile.ZipFile(zip_file_name, "w") as z:
        for artifact in ARTIFACTS_TO_UPLOAD[command]:
            z.write(os.path.join(target_path, artifact), artifact)

    # Step 1: Create ingest request
    response = requests.post(url=config.get_ingest_url(), headers=config.get_headers())
    if response.status_code != 200:
        raise DbtException(
            f"Error creating ingest request: {response.status_code}, {response.text}"
        )

    response_data = response.json()
    ingest_id = response_data["data"]["id"]
    upload_url = response_data["data"]["upload_url"]
    # Step 2: Upload the zip file to the provided URL
    with open(zip_file_name, "rb") as f:
        upload_response = requests.put(url=upload_url, data=f.read())
        if upload_response.status_code not in (200, 204):
            raise DbtException(
                f"Error uploading artifacts: {upload_response.status_code}, {upload_response.text}"
            )

    # Step 3: Mark the ingest as successful
    complete_response = requests.patch(
        url=config.get_complete_url(ingest_id),
        headers=config.get_headers(),
        json={"upload_status": "SUCCESS"},
    )

    if complete_response.status_code != 204:
        raise DbtException(
            f"Error completing ingest: {complete_response.status_code}, {complete_response.text}"
        )
    fire_event(ArtifactUploadSuccess(msg=f"command {command} completed successfully"))
