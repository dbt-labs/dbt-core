import os
import unittest
import uuid
from unittest import mock
from unittest.mock import MagicMock, call, patch

from dbt.cli.artifact_upload import ArtifactUploadConfig, upload_artifacts
from dbt.constants import MANIFEST_FILE_NAME, RUN_RESULTS_FILE_NAME
from dbt.exceptions import DbtProjectError
from dbt_common.exceptions import DbtBaseException


class TestArtifactUploadConfig(unittest.TestCase):
    def setUp(self):
        self.config = ArtifactUploadConfig(
            tenant="test-tenant",
            DBT_CLOUD_TOKEN="test-token",
            DBT_CLOUD_ACCOUNT_ID="1234",
            DBT_CLOUD_ENVIRONMENT_ID="5678",
        )
        self.test_invocation_id = str(uuid.uuid4())

    def test_get_ingest_url(self):
        expected_url = (
            "https://test-tenant.dbt.com/api/private/accounts/1234/environments/5678/ingests/"
        )
        self.assertEqual(self.config.get_ingest_url(), expected_url)

    def test_get_complete_url(self):
        ingest_id = "9012"
        expected_url = "https://test-tenant.dbt.com/api/private/accounts/1234/environments/5678/ingests/9012/complete/"
        self.assertEqual(self.config.get_complete_url(ingest_id), expected_url)

    def test_get_headers_with_invocation_id(self):
        expected_headers = {
            "Accept": "application/json",
            "X-Invocation-Id": self.test_invocation_id,
            "Authorization": "Token test-token",
        }
        self.assertEqual(
            self.config.get_headers(invocation_id=self.test_invocation_id),
            expected_headers,
        )

    def test_get_headers_without_invocation_id(self):
        with mock.patch("uuid.uuid4") as mock_uuid:
            mock_uuid.return_value = uuid.UUID("12345678-1234-1234-1234-123456789012")
            expected_headers = {
                "Accept": "application/json",
                "X-Invocation-Id": "12345678-1234-1234-1234-123456789012",
                "Authorization": "Token test-token",
            }
            self.assertEqual(self.config.get_headers(), expected_headers)


class TestUploadArtifacts(unittest.TestCase):
    def setUp(self):
        self.project_dir = "/fake/project/dir"
        self.target_path = "/fake/project/dir/target"
        self.command = "run"

        # Create patchers
        self.load_project_patcher = patch("dbt.cli.artifact_upload.load_project")
        self.zipfile_patcher = patch("dbt.cli.artifact_upload.zipfile.ZipFile")
        self.os_path_join_patcher = patch("dbt.cli.artifact_upload.os.path.join")
        self.requests_post_patcher = patch("dbt.cli.artifact_upload.requests.post")
        self.requests_put_patcher = patch("dbt.cli.artifact_upload.requests.put")
        self.requests_patch_patcher = patch("dbt.cli.artifact_upload.requests.patch")
        self.open_patcher = patch("builtins.open", mock.mock_open(read_data=b"test data"))
        self.fire_event_patcher = patch("dbt.cli.artifact_upload.fire_event")

        # Start patchers
        self.mock_load_project = self.load_project_patcher.start()
        self.mock_zipfile = self.zipfile_patcher.start()
        self.mock_os_path_join = self.os_path_join_patcher.start()
        self.mock_requests_post = self.requests_post_patcher.start()
        self.mock_requests_put = self.requests_put_patcher.start()
        self.mock_requests_patch = self.requests_patch_patcher.start()
        self.mock_open = self.open_patcher.start()
        self.mock_fire_event = self.fire_event_patcher.start()

        # Configure mocks
        self.mock_project = MagicMock()
        self.mock_project.dbt_cloud = {"tenant": "test-tenant"}
        self.mock_load_project.return_value = self.mock_project

        self.mock_os_path_join.side_effect = lambda path, file: f"{path}/{file}"

        # Mock response for POST request (create ingest)
        self.mock_post_response = MagicMock()
        self.mock_post_response.status_code = 200
        self.mock_post_response.json.return_value = {
            "data": {"id": "ingest123", "upload_url": "https://test-upload-url.com"}
        }
        self.mock_requests_post.return_value = self.mock_post_response

        # Mock response for PUT request (upload artifacts)
        self.mock_put_response = MagicMock()
        self.mock_put_response.status_code = 200
        self.mock_requests_put.return_value = self.mock_put_response

        # Mock response for PATCH request (complete ingest)
        self.mock_patch_response = MagicMock()
        self.mock_patch_response.status_code = 204
        self.mock_requests_patch.return_value = self.mock_patch_response

        # Setup the env var for the test
        self.original_token = os.environ.get("DBT_CLOUD_TOKEN")
        self.original_account_id = os.environ.get("DBT_CLOUD_ACCOUNT_ID")
        self.original_environment_id = os.environ.get("DBT_CLOUD_ENVIRONMENT_ID")

        os.environ["DBT_CLOUD_TOKEN"] = "test-token"
        os.environ["DBT_CLOUD_ACCOUNT_ID"] = "1234"
        os.environ["DBT_CLOUD_ENVIRONMENT_ID"] = "5678"

    def tearDown(self):
        self.load_project_patcher.stop()
        self.zipfile_patcher.stop()
        self.os_path_join_patcher.stop()
        self.requests_post_patcher.stop()
        self.requests_put_patcher.stop()
        self.requests_patch_patcher.stop()
        self.open_patcher.stop()
        self.fire_event_patcher.stop()
        if self.original_token:
            os.environ["DBT_CLOUD_TOKEN"] = self.original_token
        if self.original_account_id:
            os.environ["DBT_CLOUD_ACCOUNT_ID"] = self.original_account_id
        if self.original_environment_id:
            os.environ["DBT_CLOUD_ENVIRONMENT_ID"] = self.original_environment_id

    def test_upload_artifacts_skips_for_invalid_command(self):
        # Test with an invalid command
        upload_artifacts(self.project_dir, self.target_path, "invalid_command")

        # Verify that fire_event was called with ArtifactUploadSkipped
        self.mock_fire_event.assert_called_once()
        event_arg = self.mock_fire_event.call_args[0][0]
        self.assertEqual(event_arg.msg, "No artifacts to upload for command invalid_command")

        # Verify that no other methods were called
        self.mock_load_project.assert_not_called()
        self.mock_zipfile.assert_not_called()
        self.mock_requests_post.assert_not_called()

    def test_upload_artifacts_successful_upload(self):
        # Set up mock for ZipFile context manager
        mock_zipfile_instance = MagicMock()
        self.mock_zipfile.return_value.__enter__.return_value = mock_zipfile_instance

        # Call the function
        upload_artifacts(self.project_dir, self.target_path, self.command)

        # Verify the project was loaded
        self.mock_load_project.assert_called_once_with(
            self.project_dir, version_check=False, profile=mock.ANY, cli_vars=None
        )

        # Verify zip file was created and artifacts were added
        self.mock_zipfile.assert_called_once_with("target.zip", "w")
        expected_artifact_calls = [
            call(f"{self.target_path}/{MANIFEST_FILE_NAME}", MANIFEST_FILE_NAME),
            call(f"{self.target_path}/{RUN_RESULTS_FILE_NAME}", RUN_RESULTS_FILE_NAME),
        ]
        mock_zipfile_instance.write.assert_has_calls(expected_artifact_calls)

        # Verify API calls
        self.mock_requests_post.assert_called_once()
        self.mock_requests_put.assert_called_once_with(
            url="https://test-upload-url.com", data=b"test data"
        )
        self.mock_requests_patch.assert_called_once_with(
            url=mock.ANY, headers=mock.ANY, json={"upload_status": "SUCCESS"}
        )

        # Verify fire_event was called with ArtifactUploadSuccess
        success_event_call = [
            call
            for call in self.mock_fire_event.call_args_list
            if "completed successfully" in call[0][0].msg
        ]
        self.assertTrue(len(success_event_call) > 0)

    def test_upload_artifacts_default_target_path(self):
        # Call the function with target_path=None
        mock_zipfile_instance = MagicMock()
        self.mock_zipfile.return_value.__enter__.return_value = mock_zipfile_instance

        upload_artifacts(self.project_dir, None, self.command)

        # Verify the default target path was used
        expected_artifact_calls = [
            call(f"target/{MANIFEST_FILE_NAME}", MANIFEST_FILE_NAME),
            call(f"target/{RUN_RESULTS_FILE_NAME}", RUN_RESULTS_FILE_NAME),
        ]
        mock_zipfile_instance.write.assert_has_calls(expected_artifact_calls)

    def test_upload_artifacts_missing_tenant_config(self):
        # Set up project without dbt_cloud config
        self.mock_project.dbt_cloud = {}

        # Verify that the function raises an exception
        with self.assertRaises(DbtProjectError) as context:
            upload_artifacts(self.project_dir, self.target_path, self.command)

        self.assertIn("tenant not found", str(context.exception))

    def test_upload_artifacts_create_ingest_failure(self):
        # Set up failed POST response
        self.mock_post_response.status_code = 500
        self.mock_post_response.text = "Internal server error"

        # Verify that the function raises an exception
        with self.assertRaises(DbtBaseException) as context:
            upload_artifacts(self.project_dir, self.target_path, self.command)

        self.assertIn("Error creating ingest request", str(context.exception))

    def test_upload_artifacts_upload_failure(self):
        # Set up successful POST but failed PUT
        self.mock_put_response.status_code = 500
        self.mock_put_response.text = "Upload failed"

        # Verify that the function raises an exception
        with self.assertRaises(DbtBaseException) as context:
            upload_artifacts(self.project_dir, self.target_path, self.command)

        self.assertIn("Error uploading artifacts", str(context.exception))

    def test_upload_artifacts_complete_failure(self):
        # Set up successful POST and PUT but failed PATCH
        self.mock_patch_response.status_code = 500
        self.mock_patch_response.text = "Complete failed"

        # Verify that the function raises an exception
        with self.assertRaises(DbtBaseException) as context:
            upload_artifacts(self.project_dir, self.target_path, self.command)

        self.assertIn("Error completing ingest", str(context.exception))


if __name__ == "__main__":
    unittest.main()
