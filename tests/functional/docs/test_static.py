import pytest

from dbt.clients.system import load_file_contents
from dbt.include.global_project import DOCS_INDEX_FILE_PATH
from dbt.tests.util import run_dbt
import os


class TestStaticGenerate:
    @pytest.fixture(scope="class")
    def models(self):
        return {"my_model.sql": "select 1 as fun"}

    def test_static_generated(self, project):
        run_dbt(["docs", "generate", "--static"])

        source_index_html = load_file_contents(DOCS_INDEX_FILE_PATH)

        target_index_html = load_file_contents(
            os.path.join(project.project_root, "target", "index.html")
        )

        # Validate index.html was copied correctly
        assert len(target_index_html) == len(source_index_html)
        assert hash(target_index_html) == hash(source_index_html)

        manifest_data = load_file_contents(
            os.path.join(project.project_root, "target", "manifest.json")
        )

        catalog_data = load_file_contents(
            os.path.join(project.project_root, "target", "catalog.json")
        )

        static_index_html = load_file_contents(
            os.path.join(project.project_root, "target", "static_index.html")
        )

        # Calculate expected static_index.html
        expected_static_index_html = source_index_html
        expected_static_index_html = expected_static_index_html.replace(
            '"MANIFEST.JSON INLINE DATA"', manifest_data
        )
        expected_static_index_html = expected_static_index_html.replace(
            '"CATALOG.JSON INLINE DATA"', catalog_data
        )

        # Validate static_index.html was generated correctly
        assert len(expected_static_index_html) == len(static_index_html)
        assert hash(expected_static_index_html) == hash(static_index_html)
