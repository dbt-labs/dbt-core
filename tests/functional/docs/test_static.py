import pytest

from dbt.include.global_project import DOCS_INDEX_FILE_PATH
from dbt.tests.util import run_dbt
import os


class TestStaticGenerate:
    @pytest.fixture(scope="class")
    def models(self):
        return {"my_model.sql": "select 1 as fun"}

    def test_static_generated(self, project):
        run_dbt(["docs", "generate", "--static"])

        with open(DOCS_INDEX_FILE_PATH) as d:
            source_index_html = d.read()

        with open(os.path.join(project.project_root, "target", "index.html")) as d:
            target_index_html = d.read()

        # Validate index.html was copied correctly
        assert len(target_index_html) == len(source_index_html)
        assert hash(target_index_html) == hash(source_index_html)

        with open(os.path.join(project.project_root, "target", "manifest.json")) as d:
            manifest_data = d.read()

        with open(os.path.join(project.project_root, "target", "catalog.json")) as d:
            catalog_data = d.read()

        with open(os.path.join(project.project_root, "target", "static_index.html")) as d:
            static_index_html = d.read()

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
