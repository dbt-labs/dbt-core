from dbt.tests.util import get_manifest, run_dbt, write_file
from tests.fixtures.jaffle_shop import JaffleShopProject

# Note: in an actual file (as opposed to a string that we write into a files)
# there would only be a single backslash.
sources_yml = """
sources:
  - name: something_else
    database: raw
    schema: jaffle_shop
    tables:
      - name: "\\"/test/orders\\""
      - name: customers
"""


class TestNameChars(JaffleShopProject):
    def test_quotes_in_table_names(self, project):
        # Write out a sources definition that includes a table name with quotes and a forward slash
        # Note: forward slashes are not legal in filenames in Linux (or Windows),
        # so we won't see forward slashes in model names, because they come from file names.
        write_file(sources_yml, project.project_root, "models", "sources.yml")
        manifest = run_dbt(["parse"])
        assert len(manifest.sources) == 2
        assert 'source.jaffle_shop.something_else."/test/orders"' in manifest.sources.keys()
        # We've written out the manifest.json artifact, we want to ensure
        # that it can be read in again (the json is valid).
        # Note: the key in the json actually looks like: "source.jaffle_shop.something_else.\"/test/orders\""
        new_manifest = get_manifest(project.project_root)
        assert new_manifest
        assert 'source.jaffle_shop.something_else."/test/orders"' in new_manifest.sources.keys()
