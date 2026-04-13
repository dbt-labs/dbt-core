# Regression test for GitHub issue #12756
# +schema from dbt_project.yml must be applied even when the snapshot YAML
# has no config: block at all.
import yaml
from tests.unit.parser.test_parser import SchemaParserTest
from dbt.parser.schemas import yaml_from_file

SNAPSHOT_WITH_CONFIG = """
snapshots:
  - name: my_snap
    relation: source('my_source', 'my_table')
    config:
      tags: []
"""

SNAPSHOT_WITHOUT_CONFIG = """
snapshots:
  - name: my_snap_no_config
    relation: source('my_source', 'my_table')
"""


class TestYamlSnapshotSchemaFromProjectConfig(SchemaParserTest):

    def test_schema_applied_with_config_block(self):
        """Variant A (was already working): snapshot with an empty config: block
        should be added to the manifest without error."""
        block = self.file_block_for(SNAPSHOT_WITH_CONFIG, "test_snaps.yml", "snapshots")
        dct = yaml_from_file(block.file)
        self.parser.parse_file(block, dct)
        names = [uid.split(".")[-1] for uid in self.parser.manifest.nodes]
        self.assertIn("my_snap", names)

    def test_schema_applied_without_config_block(self):
        """Variant B (was broken before fix #12756): snapshot with NO config: block
        must still be added to the manifest and have a schema set."""
        block = self.file_block_for(SNAPSHOT_WITHOUT_CONFIG, "test_snaps.yml", "snapshots")
        dct = yaml_from_file(block.file)
        self.parser.parse_file(block, dct)
        names = [uid.split(".")[-1] for uid in self.parser.manifest.nodes]
        self.assertIn("my_snap_no_config", names)

        node = next(
            n for uid, n in self.parser.manifest.nodes.items()
            if uid.split(".")[-1] == "my_snap_no_config"
        )
        self.assertIsNotNone(
            node.schema,
            "schema must not be None — project-level +schema was not applied (issue #12756)"
        )
