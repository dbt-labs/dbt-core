import csv
from io import StringIO
from typing import Any, Dict, List, Optional

from dbt.contracts.files import FixtureSourceFile
from dbt.contracts.graph.nodes import UnitTestFileFixture
from dbt.node_types import NodeType
from dbt.parser.base import Parser
from dbt.parser.search import FileBlock


class FixtureParser(Parser[UnitTestFileFixture]):
    @property
    def resource_type(self) -> NodeType:
        return NodeType.Fixture

    @classmethod
    def get_compiled_path(cls, block: FileBlock):
        # Is this necessary?
        return block.path.relative_path

    def generate_unique_id(self, resource_name: str, _: Optional[str] = None) -> str:
        return f"fixture.{self.project.project_name}.{resource_name}"

    def parse_file(self, file_block: FileBlock):
        assert isinstance(file_block.file, FixtureSourceFile)
        unique_id = self.generate_unique_id(file_block.name)

        if file_block.file.path.relative_path.endswith(".sql"):
            rows = file_block.file.contents  # type: ignore
        else:  # endswith('.csv')
            rows = self.get_rows(file_block.file.contents)  # type: ignore

        fixture = UnitTestFileFixture(
            name=file_block.name,
            path=file_block.file.path.relative_path,
            original_file_path=file_block.path.original_file_path,
            package_name=self.project.project_name,
            unique_id=unique_id,
            resource_type=NodeType.Fixture,
            rows=rows,
        )
        self.manifest.add_fixture(file_block.file, fixture)

    def get_rows(self, contents) -> List[Dict[str, Any]]:
        rows = []
        dummy_file = StringIO(contents)
        reader = csv.DictReader(dummy_file)
        for row in reader:
            rows.append(row)
        return rows
