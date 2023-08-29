from dbt.contracts.graph.model_config import NodeConfig
from dbt_extractor import py_extract_from_source  # type: ignore
from dbt.contracts.graph.unparsed import UnparsedUnitTestSuite
from dbt.contracts.graph.nodes import (
    ModelNode,
    UnitTestNode,
    RefArgs,
    UnitTestDefinition,
    DependsOn,
)
from dbt.config import RuntimeConfig
from dbt.contracts.graph.manifest import Manifest
from dbt.parser.schemas import (
    SchemaParser,
    YamlBlock,
    ValidationError,
    JSONValidationError,
    YamlParseDictError,
    YamlReader,
)
from dbt.node_types import NodeType

from dbt.exceptions import (
    ParsingError,
)

from dbt.contracts.files import FileHash
from dbt.graph import UniqueId

from dbt.context.providers import generate_parse_exposure, get_rendered
from typing import List, Set
from dbt.utils import get_pseudo_test_path


def _is_model_node(node_id, manifest):
    return manifest.nodes[node_id].resource_type == NodeType.Model


class UnitTestManifestLoader:
    def __init__(self, manifest, root_project, selected) -> None:
        self.manifest: Manifest = manifest
        self.root_project: RuntimeConfig = root_project
        # selected comes from the initial selection against a "regular" manifest
        self.selected: Set[UniqueId] = selected
        self.unit_test_manifest = Manifest(macros=manifest.macros)

    def load(self) -> Manifest:
        for unique_id in self.selected:
            unit_test_case = self.manifest.unit_tests[unique_id]
            self.parse_unit_test_case(unit_test_case)

        return self.unit_test_manifest

    def parse_unit_test_case(self, test_case: UnitTestDefinition):
        package_name = self.root_project.project_name

        # Create unit test node based on the "actual" tested node
        actual_node = self.manifest.ref_lookup.perform_lookup(
            f"model.{package_name}.{test_case.model}", self.manifest
        )

        # Create UnitTestNode based on model being tested. Since selection has
        # already been done, we don't have to care about fields that are necessary
        # for selection.
        # Note: no depends_on, that's added later using input nodes
        name = f"{test_case.model}__{test_case.name}"
        unit_test_node = UnitTestNode(
            name=name,
            resource_type=NodeType.Unit,
            package_name=package_name,
            path=get_pseudo_test_path(name, test_case.original_file_path),
            original_file_path=test_case.original_file_path,
            unique_id=test_case.unique_id,
            config=NodeConfig(materialized="unit", _extra={"expected_rows": test_case.expect}),
            raw_code=actual_node.raw_code,
            database=actual_node.database,
            schema=actual_node.schema,
            alias=name,
            fqn=test_case.unique_id.split("."),
            checksum=FileHash.empty(),
            attached_node=actual_node.unique_id,
            overrides=test_case.overrides,
        )

        # TODO: generalize this method
        ctx = generate_parse_exposure(
            unit_test_node,  # type: ignore
            self.root_project,
            self.manifest,
            package_name,
        )
        get_rendered(unit_test_node.raw_code, ctx, unit_test_node, capture_macros=True)
        # unit_test_node now has a populated refs/sources

        self.unit_test_manifest.nodes[unit_test_node.unique_id] = unit_test_node

        # Now create input_nodes for the test inputs
        """
        given:
          - input: ref('my_model_a')
            rows: []
          - input: ref('my_model_b')
            rows:
              - {id: 1, b: 2}
              - {id: 2, b: 2}
        """
        # Add the model "input" nodes, consisting of all referenced models in the unit test.
        # This creates a model for every input in every test, so there may be multiple
        # input models substituting for the same input ref'd model.
        for given in test_case.given:
            # extract the original_input_node from the ref in the "input" key of the given list
            original_input_node = self._get_original_input_node(given.input)

            original_input_node_columns = None
            if (
                original_input_node.resource_type == NodeType.Model
                and original_input_node.config.contract.enforced
            ):
                original_input_node_columns = {
                    column.name: column.data_type for column in original_input_node.columns
                }

            # TODO: package_name?
            input_name = f"{test_case.model}__{test_case.name}__{original_input_node.name}"
            input_unique_id = f"model.{package_name}.{input_name}"

            input_node = ModelNode(
                raw_code=self._build_raw_code(given.rows, original_input_node_columns),
                resource_type=NodeType.Model,
                package_name=package_name,
                path=original_input_node.path,
                original_file_path=original_input_node.original_file_path,
                unique_id=input_unique_id,
                name=input_name,
                config=NodeConfig(materialized="ephemeral"),
                database=original_input_node.database,
                schema=original_input_node.schema,
                alias=original_input_node.alias,
                fqn=input_unique_id.split("."),
                checksum=FileHash.empty(),
            )
            self.unit_test_manifest.nodes[input_node.unique_id] = input_node
            # Add unique ids of input_nodes to depends_on
            unit_test_node.depends_on.nodes.append(input_node.unique_id)

    def _build_raw_code(self, rows, column_name_to_data_types) -> str:
        return ("{{{{ get_fixture_sql({rows}, {column_name_to_data_types}) }}}}").format(
            rows=rows, column_name_to_data_types=column_name_to_data_types
        )

    def _get_original_input_node(self, input: str):
        """input: ref('my_model_a')"""
        # Exract the ref or sources
        statically_parsed = py_extract_from_source(f"{{{{ {input} }}}}")
        if statically_parsed["refs"]:
            # set refs and sources on the node object
            refs: List[RefArgs] = []
            for ref in statically_parsed["refs"]:
                name = ref.get("name")
                package = ref.get("package")
                version = ref.get("version")
                refs.append(RefArgs(name, package, version))
                # TODO: disabled lookup, versioned lookup, public models
                original_input_node = self.manifest.ref_lookup.find(
                    name, package, version, self.manifest
                )
        elif statically_parsed["sources"]:
            input_package_name, input_source_name = statically_parsed["sources"][0]
            original_input_node = self.manifest.source_lookup.find(
                input_source_name, input_package_name, self.manifest
            )
        else:
            raise ParsingError("given input must be ref or source")

        return original_input_node


class UnitTestParser(YamlReader):
    def __init__(self, schema_parser: SchemaParser, yaml: YamlBlock):
        super().__init__(schema_parser, yaml, "unit")
        self.schema_parser = schema_parser
        self.yaml = yaml

    def parse(self):
        for data in self.get_key_dicts():
            try:
                UnparsedUnitTestSuite.validate(data)
                unparsed = UnparsedUnitTestSuite.from_dict(data)
            except (ValidationError, JSONValidationError) as exc:
                raise YamlParseDictError(self.yaml.path, self.key, data, exc)
            package_name = self.project.project_name

            actual_node = self.manifest.ref_lookup.perform_lookup(
                f"model.{package_name}.{unparsed.model}", self.manifest
            )
            if not actual_node:
                raise ParsingError(
                    "Unable to find model {unparsed.model} for unit tests in {self.yaml.path.original_file_path}"
                )
            for test in unparsed.tests:
                unit_test_case_unique_id = f"unit.{package_name}.{test.name}.{unparsed.model}"
                unit_test_case = UnitTestDefinition(
                    name=test.name,
                    model=unparsed.model,
                    resource_type=NodeType.Unit,
                    package_name=package_name,
                    path=self.yaml.path.relative_path,
                    original_file_path=self.yaml.path.original_file_path,
                    unique_id=unit_test_case_unique_id,
                    attached_node=actual_node.unique_id,
                    given=test.given,
                    expect=test.expect,
                    description=test.description,
                    overrides=test.overrides,
                    depends_on=DependsOn(nodes=[actual_node.unique_id]),
                    fqn=[package_name, test.name],
                )
                self.manifest.add_unit_test(self.yaml.file, unit_test_case)
