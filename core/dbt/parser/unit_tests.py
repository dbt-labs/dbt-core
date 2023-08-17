from dbt.contracts.graph.unit_tests import UnitTestSuite
from dbt.contracts.graph.model_config import NodeConfig
from dbt_extractor import py_extract_from_source  # type: ignore
from dbt.contracts.graph.nodes import (
    ModelNode,
    UnitTestNode,
    RefArgs,
)
from dbt.contracts.graph.manifest import Manifest
from dbt.parser.schemas import (
    SchemaParser,
    YamlBlock,
    ValidationError,
    JSONValidationError,
    YamlParseDictError,
    YamlReader,
)

from dbt.exceptions import (
    ParsingError,
)
from dbt.parser.search import FileBlock

from dbt.contracts.files import FileHash, SchemaSourceFile
from dbt.node_types import NodeType

from dbt.context.providers import generate_parse_exposure, get_rendered
from typing import List


def _is_model_node(node_id, manifest):
    return manifest.nodes[node_id].resource_type == NodeType.Model


class UnitTestManifestLoader:
    @classmethod
    def load(cls, manifest, root_project) -> Manifest:
        unit_test_manifest = Manifest(macros=manifest.macros)
        for file in manifest.files.values():
            block = FileBlock(file)
            if isinstance(file, SchemaSourceFile):
                dct = file.dict_from_yaml
                if "unit" in dct:
                    yaml_block = YamlBlock.from_file_block(block, dct)
                    # TODO: first root_project should be project, or we should only parse unit tests from root_project
                    schema_parser = SchemaParser(root_project, manifest, root_project)
                    parser = UnitTestParser(schema_parser, yaml_block, unit_test_manifest)
                    parser.parse()

        model_to_unit_tests = {}
        for node in unit_test_manifest.nodes.values():
            if isinstance(node, UnitTestNode):
                model_name = node.name.split("__")[0]
                if model_name not in model_to_unit_tests:
                    model_to_unit_tests[model_name] = [node.unique_id]
                else:
                    model_to_unit_tests[model_name].append(node.unique_id)

        for node in unit_test_manifest.nodes.values():
            if isinstance(node, UnitTestNode):
                # a unit test should depend on its fixture nodes, and any unit tests on its ref'd nodes
                for ref in node.refs:
                    for unique_id in model_to_unit_tests.get(ref.name, []):
                        node.depends_on.nodes.append(unique_id)
        return unit_test_manifest


class UnitTestParser(YamlReader):
    def __init__(self, schema_parser: SchemaParser, yaml: YamlBlock, unit_test_manifest: Manifest):
        super().__init__(schema_parser, yaml, "unit")
        self.yaml = yaml
        self.unit_test_manifest = unit_test_manifest

    def parse_unit_test(self, unparsed: UnitTestSuite):
        package_name = self.project.project_name
        path = self.yaml.path.relative_path
        # TODO: fix
        checksum = "f8f57c9e32eafaacfb002a4d03a47ffb412178f58f49ba58fd6f436f09f8a1d6"
        unit_test_node_ids = []
        for unit_test in unparsed.tests:
            input_nodes = []
            original_input_nodes = []
            for given in unit_test.given:
                original_input_node = self._get_original_input_node(given.input)
                original_input_nodes.append(original_input_node)

                original_input_node_columns = None
                if (
                    original_input_node.resource_type == NodeType.Model
                    and original_input_node.config.contract.enforced
                ):
                    original_input_node_columns = {
                        column.name: column.data_type for column in original_input_node.columns
                    }

                # TODO: package_name?
                input_name = f"{unparsed.model}__{unit_test.name}__{original_input_node.name}"
                input_unique_id = f"model.{package_name}.{input_name}"

                input_node = ModelNode(
                    raw_code=self._build_raw_code(given.rows, original_input_node_columns),
                    resource_type=NodeType.Model,
                    package_name=package_name,
                    path=path,
                    # original_file_path=self.yaml.path.original_file_path,
                    original_file_path=f"models_unit_test/{input_name}.sql",
                    unique_id=input_unique_id,
                    name=input_name,
                    config=NodeConfig(materialized="ephemeral"),
                    database=original_input_node.database,
                    schema=original_input_node.schema,
                    alias=original_input_node.alias,
                    fqn=input_unique_id.split("."),
                    checksum=FileHash(name="sha256", checksum=checksum),
                )
                input_nodes.append(input_node)

            actual_node = self.manifest.ref_lookup.perform_lookup(
                f"model.{package_name}.{unparsed.model}", self.manifest
            )
            unit_test_unique_id = f"unit.{package_name}.{unit_test.name}.{unparsed.model}"
            unit_test_node = UnitTestNode(
                resource_type=NodeType.Unit,
                package_name=package_name,
                path=f"{unparsed.model}.sql",
                # original_file_path=self.yaml.path.original_file_path,
                original_file_path=f"models_unit_test/{unparsed.model}.sql",
                unique_id=unit_test_unique_id,
                name=f"{unparsed.model}__{unit_test.name}",
                # TODO: merge with node config
                config=NodeConfig(materialized="unit", _extra={"expected_rows": unit_test.expect}),
                raw_code=actual_node.raw_code,
                database=actual_node.database,
                schema=actual_node.schema,
                alias=f"{unparsed.model}__{unit_test.name}",
                fqn=unit_test_unique_id.split("."),
                checksum=FileHash(name="sha256", checksum=checksum),
                attached_node=actual_node.unique_id,
                overrides=unit_test.overrides,
            )

            # TODO: generalize this method
            ctx = generate_parse_exposure(
                unit_test_node,  # type: ignore
                self.root_project,
                self.schema_parser.manifest,
                package_name,
            )
            get_rendered(unit_test_node.raw_code, ctx, unit_test_node, capture_macros=True)
            # unit_test_node now has a populated refs/sources

            # during compilation, refs will resolve to fixtures,
            # so add original input node ids to depends on explicitly to preserve lineage
            for original_input_node in original_input_nodes:
                # TODO: consider nulling out the original_input_node.raw_code
                self.unit_test_manifest.nodes[original_input_node.unique_id] = original_input_node
                unit_test_node.depends_on.nodes.append(original_input_node.unique_id)

            self.unit_test_manifest.nodes[unit_test_node.unique_id] = unit_test_node
            # self.unit_test_manifest.nodes[actual_node.unique_id] = actual_node
            for input_node in input_nodes:
                self.unit_test_manifest.nodes[input_node.unique_id] = input_node
                # should be a process_refs / process_sources call isntead?
                unit_test_node.depends_on.nodes.append(input_node.unique_id)
            unit_test_node_ids.append(unit_test_node.unique_id)

        # find out all nodes that are referenced but not in unittest manifest
        all_depends_on = set()
        for node_id in self.unit_test_manifest.nodes:
            if _is_model_node(node_id, self.unit_test_manifest):
                all_depends_on.update(self.unit_test_manifest.nodes[node_id].depends_on.nodes)  # type: ignore
        not_in_manifest = all_depends_on - set(self.unit_test_manifest.nodes.keys())

        # copy those node also over into unit_test_manifest
        for node_id in not_in_manifest:
            self.unit_test_manifest.nodes[node_id] = self.manifest.nodes[node_id]

    def parse(self):
        for data in self.get_key_dicts():
            try:
                UnitTestSuite.validate(data)
                unparsed = UnitTestSuite.from_dict(data)
            except (ValidationError, JSONValidationError) as exc:
                raise YamlParseDictError(self.yaml.path, self.key, data, exc)

            self.parse_unit_test(unparsed)

    def _build_raw_code(self, rows, column_name_to_data_types) -> str:
        return ("{{{{ get_fixture_sql({rows}, {column_name_to_data_types}) }}}}").format(
            rows=rows, column_name_to_data_types=column_name_to_data_types
        )

    def _get_original_input_node(self, input: str):
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
