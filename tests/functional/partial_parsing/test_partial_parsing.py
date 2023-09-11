import pytest
from unittest import mock

from dbt.tests.util import run_dbt, get_manifest, write_file, rm_file, run_dbt_and_capture
from dbt.tests.fixtures.project import write_project_files
from tests.functional.partial_parsing.fixtures import (
    model_one_sql,
    model_two_sql,
    models_schema1_yml,
    models_schema2_yml,
    models_schema2b_yml,
    model_three_sql,
    model_three_modified_sql,
    model_four1_sql,
    model_four2_sql,
    models_schema4_yml,
    models_schema4b_yml,
    models_schema3_yml,
    my_macro_sql,
    my_macro2_sql,
    macros_yml,
    empty_schema_yml,
    empty_schema_with_version_yml,
    model_three_disabled_sql,
    model_three_disabled2_sql,
    raw_customers_csv,
    customers_sql,
    sources_tests1_sql,
    schema_sources1_yml,
    schema_sources2_yml,
    schema_sources3_yml,
    schema_sources4_yml,
    schema_sources5_yml,
    customers1_md,
    customers2_md,
    test_macro_sql,
    my_test_sql,
    test_macro2_sql,
    my_analysis_sql,
    sources_tests2_sql,
    local_dependency__dbt_project_yml,
    local_dependency__models__schema_yml,
    local_dependency__models__model_to_import_sql,
    local_dependency__macros__dep_macro_sql,
    local_dependency__seeds__seed_csv,
    schema_models_c_yml,
    model_a_sql,
    model_b_sql,
    macros_schema_yml,
    custom_schema_tests1_sql,
    custom_schema_tests2_sql,
    ref_override_sql,
    ref_override2_sql,
    gsm_override_sql,
    gsm_override2_sql,
    orders_sql,
    orders_downstream_sql,
    snapshot_sql,
    snapshot2_sql,
    generic_schema_yml,
    generic_test_sql,
    generic_test_schema_yml,
    generic_test_edited_sql,
    groups_schema_yml_one_group,
    groups_schema_yml_two_groups,
    groups_schema_yml_two_groups_edited,
    groups_schema_yml_one_group_model_in_group2,
    groups_schema_yml_two_groups_private_orders_valid_access,
    groups_schema_yml_two_groups_private_orders_invalid_access,
)

from dbt.exceptions import CompilationError, ParsingError
from dbt.contracts.files import ParseFileType
from dbt.contracts.results import TestStatus
from dbt.plugins.manifest import PluginNodes, ModelNodeArgs

import re
import os

os.environ["DBT_PP_TEST"] = "true"


def normalize(path):
    return os.path.normcase(os.path.normpath(path))


class TestModels:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "model_one.sql": model_one_sql,
        }

    def test_pp_models(self, project):
        # initial run
        # run_dbt(['clean'])
        results = run_dbt(["run"])
        assert len(results) == 1

        # add a model file
        write_file(model_two_sql, project.project_root, "models", "model_two.sql")
        results = run_dbt(["--partial-parse", "run"])
        assert len(results) == 2

        # add a schema file
        write_file(models_schema1_yml, project.project_root, "models", "schema.yml")
        results = run_dbt(["--partial-parse", "run"])
        assert len(results) == 2
        manifest = get_manifest(project.project_root)
        assert "model.test.model_one" in manifest.nodes
        model_one_node = manifest.nodes["model.test.model_one"]
        assert model_one_node.description == "The first model"
        assert model_one_node.patch_path == "test://" + normalize("models/schema.yml")

        # add a model and a schema file (with a test) at the same time
        write_file(models_schema2_yml, project.project_root, "models", "schema.yml")
        write_file(model_three_sql, project.project_root, "models", "model_three.sql")
        results = run_dbt(["--partial-parse", "test"], expect_pass=False)
        assert len(results) == 1
        manifest = get_manifest(project.project_root)
        project_files = [f for f in manifest.files if f.startswith("test://")]
        assert len(project_files) == 4
        model_3_file_id = "test://" + normalize("models/model_three.sql")
        assert model_3_file_id in manifest.files
        model_three_file = manifest.files[model_3_file_id]
        assert model_three_file.parse_file_type == ParseFileType.Model
        assert type(model_three_file).__name__ == "SourceFile"
        model_three_node = manifest.nodes[model_three_file.nodes[0]]
        schema_file_id = "test://" + normalize("models/schema.yml")
        assert model_three_node.patch_path == schema_file_id
        assert model_three_node.description == "The third model"
        schema_file = manifest.files[schema_file_id]
        assert type(schema_file).__name__ == "SchemaSourceFile"
        assert len(schema_file.tests) == 1
        tests = schema_file.get_all_test_ids()
        assert tests == ["test.test.unique_model_three_id.6776ac8160"]
        unique_test_id = tests[0]
        assert unique_test_id in manifest.nodes

        # modify model sql file, ensure description still there
        write_file(model_three_modified_sql, project.project_root, "models", "model_three.sql")
        results = run_dbt(["--partial-parse", "run"])
        manifest = get_manifest(project.project_root)
        model_id = "model.test.model_three"
        assert model_id in manifest.nodes
        model_three_node = manifest.nodes[model_id]
        assert model_three_node.description == "The third model"

        # Change the model 3 test from unique to not_null
        write_file(models_schema2b_yml, project.project_root, "models", "schema.yml")
        results = run_dbt(["--partial-parse", "test"], expect_pass=False)
        manifest = get_manifest(project.project_root)
        schema_file_id = "test://" + normalize("models/schema.yml")
        schema_file = manifest.files[schema_file_id]
        tests = schema_file.get_all_test_ids()
        assert tests == ["test.test.not_null_model_three_id.3162ce0a6f"]
        not_null_test_id = tests[0]
        assert not_null_test_id in manifest.nodes.keys()
        assert unique_test_id not in manifest.nodes.keys()
        assert len(results) == 1

        # go back to previous version of schema file, removing patch, test, and model for model three
        write_file(models_schema1_yml, project.project_root, "models", "schema.yml")
        rm_file(project.project_root, "models", "model_three.sql")
        results = run_dbt(["--partial-parse", "run"])
        assert len(results) == 2

        # remove schema file, still have 3 models
        write_file(model_three_sql, project.project_root, "models", "model_three.sql")
        rm_file(project.project_root, "models", "schema.yml")
        results = run_dbt(["--partial-parse", "run"])
        assert len(results) == 3
        manifest = get_manifest(project.project_root)
        schema_file_id = "test://" + normalize("models/schema.yml")
        assert schema_file_id not in manifest.files
        project_files = [f for f in manifest.files if f.startswith("test://")]
        assert len(project_files) == 3

        # Put schema file back and remove a model
        # referred to in schema file
        write_file(models_schema2_yml, project.project_root, "models", "schema.yml")
        rm_file(project.project_root, "models", "model_three.sql")
        with pytest.raises(CompilationError):
            results = run_dbt(["--partial-parse", "--warn-error", "run"])

        # Put model back again
        write_file(model_three_sql, project.project_root, "models", "model_three.sql")
        results = run_dbt(["--partial-parse", "run"])
        assert len(results) == 3

        # Add model four refing model three
        write_file(model_four1_sql, project.project_root, "models", "model_four.sql")
        results = run_dbt(["--partial-parse", "run"])
        assert len(results) == 4

        # Remove model_three and change model_four to ref model_one
        # and change schema file to remove model_three
        rm_file(project.project_root, "models", "model_three.sql")
        write_file(model_four2_sql, project.project_root, "models", "model_four.sql")
        write_file(models_schema1_yml, project.project_root, "models", "schema.yml")
        results = run_dbt(["--partial-parse", "run"])
        assert len(results) == 3

        # Remove model four, put back model three, put back schema file
        write_file(model_three_sql, project.project_root, "models", "model_three.sql")
        write_file(models_schema2_yml, project.project_root, "models", "schema.yml")
        rm_file(project.project_root, "models", "model_four.sql")
        results = run_dbt(["--partial-parse", "run"])
        assert len(results) == 3

        # disable model three in the schema file
        write_file(models_schema4_yml, project.project_root, "models", "schema.yml")
        results = run_dbt(["--partial-parse", "run"])
        assert len(results) == 2

        # update enabled config to be true for model three in the schema file
        write_file(models_schema4b_yml, project.project_root, "models", "schema.yml")
        results = run_dbt(["--partial-parse", "run"])
        assert len(results) == 3

        # disable model three in the schema file again
        write_file(models_schema4_yml, project.project_root, "models", "schema.yml")
        results = run_dbt(["--partial-parse", "run"])
        assert len(results) == 2

        # remove disabled config for model three in the schema file to check it gets enabled
        write_file(models_schema4b_yml, project.project_root, "models", "schema.yml")
        results = run_dbt(["--partial-parse", "run"])
        assert len(results) == 3

        # Add a macro
        write_file(my_macro_sql, project.project_root, "macros", "my_macro.sql")
        results = run_dbt(["--partial-parse", "run"])
        assert len(results) == 3
        manifest = get_manifest(project.project_root)
        macro_id = "macro.test.do_something"
        assert macro_id in manifest.macros

        # Modify the macro
        write_file(my_macro2_sql, project.project_root, "macros", "my_macro.sql")
        results = run_dbt(["--partial-parse", "run"])
        assert len(results) == 3

        # Add a macro patch
        write_file(models_schema3_yml, project.project_root, "models", "schema.yml")
        results = run_dbt(["--partial-parse", "run"])
        assert len(results) == 3

        # Remove the macro
        rm_file(project.project_root, "macros", "my_macro.sql")
        with pytest.raises(CompilationError):
            results = run_dbt(["--partial-parse", "--warn-error", "run"])

        # put back macro file, got back to schema file with no macro
        # add separate macro patch schema file
        write_file(models_schema2_yml, project.project_root, "models", "schema.yml")
        write_file(my_macro_sql, project.project_root, "macros", "my_macro.sql")
        write_file(macros_yml, project.project_root, "macros", "macros.yml")
        results = run_dbt(["--partial-parse", "run"])

        # delete macro and schema file
        rm_file(project.project_root, "macros", "my_macro.sql")
        rm_file(project.project_root, "macros", "macros.yml")
        results = run_dbt(["--partial-parse", "run"])
        assert len(results) == 3

        # Add an empty schema file
        write_file(empty_schema_yml, project.project_root, "models", "eschema.yml")
        results = run_dbt(["--partial-parse", "run"])
        assert len(results) == 3

        # Add version to empty schema file
        write_file(empty_schema_with_version_yml, project.project_root, "models", "eschema.yml")
        results = run_dbt(["--partial-parse", "run"])
        assert len(results) == 3

        # Disable model_three
        write_file(model_three_disabled_sql, project.project_root, "models", "model_three.sql")
        results = run_dbt(["--partial-parse", "run"])
        assert len(results) == 2
        manifest = get_manifest(project.project_root)
        model_id = "model.test.model_three"
        assert model_id in manifest.disabled
        assert model_id not in manifest.nodes

        # Edit disabled model three
        write_file(model_three_disabled2_sql, project.project_root, "models", "model_three.sql")
        results = run_dbt(["--partial-parse", "run"])
        assert len(results) == 2
        manifest = get_manifest(project.project_root)
        model_id = "model.test.model_three"
        assert model_id in manifest.disabled
        assert model_id not in manifest.nodes

        # Remove disabled from model three
        write_file(model_three_sql, project.project_root, "models", "model_three.sql")
        results = run_dbt(["--partial-parse", "run"])
        assert len(results) == 3
        manifest = get_manifest(project.project_root)
        model_id = "model.test.model_three"
        assert model_id in manifest.nodes
        assert model_id not in manifest.disabled


class TestSources:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "model_one.sql": model_one_sql,
        }

    def test_pp_sources(self, project):
        # initial run
        write_file(raw_customers_csv, project.project_root, "seeds", "raw_customers.csv")
        write_file(sources_tests1_sql, project.project_root, "macros", "tests.sql")
        results = run_dbt(["run"])
        assert len(results) == 1

        # Partial parse running 'seed'
        run_dbt(["--partial-parse", "seed"])
        manifest = get_manifest(project.project_root)
        seed_file_id = "test://" + normalize("seeds/raw_customers.csv")
        assert seed_file_id in manifest.files

        # Add another seed file
        write_file(raw_customers_csv, project.project_root, "seeds", "more_customers.csv")
        run_dbt(["--partial-parse", "run"])
        seed_file_id = "test://" + normalize("seeds/more_customers.csv")
        manifest = get_manifest(project.project_root)
        assert seed_file_id in manifest.files
        seed_id = "seed.test.more_customers"
        assert seed_id in manifest.nodes

        # Remove seed file and add a schema files with a source referring to raw_customers
        rm_file(project.project_root, "seeds", "more_customers.csv")
        write_file(schema_sources1_yml, project.project_root, "models", "sources.yml")
        results = run_dbt(["--partial-parse", "run"])
        manifest = get_manifest(project.project_root)
        assert len(manifest.sources) == 1
        file_id = "test://" + normalize("models/sources.yml")
        assert file_id in manifest.files

        # add a model referring to raw_customers source
        write_file(customers_sql, project.project_root, "models", "customers.sql")
        results = run_dbt(["--partial-parse", "run"])
        assert len(results) == 2

        # remove sources schema file
        rm_file(project.project_root, "models", "sources.yml")
        with pytest.raises(CompilationError):
            results = run_dbt(["--partial-parse", "run"])

        # put back sources and add an exposures file
        write_file(schema_sources2_yml, project.project_root, "models", "sources.yml")
        results = run_dbt(["--partial-parse", "run"])

        # remove seed referenced in exposures file
        rm_file(project.project_root, "seeds", "raw_customers.csv")
        with pytest.raises(CompilationError):
            results = run_dbt(["--partial-parse", "run"])

        # put back seed and remove depends_on from exposure
        write_file(raw_customers_csv, project.project_root, "seeds", "raw_customers.csv")
        write_file(schema_sources3_yml, project.project_root, "models", "sources.yml")
        results = run_dbt(["--partial-parse", "run"])

        # Add seed config with test to schema.yml, remove exposure
        write_file(schema_sources4_yml, project.project_root, "models", "sources.yml")
        results = run_dbt(["--partial-parse", "run"])

        # Change seed name to wrong name
        write_file(schema_sources5_yml, project.project_root, "models", "sources.yml")
        with pytest.raises(CompilationError):
            results = run_dbt(["--partial-parse", "--warn-error", "run"])

        # Put back seed name to right name
        write_file(schema_sources4_yml, project.project_root, "models", "sources.yml")
        results = run_dbt(["--partial-parse", "run"])

        # Add docs file customers.md
        write_file(customers1_md, project.project_root, "models", "customers.md")
        results = run_dbt(["--partial-parse", "run"])

        # Change docs file customers.md
        write_file(customers2_md, project.project_root, "models", "customers.md")
        results = run_dbt(["--partial-parse", "run"])

        # Delete docs file
        rm_file(project.project_root, "models", "customers.md")
        results = run_dbt(["--partial-parse", "run"])

        # Add a data test
        write_file(test_macro_sql, project.project_root, "macros", "test-macro.sql")
        write_file(my_test_sql, project.project_root, "tests", "my_test.sql")
        results = run_dbt(["--partial-parse", "test"])
        manifest = get_manifest(project.project_root)
        assert len(manifest.nodes) == 9
        test_id = "test.test.my_test"
        assert test_id in manifest.nodes

        # Change macro that data test depends on
        write_file(test_macro2_sql, project.project_root, "macros", "test-macro.sql")
        results = run_dbt(["--partial-parse", "test"])
        manifest = get_manifest(project.project_root)

        # Add an analysis
        write_file(my_analysis_sql, project.project_root, "analyses", "my_analysis.sql")
        results = run_dbt(["--partial-parse", "run"])
        manifest = get_manifest(project.project_root)

        # Remove data test
        rm_file(project.project_root, "tests", "my_test.sql")
        results = run_dbt(["--partial-parse", "test"])
        manifest = get_manifest(project.project_root)
        assert len(manifest.nodes) == 9

        # Remove analysis
        rm_file(project.project_root, "analyses", "my_analysis.sql")
        results = run_dbt(["--partial-parse", "run"])
        manifest = get_manifest(project.project_root)
        assert len(manifest.nodes) == 8

        # Change source test
        write_file(sources_tests2_sql, project.project_root, "macros", "tests.sql")
        results = run_dbt(["--partial-parse", "run"])


class TestPartialParsingDependency:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "model_one.sql": model_one_sql,
        }

    @pytest.fixture(scope="class", autouse=True)
    def setUp(self, project_root):
        local_dependency_files = {
            "dbt_project.yml": local_dependency__dbt_project_yml,
            "models": {
                "schema.yml": local_dependency__models__schema_yml,
                "model_to_import.sql": local_dependency__models__model_to_import_sql,
            },
            "macros": {"dep_macro.sql": local_dependency__macros__dep_macro_sql},
            "seeds": {"seed.csv": local_dependency__seeds__seed_csv},
        }
        write_project_files(project_root, "local_dependency", local_dependency_files)

    @pytest.fixture(scope="class")
    def packages(self):
        return {"packages": [{"local": "local_dependency"}]}

    def test_parsing_with_dependency(self, project):
        run_dbt(["clean"])
        run_dbt(["deps"])
        run_dbt(["seed"])
        run_dbt(["run"])

        # Add a source override
        write_file(schema_models_c_yml, project.project_root, "models", "schema.yml")
        results = run_dbt(["--partial-parse", "run"])
        assert len(results) == 2
        manifest = get_manifest(project.project_root)
        assert len(manifest.sources) == 1
        source_id = "source.local_dep.seed_source.seed"
        assert source_id in manifest.sources
        # We have 1 root model, 1 local_dep model, 1 local_dep seed, 1 local_dep source test, 2 root source tests
        assert len(manifest.nodes) == 5
        test_id = "test.local_dep.source_unique_seed_source_seed_id.afa94935ed"
        assert test_id in manifest.nodes

        # Remove a source override
        rm_file(project.project_root, "models", "schema.yml")
        results = run_dbt(["--partial-parse", "run"])
        manifest = get_manifest(project.project_root)
        assert len(manifest.sources) == 1


class TestNestedMacros:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "model_a.sql": model_a_sql,
            "model_b.sql": model_b_sql,
            "schema.yml": macros_schema_yml,
        }

    @pytest.fixture(scope="class")
    def macros(self):
        return {
            "custom_schema_tests.sql": custom_schema_tests1_sql,
        }

    def test_nested_macros(self, project):
        results = run_dbt()
        assert len(results) == 2
        manifest = get_manifest(project.project_root)
        macro_child_map = manifest.build_macro_child_map()
        macro_unique_id = "macro.test.test_type_two"
        assert macro_unique_id in macro_child_map

        results = run_dbt(["test"], expect_pass=False)
        results = sorted(results, key=lambda r: r.node.name)
        assert len(results) == 2
        # type_one_model_a_
        assert results[0].status == TestStatus.Fail
        assert re.search(r"union all", results[0].node.compiled_code)
        # type_two_model_a_
        assert results[1].status == TestStatus.Warn
        assert results[1].node.config.severity == "WARN"

        write_file(
            custom_schema_tests2_sql, project.project_root, "macros", "custom_schema_tests.sql"
        )
        results = run_dbt(["--partial-parse", "test"], expect_pass=False)
        manifest = get_manifest(project.project_root)
        test_node_id = "test.test.type_two_model_a_.842bc6c2a7"
        assert test_node_id in manifest.nodes
        results = sorted(results, key=lambda r: r.node.name)
        assert len(results) == 2
        # type_two_model_a_
        assert results[1].status == TestStatus.Fail
        assert results[1].node.config.severity == "ERROR"


class TestSkipMacros:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "model_one.sql": model_one_sql,
            "eschema.yml": empty_schema_yml,
        }

    def test_skip_macros(self, project):
        # initial run so we have a msgpack file
        # includes empty_schema file for bug #4850
        results = run_dbt()

        # add a new ref override macro
        write_file(ref_override_sql, project.project_root, "macros", "ref_override.sql")
        results, log_output = run_dbt_and_capture(["--partial-parse", "run"])
        assert "Starting full parse." in log_output

        # modify a ref override macro
        write_file(ref_override2_sql, project.project_root, "macros", "ref_override.sql")
        results, log_output = run_dbt_and_capture(["--partial-parse", "run"])
        assert "Starting full parse." in log_output

        # remove a ref override macro
        rm_file(project.project_root, "macros", "ref_override.sql")
        results, log_output = run_dbt_and_capture(["--partial-parse", "run"])
        assert "Starting full parse." in log_output

        # custom generate_schema_name macro
        write_file(gsm_override_sql, project.project_root, "macros", "gsm_override.sql")
        results, log_output = run_dbt_and_capture(["--partial-parse", "run"])
        assert "Starting full parse." in log_output

        # change generate_schema_name macro
        write_file(gsm_override2_sql, project.project_root, "macros", "gsm_override.sql")
        results, log_output = run_dbt_and_capture(["--partial-parse", "run"])
        assert "Starting full parse." in log_output


class TestSnapshots:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "orders.sql": orders_sql,
        }

    def test_pp_snapshots(self, project):

        # initial run
        results = run_dbt()
        assert len(results) == 1

        # add snapshot
        write_file(snapshot_sql, project.project_root, "snapshots", "snapshot.sql")
        results = run_dbt(["--partial-parse", "run"])
        assert len(results) == 1
        manifest = get_manifest(project.project_root)
        snapshot_id = "snapshot.test.orders_snapshot"
        assert snapshot_id in manifest.nodes
        snapshot2_id = "snapshot.test.orders2_snapshot"
        assert snapshot2_id in manifest.nodes

        # run snapshot
        results = run_dbt(["--partial-parse", "snapshot"])
        assert len(results) == 2

        # modify snapshot
        write_file(snapshot2_sql, project.project_root, "snapshots", "snapshot.sql")
        results = run_dbt(["--partial-parse", "run"])
        assert len(results) == 1

        # delete snapshot
        rm_file(project.project_root, "snapshots", "snapshot.sql")
        results = run_dbt(["--partial-parse", "run"])
        assert len(results) == 1


class TestTests:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "orders.sql": orders_sql,
            "schema.yml": generic_schema_yml,
        }

    @pytest.fixture(scope="class")
    def tests(self):
        # Make sure "generic" directory is created
        return {"generic": {"readme.md": ""}}

    def test_pp_generic_tests(self, project):

        # initial run
        results = run_dbt()
        assert len(results) == 1
        manifest = get_manifest(project.project_root)
        expected_nodes = ["model.test.orders", "test.test.unique_orders_id.1360ecc70e"]
        assert expected_nodes == list(manifest.nodes.keys())

        # add generic test in test-path
        write_file(generic_test_sql, project.project_root, "tests", "generic", "generic_test.sql")
        write_file(generic_test_schema_yml, project.project_root, "models", "schema.yml")
        results = run_dbt(["--partial-parse", "run"])
        assert len(results) == 1
        manifest = get_manifest(project.project_root)
        test_id = "test.test.is_odd_orders_id.82834fdc5b"
        assert test_id in manifest.nodes
        expected_nodes = [
            "model.test.orders",
            "test.test.unique_orders_id.1360ecc70e",
            "test.test.is_odd_orders_id.82834fdc5b",
        ]
        assert expected_nodes == list(manifest.nodes.keys())

        # edit generic test in test-path
        write_file(
            generic_test_edited_sql, project.project_root, "tests", "generic", "generic_test.sql"
        )
        results = run_dbt(["--partial-parse", "run"])
        assert len(results) == 1
        manifest = get_manifest(project.project_root)
        test_id = "test.test.is_odd_orders_id.82834fdc5b"
        assert test_id in manifest.nodes
        expected_nodes = [
            "model.test.orders",
            "test.test.unique_orders_id.1360ecc70e",
            "test.test.is_odd_orders_id.82834fdc5b",
        ]
        assert expected_nodes == list(manifest.nodes.keys())


class TestGroups:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "orders.sql": orders_sql,
            "orders_downstream.sql": orders_downstream_sql,
            "schema.yml": groups_schema_yml_one_group,
        }

    def test_pp_groups(self, project):

        # initial run
        results = run_dbt()
        assert len(results) == 2
        manifest = get_manifest(project.project_root)
        expected_nodes = ["model.test.orders", "model.test.orders_downstream"]
        expected_groups = ["group.test.test_group"]
        assert expected_nodes == sorted(list(manifest.nodes.keys()))
        assert expected_groups == sorted(list(manifest.groups.keys()))

        # add group to schema
        write_file(groups_schema_yml_two_groups, project.project_root, "models", "schema.yml")
        results = run_dbt(["--partial-parse", "run"])
        assert len(results) == 2
        manifest = get_manifest(project.project_root)
        expected_nodes = ["model.test.orders", "model.test.orders_downstream"]
        expected_groups = ["group.test.test_group", "group.test.test_group2"]
        assert expected_nodes == sorted(list(manifest.nodes.keys()))
        assert expected_groups == sorted(list(manifest.groups.keys()))

        # edit group in schema
        write_file(
            groups_schema_yml_two_groups_edited, project.project_root, "models", "schema.yml"
        )
        results = run_dbt(["--partial-parse", "run"])
        assert len(results) == 2
        manifest = get_manifest(project.project_root)
        expected_nodes = ["model.test.orders", "model.test.orders_downstream"]
        expected_groups = ["group.test.test_group", "group.test.test_group2_edited"]
        assert expected_nodes == sorted(list(manifest.nodes.keys()))
        assert expected_groups == sorted(list(manifest.groups.keys()))

        # delete group in schema
        write_file(groups_schema_yml_one_group, project.project_root, "models", "schema.yml")
        results = run_dbt(["--partial-parse", "run"])
        assert len(results) == 2
        manifest = get_manifest(project.project_root)
        expected_nodes = ["model.test.orders", "model.test.orders_downstream"]
        expected_groups = ["group.test.test_group"]
        assert expected_nodes == sorted(list(manifest.nodes.keys()))
        assert expected_groups == sorted(list(manifest.groups.keys()))

        # add back second group
        write_file(groups_schema_yml_two_groups, project.project_root, "models", "schema.yml")
        results = run_dbt(["--partial-parse", "run"])
        assert len(results) == 2

        # remove second group with model still configured to second group
        write_file(
            groups_schema_yml_one_group_model_in_group2,
            project.project_root,
            "models",
            "schema.yml",
        )
        with pytest.raises(ParsingError):
            results = run_dbt(["--partial-parse", "run"])

        # add back second group, make orders private with valid ref
        write_file(
            groups_schema_yml_two_groups_private_orders_valid_access,
            project.project_root,
            "models",
            "schema.yml",
        )
        results = run_dbt(["--partial-parse", "run"])
        assert len(results) == 2

        write_file(
            groups_schema_yml_two_groups_private_orders_invalid_access,
            project.project_root,
            "models",
            "schema.yml",
        )
        with pytest.raises(ParsingError):
            results = run_dbt(["--partial-parse", "run"])


class TestExternalModels:
    @pytest.fixture(scope="class")
    def external_model_node(self):
        return ModelNodeArgs(
            name="external_model",
            package_name="external",
            identifier="test_identifier",
            schema="test_schema",
        )

    @pytest.fixture(scope="class")
    def external_model_node_versioned(self):
        return ModelNodeArgs(
            name="external_model_versioned",
            package_name="external",
            identifier="test_identifier_v1",
            schema="test_schema",
            version=1,
        )

    @pytest.fixture(scope="class")
    def external_model_node_depends_on(self):
        return ModelNodeArgs(
            name="external_model_depends_on",
            package_name="external",
            identifier="test_identifier_depends_on",
            schema="test_schema",
            depends_on_nodes=["model.external.external_model_depends_on_parent"],
        )

    @pytest.fixture(scope="class")
    def external_model_node_depends_on_parent(self):
        return ModelNodeArgs(
            name="external_model_depends_on_parent",
            package_name="external",
            identifier="test_identifier_depends_on_parent",
            schema="test_schema",
        )

    @pytest.fixture(scope="class")
    def models(self):
        return {"model_one.sql": model_one_sql}

    @mock.patch("dbt.plugins.get_plugin_manager")
    def test_pp_external_models(
        self,
        get_plugin_manager,
        project,
        external_model_node,
        external_model_node_versioned,
        external_model_node_depends_on,
        external_model_node_depends_on_parent,
    ):
        # initial plugin - one external model
        external_nodes = PluginNodes()
        external_nodes.add_model(external_model_node)
        get_plugin_manager.return_value.get_nodes.return_value = external_nodes

        # initial parse
        manifest = run_dbt(["parse"])
        assert len(manifest.nodes) == 2
        assert set(manifest.nodes.keys()) == {
            "model.external.external_model",
            "model.test.model_one",
        }
        assert len(manifest.external_node_unique_ids) == 1
        assert manifest.external_node_unique_ids == ["model.external.external_model"]

        # add a model file
        write_file(model_two_sql, project.project_root, "models", "model_two.sql")
        manifest = run_dbt(["--partial-parse", "parse"])
        assert len(manifest.nodes) == 3

        # add an external model
        external_nodes.add_model(external_model_node_versioned)
        manifest = run_dbt(["--partial-parse", "parse"])
        assert len(manifest.nodes) == 4
        assert len(manifest.external_node_unique_ids) == 2

        # add a model file that depends on external model
        write_file(
            "SELECT * FROM {{ref('external', 'external_model')}}",
            project.project_root,
            "models",
            "model_depends_on_external.sql",
        )
        manifest = run_dbt(["--partial-parse", "parse"])
        assert len(manifest.nodes) == 5
        assert len(manifest.external_node_unique_ids) == 2

        # remove a model file that depends on external model
        rm_file(project.project_root, "models", "model_depends_on_external.sql")
        manifest = run_dbt(["--partial-parse", "parse"])
        assert len(manifest.nodes) == 4

        # add an external node with depends on
        external_nodes.add_model(external_model_node_depends_on)
        external_nodes.add_model(external_model_node_depends_on_parent)
        manifest = run_dbt(["--partial-parse", "parse"])
        assert len(manifest.nodes) == 6
        assert len(manifest.external_node_unique_ids) == 4

        # skip files parsing - ensure no issues
        run_dbt(["--partial-parse", "parse"])
        assert len(manifest.nodes) == 6
        assert len(manifest.external_node_unique_ids) == 4
