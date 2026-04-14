"""Tests for FunctionLookup overloaded UDF support.

FunctionLookup indexes functions by both their internal name (search_name)
and their alias (identifier). When multiple functions share an alias
(overloaded UDFs), find_all() returns all of them so that dependency
edges are created to every overload.
"""

import pytest

from dbt.artifacts.resources import FunctionArgument, FunctionReturns
from dbt.contracts.files import FileHash
from dbt.contracts.graph.manifest import FunctionLookup, Manifest
from dbt.contracts.graph.nodes import FunctionNode, NodeType


def _make_function_node(
    name: str,
    package_name: str = "test_project",
    alias: str | None = None,
    arguments: list[FunctionArgument] | None = None,
    data_type: str = "int",
) -> FunctionNode:
    """Helper to create a FunctionNode for testing."""
    return FunctionNode(
        resource_type=NodeType.Function,
        name=name,
        returns=FunctionReturns(data_type=data_type),
        database="db",
        schema="public",
        package_name=package_name,
        path=f"functions/{name}.sql",
        original_file_path=f"functions/{name}.sql",
        unique_id=f"function.{package_name}.{name}",
        fqn=[package_name, name],
        alias=alias or name,
        checksum=FileHash.from_contents(name),
        arguments=arguments or [],
    )


class TestFunctionLookupBasic:
    """Test basic FunctionLookup behavior (non-overloaded)."""

    def test_find_by_name(self):
        fn = _make_function_node("double_it")
        manifest = Manifest(functions={fn.unique_id: fn})
        lookup = FunctionLookup(manifest)

        result = lookup.find("double_it", "test_project", manifest)
        assert result is not None
        assert result.unique_id == "function.test_project.double_it"

    def test_find_missing_returns_none(self):
        fn = _make_function_node("double_it")
        manifest = Manifest(functions={fn.unique_id: fn})
        lookup = FunctionLookup(manifest)

        result = lookup.find("nonexistent", "test_project", manifest)
        assert result is None

    def test_find_all_single_function(self):
        fn = _make_function_node("double_it")
        manifest = Manifest(functions={fn.unique_id: fn})
        lookup = FunctionLookup(manifest)

        results = lookup.find_all("double_it", "test_project", manifest)
        assert len(results) == 1
        assert results[0].unique_id == "function.test_project.double_it"


class TestFunctionLookupOverloaded:
    """Test FunctionLookup with overloaded UDFs sharing the same alias."""

    @pytest.fixture
    def overloaded_functions(self):
        """Create two functions with different names but the same alias."""
        fn_varchar = _make_function_node(
            name="null_if_empty_varchar",
            alias="null_if_empty",
            arguments=[FunctionArgument(name="val", data_type="varchar")],
            data_type="varchar",
        )
        fn_array = _make_function_node(
            name="null_if_empty_array",
            alias="null_if_empty",
            arguments=[FunctionArgument(name="val", data_type="array")],
            data_type="array",
        )
        return fn_varchar, fn_array

    def test_find_by_alias_returns_first_overload(self, overloaded_functions):
        fn_varchar, fn_array = overloaded_functions
        manifest = Manifest(
            functions={fn_varchar.unique_id: fn_varchar, fn_array.unique_id: fn_array}
        )
        lookup = FunctionLookup(manifest)

        # Looking up by alias should return at least one function
        result = lookup.find("null_if_empty", "test_project", manifest)
        assert result is not None
        assert result.alias == "null_if_empty"

    def test_find_all_by_alias_returns_all_overloads(self, overloaded_functions):
        fn_varchar, fn_array = overloaded_functions
        manifest = Manifest(
            functions={fn_varchar.unique_id: fn_varchar, fn_array.unique_id: fn_array}
        )
        lookup = FunctionLookup(manifest)

        results = lookup.find_all("null_if_empty", "test_project", manifest)
        assert len(results) == 2
        unique_ids = {r.unique_id for r in results}
        assert unique_ids == {
            "function.test_project.null_if_empty_varchar",
            "function.test_project.null_if_empty_array",
        }

    def test_find_by_internal_name_returns_specific_overload(self, overloaded_functions):
        fn_varchar, fn_array = overloaded_functions
        manifest = Manifest(
            functions={fn_varchar.unique_id: fn_varchar, fn_array.unique_id: fn_array}
        )
        lookup = FunctionLookup(manifest)

        # Looking up by internal name should return that specific overload
        result = lookup.find("null_if_empty_varchar", "test_project", manifest)
        assert result is not None
        assert result.unique_id == "function.test_project.null_if_empty_varchar"

        result = lookup.find("null_if_empty_array", "test_project", manifest)
        assert result is not None
        assert result.unique_id == "function.test_project.null_if_empty_array"

    def test_get_all_unique_ids_by_alias(self, overloaded_functions):
        fn_varchar, fn_array = overloaded_functions
        manifest = Manifest(
            functions={fn_varchar.unique_id: fn_varchar, fn_array.unique_id: fn_array}
        )
        lookup = FunctionLookup(manifest)

        ids = lookup.get_all_unique_ids("null_if_empty", "test_project")
        assert len(ids) == 2
        assert set(ids) == {
            "function.test_project.null_if_empty_varchar",
            "function.test_project.null_if_empty_array",
        }

    def test_no_alias_no_duplicate_indexing(self):
        """When alias equals name, function should only be indexed once."""
        fn = _make_function_node("double_it")
        manifest = Manifest(functions={fn.unique_id: fn})
        lookup = FunctionLookup(manifest)

        # Should only find one entry, not a duplicate
        ids = lookup.get_all_unique_ids("double_it", "test_project")
        assert len(ids) == 1

    def test_find_all_without_package_returns_all(self, overloaded_functions):
        fn_varchar, fn_array = overloaded_functions
        manifest = Manifest(
            functions={fn_varchar.unique_id: fn_varchar, fn_array.unique_id: fn_array}
        )
        lookup = FunctionLookup(manifest)

        # package=None should return all overloads across packages
        results = lookup.find_all("null_if_empty", None, manifest)
        assert len(results) == 2

    def test_overloads_across_packages(self):
        """Overloads in different packages should be independently resolvable."""
        fn_pkg1 = _make_function_node(
            name="my_func_int",
            package_name="pkg1",
            alias="my_func",
            arguments=[FunctionArgument(name="val", data_type="int")],
        )
        fn_pkg2 = _make_function_node(
            name="my_func_str",
            package_name="pkg2",
            alias="my_func",
            arguments=[FunctionArgument(name="val", data_type="varchar")],
        )
        manifest = Manifest(functions={fn_pkg1.unique_id: fn_pkg1, fn_pkg2.unique_id: fn_pkg2})
        lookup = FunctionLookup(manifest)

        # By specific package
        results_pkg1 = lookup.find_all("my_func", "pkg1", manifest)
        assert len(results_pkg1) == 1
        assert results_pkg1[0].package_name == "pkg1"

        results_pkg2 = lookup.find_all("my_func", "pkg2", manifest)
        assert len(results_pkg2) == 1
        assert results_pkg2[0].package_name == "pkg2"

        # Without package filter
        results_all = lookup.find_all("my_func", None, manifest)
        assert len(results_all) == 2
