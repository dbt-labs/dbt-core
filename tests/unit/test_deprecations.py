import argparse
import pytest

from dbt.internal_deprecations import deprecated
import dbt.exceptions
from dbt.node_types import NodeType
from dbt.flags import set_from_args
from argparse import Namespace


@deprecated(reason="just because", version="1.23.0", suggested_action="Make some updates")
def to_be_decorated():
    return 5


# simpletest that the return value is not modified
def test_deprecated_func():
    set_from_args(Namespace(WARN_ERROR=False), None)
    assert hasattr(to_be_decorated, "__wrapped__")
    assert to_be_decorated() == 5


class TestDeprecatedFunctions:
    def is_deprecated(self, func):
        assert hasattr(func, "__wrapped__")
        # TODO: add in log check

    def test_warn(self):
        self.is_deprecated(dbt.exceptions.warn)


class TestDeprecatedExceptionFunctions:
    @pytest.fixture(scope="class", autouse=True)
    def setUp(self):
        set_from_args(Namespace(WARN_ERROR=False), None)

    def runFunc(self, func, *args):
        return func(*args)

    def is_deprecated(self, func):
        assert hasattr(func, "__wrapped__")
        # TODO: add in log check

    def test_missing_config(self):
        func = dbt.exceptions.missing_config
        exception = dbt.exceptions.MissingConfigError
        model = argparse.Namespace()
        model.unique_id = ""
        name = ""

        self.is_deprecated(func)

        assert hasattr(func, "__wrapped__")
        with pytest.raises(exception):
            func(model, name)

    def test_missing_materialization(self):
        func = dbt.exceptions.missing_materialization
        exception = dbt.exceptions.MissingMaterializationError
        model = argparse.Namespace()
        model.config = argparse.Namespace()
        model.config.materialized = ""
        adapter_type = ""

        self.is_deprecated(func)

        assert hasattr(func, "__wrapped__")
        with pytest.raises(exception):
            func(model, adapter_type)

    def test_missing_relation(self):
        func = dbt.exceptions.missing_relation
        exception = dbt.exceptions.MissingRelationError
        relation = ""

        self.is_deprecated(func)

        assert hasattr(func, "__wrapped__")
        with pytest.raises(exception):
            func(relation)

    def test_raise_ambiguous_alias(self):
        func = dbt.exceptions.raise_ambiguous_alias
        exception = dbt.exceptions.AmbiguousAliasError
        node_1 = argparse.Namespace()
        node_1.unique_id = ""
        node_1.original_file_path = ""
        node_2 = argparse.Namespace()
        node_2.unique_id = ""
        node_2.original_file_path = ""
        duped_name = "string"

        self.is_deprecated(func)

        assert hasattr(func, "__wrapped__")
        with pytest.raises(exception):
            func(node_1, node_2, duped_name)

    def test_raise_ambiguous_catalog_match(self):
        func = dbt.exceptions.raise_ambiguous_catalog_match
        exception = dbt.exceptions.AmbiguousCatalogMatchError
        unique_id = ""
        match_1 = {"metadata": {"schema": ""}}
        match_2 = {"metadata": {"schema": ""}}

        self.is_deprecated(func)

        assert hasattr(func, "__wrapped__")
        with pytest.raises(exception):
            func(unique_id, match_1, match_2)

    def test_raise_cache_inconsistent(self):
        func = dbt.exceptions.raise_cache_inconsistent
        exception = dbt.exceptions.CacheInconsistencyError
        msg = ""

        self.is_deprecated(func)

        assert hasattr(func, "__wrapped__")
        with pytest.raises(exception):
            func(msg)

    def test_raise_dataclass_not_dict(self):
        func = dbt.exceptions.raise_dataclass_not_dict
        exception = dbt.exceptions.DataclassNotDictError
        obj = ""

        self.is_deprecated(func)

        assert hasattr(func, "__wrapped__")
        with pytest.raises(exception):
            func(obj)

    def test_raise_compiler_error(self):
        func = dbt.exceptions.raise_compiler_error
        exception = dbt.exceptions.CompilationError
        msg = ""

        self.is_deprecated(func)

        assert hasattr(func, "__wrapped__")
        with pytest.raises(exception):
            func(msg)

    def test_raise_database_error(self):
        func = dbt.exceptions.raise_database_error
        exception = dbt.exceptions.DbtDatabaseError
        msg = ""

        self.is_deprecated(func)

        assert hasattr(func, "__wrapped__")
        with pytest.raises(exception):
            func(msg)

    def test_raise_dep_not_found(self):
        func = dbt.exceptions.raise_dep_not_found
        exception = dbt.exceptions.DependencyNotFoundError
        node = ""
        node_description = ""
        required_pkg = ""

        self.is_deprecated(func)

        assert hasattr(func, "__wrapped__")
        with pytest.raises(exception):
            func(node, node_description, required_pkg)

    def test_raise_dependency_error(self):
        func = dbt.exceptions.raise_dependency_error
        exception = dbt.exceptions.DependencyError
        msg = ""

        self.is_deprecated(func)

        assert hasattr(func, "__wrapped__")
        with pytest.raises(exception):
            func(msg)

    def test_raise_duplicate_patch_name(self):
        func = dbt.exceptions.raise_duplicate_patch_name
        exception = dbt.exceptions.DuplicatePatchPathError
        patch_1 = argparse.Namespace()
        patch_1.name = ""
        patch_1.original_file_path = ""
        existing_patch_path = ""

        self.is_deprecated(func)

        assert hasattr(func, "__wrapped__")
        with pytest.raises(exception):
            func(patch_1, existing_patch_path)

    def test_raise_duplicate_resource_name(self):
        func = dbt.exceptions.raise_duplicate_resource_name
        exception = dbt.exceptions.DuplicateResourceNameError
        node_1 = argparse.Namespace()
        node_1.name = ""
        node_1.resource_type = NodeType("model")
        node_1.column_name = ""
        node_1.unique_id = ""
        node_1.original_file_path = ""
        node_2 = argparse.Namespace()
        node_2.name = ""
        node_2.resource_type = ""
        node_2.unique_id = ""
        node_2.original_file_path = ""

        self.is_deprecated(func)

        assert hasattr(func, "__wrapped__")
        with pytest.raises(exception):
            func(node_1, node_2)

    def test_raise_invalid_property_yml_version(self):
        func = dbt.exceptions.raise_invalid_property_yml_version
        exception = dbt.exceptions.PropertyYMLError
        path = ""
        issue = ""

        self.is_deprecated(func)

        assert hasattr(func, "__wrapped__")
        with pytest.raises(exception):
            func(path, issue)

    def test_raise_not_implemented(self):
        func = dbt.exceptions.raise_not_implemented
        exception = dbt.exceptions.NotImplementedError
        msg = ""

        self.is_deprecated(func)

        assert hasattr(func, "__wrapped__")
        with pytest.raises(exception):
            func(msg)

    def test_relation_wrong_type(self):
        func = dbt.exceptions.relation_wrong_type
        exception = dbt.exceptions.RelationWrongTypeError

        relation = argparse.Namespace()
        relation.type = ""
        expected_type = ""

        self.is_deprecated(func)

        assert hasattr(func, "__wrapped__")
        with pytest.raises(exception):
            func(relation, expected_type)

    def test_raise_duplicate_alias(self):
        func = dbt.exceptions.raise_duplicate_alias
        exception = dbt.exceptions.DuplicateAliasError
        kwargs = {"": ""}
        aliases = {"": ""}
        canonical_key = ""

        self.is_deprecated(func)

        assert hasattr(func, "__wrapped__")
        with pytest.raises(exception):
            func(kwargs, aliases, canonical_key)

    def test_raise_duplicate_source_patch_name(self):
        func = dbt.exceptions.raise_duplicate_source_patch_name
        exception = dbt.exceptions.DuplicateSourcePatchNameError
        patch_1 = argparse.Namespace()
        patch_1.name = ""
        patch_1.path = ""
        patch_1.overrides = ""
        patch_2 = argparse.Namespace()
        patch_2.path = ""

        self.is_deprecated(func)

        assert hasattr(func, "__wrapped__")
        with pytest.raises(exception):
            func(patch_1, patch_2)

    def test_raise_duplicate_macro_patch_name(self):
        func = dbt.exceptions.raise_duplicate_macro_patch_name
        exception = dbt.exceptions.DuplicateMacroPatchNameError
        patch_1 = argparse.Namespace()
        patch_1.package_name = ""
        patch_1.name = ""
        patch_1.original_file_path = ""
        existing_patch_path = ""

        self.is_deprecated(func)

        assert hasattr(func, "__wrapped__")
        with pytest.raises(exception):
            func(patch_1, existing_patch_path)

    def test_raise_duplicate_macro_name(self):
        func = dbt.exceptions.raise_duplicate_macro_name
        exception = dbt.exceptions.DuplicateMacroNameError
        node_1 = argparse.Namespace()
        node_1.name = ""
        node_1.package_name = ""
        node_1.original_file_path = ""
        node_1.unique_id = ""
        node_2 = argparse.Namespace()
        node_2.package_name = ""
        node_2.unique_id = ""
        node_2.original_file_path = ""
        namespace = ""

        self.is_deprecated(func)

        assert hasattr(func, "__wrapped__")
        with pytest.raises(exception):
            func(node_1, node_2, namespace)

    def test_approximate_relation_match(self):
        func = dbt.exceptions.approximate_relation_match
        exception = dbt.exceptions.ApproximateMatchError
        target = ""
        relation = ""

        self.is_deprecated(func)

        assert hasattr(func, "__wrapped__")
        with pytest.raises(exception):
            func(target, relation)

    def test_get_relation_returned_multiple_results(self):
        func = dbt.exceptions.get_relation_returned_multiple_results
        exception = dbt.exceptions.RelationReturnedMultipleResultsError
        kwargs = {}
        matches = []

        self.is_deprecated(func)

        assert hasattr(func, "__wrapped__")
        with pytest.raises(exception):
            func(kwargs, matches)

    def test_system_error(self):
        func = dbt.exceptions.system_error
        exception = dbt.exceptions.OperationError
        operation_name = ""

        self.is_deprecated(func)

        assert hasattr(func, "__wrapped__")
        with pytest.raises(exception):
            func(operation_name)

    def test_invalid_materialization_argument(self):
        func = dbt.exceptions.invalid_materialization_argument
        exception = dbt.exceptions.MaterializationArgError
        name = ""
        argument = ""

        self.is_deprecated(func)

        assert hasattr(func, "__wrapped__")
        with pytest.raises(exception):
            func(name, argument)

    def test_bad_package_spec(self):
        func = dbt.exceptions.bad_package_spec
        exception = dbt.exceptions.BadSpecError
        repo = ""
        spec = ""
        error = argparse.Namespace()
        error.stderr = ""

        self.is_deprecated(func)

        assert hasattr(func, "__wrapped__")
        with pytest.raises(exception):
            func(repo, spec, error)

    # def test_raise_git_cloning_error(self):
    #     func = dbt.exceptions.raise_git_cloning_error
    #     exception = dbt.exceptions.CommandResultError

    #     error = dbt.exceptions.CommandResultError
    #     error.cwd = ""
    #     error.cmd = [""]
    #     error.returncode = 1
    #     error.stdout = ""
    #     error.stderr = ""

    #     self.is_deprecated(func)

    #     assert(hasattr(func, '__wrapped__'))
    #     with pytest.raises(exception):
    #         func(error)

    def test_raise_git_cloning_problem(self):
        func = dbt.exceptions.raise_git_cloning_problem
        exception = dbt.exceptions.UnknownGitCloningProblemError
        repo = ""

        self.is_deprecated(func)

        assert hasattr(func, "__wrapped__")
        with pytest.raises(exception):
            func(repo)

    def test_macro_invalid_dispatch_arg(self):
        func = dbt.exceptions.macro_invalid_dispatch_arg
        exception = dbt.exceptions.MacroDispatchArgError
        macro_name = ""

        self.is_deprecated(func)

        assert hasattr(func, "__wrapped__")
        with pytest.raises(exception):
            func(macro_name)

    def test_dependency_not_found(self):
        func = dbt.exceptions.dependency_not_found
        exception = dbt.exceptions.GraphDependencyNotFoundError
        node = argparse.Namespace()
        node.unique_id = ""
        dependency = ""

        self.is_deprecated(func)

        assert hasattr(func, "__wrapped__")
        with pytest.raises(exception):
            func(node, dependency)

    def test_target_not_found(self):
        func = dbt.exceptions.target_not_found
        exception = dbt.exceptions.TargetNotFoundError
        node = argparse.Namespace()
        node.unique_id = ""
        node.original_file_path = ""
        node.resource_type = ""
        target_name = ""
        target_kind = ""

        self.is_deprecated(func)

        assert hasattr(func, "__wrapped__")
        with pytest.raises(exception):
            func(node, target_name, target_kind)

    def test_doc_target_not_found(self):
        func = dbt.exceptions.doc_target_not_found
        exception = dbt.exceptions.DocTargetNotFoundError
        model = argparse.Namespace()
        model.unique_id = ""
        target_doc_name = ""
        target_doc_package = ""

        self.is_deprecated(func)

        assert hasattr(func, "__wrapped__")
        with pytest.raises(exception):
            func(model, target_doc_name, target_doc_package)

    def test_ref_bad_context(self):
        func = dbt.exceptions.ref_bad_context
        exception = dbt.exceptions.RefBadContextError
        model = argparse.Namespace()
        model.name = ""
        args = []

        self.is_deprecated(func)

        assert hasattr(func, "__wrapped__")
        with pytest.raises(exception):
            func(model, args)

    def test_metric_invalid_args(self):
        func = dbt.exceptions.metric_invalid_args
        exception = dbt.exceptions.MetricArgsError
        model = argparse.Namespace()
        model.unique_id = ""
        args = []

        self.is_deprecated(func)

        assert hasattr(func, "__wrapped__")
        with pytest.raises(exception):
            func(model, args)

    def test_ref_invalid_args(self):
        func = dbt.exceptions.ref_invalid_args
        exception = dbt.exceptions.RefArgsError
        model = argparse.Namespace()
        model.unique_id = ""
        args = []

        self.is_deprecated(func)

        assert hasattr(func, "__wrapped__")
        with pytest.raises(exception):
            func(model, args)

    def test_invalid_bool_error(self):
        func = dbt.exceptions.invalid_bool_error
        exception = dbt.exceptions.BooleanError
        return_value = ""
        macro_name = ""

        self.is_deprecated(func)

        assert hasattr(func, "__wrapped__")
        with pytest.raises(exception):
            func(return_value, macro_name)

    def test_invalid_type_error(self):
        func = dbt.exceptions.invalid_type_error
        exception = dbt.exceptions.MacroArgTypeError
        method_name = ""
        arg_name = ""
        got_value = ""
        expected_type = ""

        self.is_deprecated(func)

        assert hasattr(func, "__wrapped__")
        with pytest.raises(exception):
            func(method_name, arg_name, got_value, expected_type)

    def test_disallow_secret_env_var(self):
        func = dbt.exceptions.disallow_secret_env_var
        exception = dbt.exceptions.SecretEnvVarLocationError
        env_var_name = ""

        self.is_deprecated(func)

        assert hasattr(func, "__wrapped__")
        with pytest.raises(exception):
            func(env_var_name)

    def test_raise_parsing_error(self):
        func = dbt.exceptions.raise_parsing_error
        exception = dbt.exceptions.ParsingError
        msg = ""

        self.is_deprecated(func)

        assert hasattr(func, "__wrapped__")
        with pytest.raises(exception):
            func(msg)

    def test_raise_unrecognized_credentials_type(self):
        func = dbt.exceptions.raise_unrecognized_credentials_type
        exception = dbt.exceptions.UnrecognizedCredentialTypeError
        typename = ""
        supported_types = []

        self.is_deprecated(func)

        assert hasattr(func, "__wrapped__")
        with pytest.raises(exception):
            func(typename, supported_types)

    def test_raise_patch_targets_not_found(self):
        func = dbt.exceptions.raise_patch_targets_not_found
        exception = dbt.exceptions.PatchTargetNotFoundError
        node = argparse.Namespace()
        node.name = ""
        node.original_file_path = ""
        patches = {"patch": node}

        self.is_deprecated(func)

        assert hasattr(func, "__wrapped__")
        with pytest.raises(exception):
            func(patches)

    def test_multiple_matching_relations(self):
        func = dbt.exceptions.multiple_matching_relations
        exception = dbt.exceptions.RelationReturnedMultipleResultsError
        kwargs = {}
        matches = []

        self.is_deprecated(func)

        assert hasattr(func, "__wrapped__")
        with pytest.raises(exception):
            func(kwargs, matches)

    def test_materialization_not_available(self):
        func = dbt.exceptions.materialization_not_available
        exception = dbt.exceptions.MaterializationNotAvailableError
        model = argparse.Namespace()
        model.config = argparse.Namespace()
        model.config.materialized = ""
        adapter_type = ""

        self.is_deprecated(func)

        assert hasattr(func, "__wrapped__")
        with pytest.raises(exception):
            func(model, adapter_type)

    def test_macro_not_found(self):
        func = dbt.exceptions.macro_not_found
        exception = dbt.exceptions.MacroNotFoundError
        model = argparse.Namespace()
        model.unique_id = ""
        target_macro_id = ""

        self.is_deprecated(func)

        assert hasattr(func, "__wrapped__")
        with pytest.raises(exception):
            func(model, target_macro_id)
