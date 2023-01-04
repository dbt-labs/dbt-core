import functools
from typing import NoReturn

from dbt.events.functions import warn_or_error
from dbt.events.helpers import env_secrets, scrub_secrets
from dbt.events.types import JinjaLogWarning

from dbt.exceptions import (
    RuntimeException,
    MissingConfig,
    MissingMaterialization,
    MissingRelation,
    AmbiguousAlias,
    AmbiguousCatalogMatch,
    CacheInconsistency,
    DataclassNotDict,
    CompilationException,
    DatabaseException,
    DependencyNotFound,
    DependencyException,
    DuplicatePatchPath,
    DuplicateResourceName,
    InvalidPropertyYML,
    NotImplementedException,
    RelationWrongType,
)


def warn(msg, node=None):
    warn_or_error(JinjaLogWarning(msg=msg), node=node)
    return ""


def missing_config(model, name) -> NoReturn:
    raise MissingConfig(unique_id=model.unique_id, name=name)


def missing_materialization(model, adapter_type) -> NoReturn:
    raise MissingMaterialization(model=model, adapter_type=adapter_type)


def missing_relation(relation, model=None) -> NoReturn:
    raise MissingRelation(relation, model)


def raise_ambiguous_alias(node_1, node_2, duped_name=None) -> NoReturn:
    raise AmbiguousAlias(node_1, node_2, duped_name)


def raise_ambiguous_catalog_match(unique_id, match_1, match_2) -> NoReturn:
    raise AmbiguousCatalogMatch(unique_id, match_1, match_2)


def raise_cache_inconsistent(message) -> NoReturn:
    raise CacheInconsistency(message)


def raise_dataclass_not_dict(obj) -> NoReturn:
    raise DataclassNotDict(obj)


def raise_compiler_error(msg, node=None) -> NoReturn:
    raise CompilationException(msg, node)


def raise_database_error(msg, node=None) -> NoReturn:
    raise DatabaseException(msg, node)


def raise_dep_not_found(node, node_description, required_pkg) -> NoReturn:
    raise DependencyNotFound(node, node_description, required_pkg)


def raise_dependency_error(msg) -> NoReturn:
    raise DependencyException(scrub_secrets(msg, env_secrets()))


def raise_duplicate_patch_name(patch_1, existing_patch_path) -> NoReturn:
    raise DuplicatePatchPath(patch_1, existing_patch_path)


def raise_duplicate_resource_name(node_1, node_2) -> NoReturn:
    raise DuplicateResourceName(node_1, node_2)


def raise_invalid_property_yml_version(path, issue) -> NoReturn:
    raise InvalidPropertyYML(path, issue)


def raise_not_implemented(msg) -> NoReturn:
    raise NotImplementedException(msg)


def relation_wrong_type(relation, expected_type, model=None) -> NoReturn:
    raise RelationWrongType(relation, expected_type, model)


# Update this when a new function should be added to the
# dbt context's `exceptions` key!
CONTEXT_EXPORTS = {
    fn.__name__: fn
    for fn in [
        warn,
        missing_config,
        missing_materialization,
        missing_relation,
        raise_ambiguous_alias,
        raise_ambiguous_catalog_match,
        raise_cache_inconsistent,
        raise_dataclass_not_dict,
        raise_compiler_error,
        raise_database_error,
        raise_dep_not_found,
        raise_dependency_error,
        raise_duplicate_patch_name,
        raise_duplicate_resource_name,
        raise_invalid_property_yml_version,
        raise_not_implemented,
        relation_wrong_type,
    ]
}


# wraps context based exceptions in node info
def wrapper(model):
    def wrap(func):
        @functools.wraps(func)
        def inner(*args, **kwargs):
            try:
                return func(*args, **kwargs)
            except RuntimeException as exc:
                exc.add_node(model)
                raise exc

        return inner

    return wrap


def wrapped_exports(model):
    wrap = wrapper(model)
    return {name: wrap(export) for name, export in CONTEXT_EXPORTS.items()}
