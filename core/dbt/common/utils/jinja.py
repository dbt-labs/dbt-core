from dbt.common.exceptions import DbtInternalError


MACRO_PREFIX = "dbt_macro__"
DOCS_PREFIX = "dbt_docs__"


def get_dbt_macro_name(name):
    if name is None:
        raise DbtInternalError("Got None for a macro name!")
    return f"{MACRO_PREFIX}{name}"


def get_dbt_docs_name(name):
    if name is None:
        raise DbtInternalError("Got None for a doc name!")
    return f"{DOCS_PREFIX}{name}"


def get_materialization_macro_name(materialization_name, adapter_type=None, with_prefix=True):
    if adapter_type is None:
        adapter_type = "default"
    name = f"materialization_{materialization_name}_{adapter_type}"
    return get_dbt_macro_name(name) if with_prefix else name


def get_docs_macro_name(docs_name, with_prefix=True):
    return get_dbt_docs_name(docs_name) if with_prefix else docs_name


def get_test_macro_name(test_name, with_prefix=True):
    name = f"test_{test_name}"
    return get_dbt_macro_name(name) if with_prefix else name
