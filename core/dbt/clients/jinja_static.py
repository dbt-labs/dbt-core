from typing import Any, Dict, Optional, Tuple

import jinja2

from dbt.artifacts.resources import RefArgs
from dbt.exceptions import MacroNamespaceNotStringError, ParsingError
from dbt_common.clients.jinja import get_environment
from dbt_common.exceptions.macros import MacroNameNotStringError
from dbt_common.tests import test_caching_enabled
from dbt_extractor import ExtractionError, py_extract_from_source  # type: ignore

_TESTING_MACRO_CACHE: Optional[Dict[str, Any]] = {}


def statically_extract_macro_calls(string, ctx, db_wrapper=None):
    # set 'capture_macros' to capture undefined
    env = get_environment(None, capture_macros=True)

    global _TESTING_MACRO_CACHE
    if test_caching_enabled() and string in _TESTING_MACRO_CACHE:
        parsed = _TESTING_MACRO_CACHE.get(string, None)
        func_calls = getattr(parsed, "_dbt_cached_calls")
    else:
        parsed = env.parse(string)
        func_calls = tuple(parsed.find_all(jinja2.nodes.Call))

        if test_caching_enabled():
            _TESTING_MACRO_CACHE[string] = parsed
            setattr(parsed, "_dbt_cached_calls", func_calls)

    standard_calls = ["source", "ref", "config"]
    possible_macro_calls = []
    for func_call in func_calls:
        func_name = None
        if hasattr(func_call, "node") and hasattr(func_call.node, "name"):
            func_name = func_call.node.name
        else:
            if (
                hasattr(func_call, "node")
                and hasattr(func_call.node, "node")
                and type(func_call.node.node).__name__ == "Name"
                and hasattr(func_call.node, "attr")
            ):
                package_name = func_call.node.node.name
                macro_name = func_call.node.attr
                if package_name == "adapter":
                    if macro_name == "dispatch":
                        ad_macro_calls = statically_parse_adapter_dispatch(
                            func_call, ctx, db_wrapper
                        )
                        possible_macro_calls.extend(ad_macro_calls)
                    else:
                        # This skips calls such as adapter.parse_index
                        continue
                else:
                    func_name = f"{package_name}.{macro_name}"
            else:
                continue
        if not func_name:
            continue
        if func_name in standard_calls:
            continue
        elif ctx.get(func_name):
            continue
        else:
            if func_name not in possible_macro_calls:
                possible_macro_calls.append(func_name)

    return possible_macro_calls


# Call(
#   node=Getattr(
#     node=Name(
#       name='adapter',
#       ctx='load'
#     ),
#     attr='dispatch',
#     ctx='load'
#   ),
#   args=[
#     Const(value='test_pkg_and_dispatch')
#   ],
#   kwargs=[
#     Keyword(
#       key='packages',
#       value=Call(node=Getattr(node=Name(name='local_utils', ctx='load'),
#          attr='_get_utils_namespaces', ctx='load'), args=[], kwargs=[],
#          dyn_args=None, dyn_kwargs=None)
#     )
#   ],
#   dyn_args=None,
#   dyn_kwargs=None
# )
def statically_parse_adapter_dispatch(func_call, ctx, db_wrapper):
    possible_macro_calls = []
    # This captures an adapter.dispatch('<macro_name>') call.

    func_name = None
    # macro_name positional argument
    if len(func_call.args) > 0:
        func_name = func_call.args[0].value
    if func_name:
        possible_macro_calls.append(func_name)

    # packages positional argument
    macro_namespace = None
    packages_arg = None
    packages_arg_type = None

    if len(func_call.args) > 1:
        packages_arg = func_call.args[1]
        # This can be a List or a Call
        packages_arg_type = type(func_call.args[1]).__name__

    # keyword arguments
    if func_call.kwargs:
        for kwarg in func_call.kwargs:
            if kwarg.key == "macro_name":
                # This will remain to enable static resolution
                if type(kwarg.value).__name__ == "Const":
                    func_name = kwarg.value.value
                    possible_macro_calls.append(func_name)
                else:
                    raise MacroNameNotStringError(kwarg_value=kwarg.value.value)
            elif kwarg.key == "macro_namespace":
                # This will remain to enable static resolution
                kwarg_type = type(kwarg.value).__name__
                if kwarg_type == "Const":
                    macro_namespace = kwarg.value.value
                else:
                    raise MacroNamespaceNotStringError(kwarg_type)

    # positional arguments
    if packages_arg:
        if packages_arg_type == "List":
            # This will remain to enable static resolution
            packages = []
            for item in packages_arg.items:
                packages.append(item.value)
        elif packages_arg_type == "Const":
            # This will remain to enable static resolution
            macro_namespace = packages_arg.value

    if db_wrapper:
        macro = db_wrapper.dispatch(func_name, macro_namespace=macro_namespace).macro
        func_name = f"{macro.package_name}.{macro.name}"
        possible_macro_calls.append(func_name)
    else:  # this is only for tests/unit/test_macro_calls.py
        if macro_namespace:
            packages = [macro_namespace]
        else:
            packages = []
        for package_name in packages:
            possible_macro_calls.append(f"{package_name}.{func_name}")

    return possible_macro_calls


def statically_parse_ref(input: str) -> RefArgs:
    """
    Returns a RefArgs object corresponding to an input jinja expression.

    input: str representing how input node is referenced in tested model sql
        * examples:
        - "ref('my_model_a')"
        - "ref('my_model_a', version=3)"
        - "ref('package', 'my_model_a', version=3)"

    If input is not a well-formed jinja expression, TODO is raised.
    If input is not a valid ref expression, TODO is raised.
    """
    try:
        statically_parsed = py_extract_from_source(f"{{{{ {input} }}}}")
    except ExtractionError:
        raise ParsingError(f"Invalid jinja expression: {input}")

    if not statically_parsed.get("refs"):
        raise ParsingError(f"Invalid ref expression: {input}")

    ref = list(statically_parsed["refs"])[0]
    return RefArgs(package=ref.get("package"), name=ref.get("name"), version=ref.get("version"))


def statically_parse_source(input: str) -> Tuple[str, str]:
    """
    Returns a RefArgs object corresponding to an input jinja expression.

    input: str representing how input node is referenced in tested model sql
        * examples:
        - "source('my_source_schema', 'my_source_name')"

    If input is not a well-formed jinja expression, TODO is raised.
    If input is not a valid source expression, TODO is raised.
    """
    try:
        statically_parsed = py_extract_from_source(f"{{{{ {input} }}}}")
    except ExtractionError:
        raise ParsingError(f"Invalid jinja expression: {input}")

    if not statically_parsed.get("sources"):
        raise ParsingError(f"Invalid source expression: {input}")

    source = list(statically_parsed["sources"])[0]
    source_name, source_table_name = source
    return source_name, source_table_name
