import typing
from typing import Any, Dict, List, Optional, Union

import jinja2

from dbt.artifacts.resources import RefArgs
from dbt.exceptions import MacroNamespaceNotStringError, ParsingError
from dbt_common.clients.jinja import get_environment
from dbt_common.exceptions.macros import MacroNameNotStringError
from dbt_common.tests import test_caching_enabled
from dbt_extractor import ExtractionError, py_extract_from_source  # type: ignore

if typing.TYPE_CHECKING:
    from dbt.context.providers import ParseDatabaseWrapper


_TESTING_MACRO_CACHE: Dict[str, Any] = {}


def statically_extract_has_name_this(source: str) -> bool:
    """Checks whether the raw jinja has any references to `this`"""
    env = get_environment(None, capture_macros=True)
    parsed = env.parse(source)
    names = tuple(parsed.find_all(jinja2.nodes.Name))

    for name in names:
        if hasattr(name, "name") and name.name == "this":
            return True
    return False


def statically_extract_macro_calls(
    source: str, ctx: Dict[str, Any], db_wrapper: Optional["ParseDatabaseWrapper"] = None
) -> List[str]:
    # set 'capture_macros' to capture undefined
    env = get_environment(None, capture_macros=True)

    global _TESTING_MACRO_CACHE
    if test_caching_enabled() and source in _TESTING_MACRO_CACHE:
        parsed = _TESTING_MACRO_CACHE.get(source, None)
        func_calls = getattr(parsed, "_dbt_cached_calls")
    else:
        parsed = env.parse(source)
        func_calls = tuple(parsed.find_all(jinja2.nodes.Call))

        if test_caching_enabled():
            _TESTING_MACRO_CACHE[source] = parsed
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


def statically_parse_adapter_dispatch(
    func_call, ctx: Dict[str, Any], db_wrapper: Optional["ParseDatabaseWrapper"]
) -> List[str]:
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
        func_name = f"{macro.package_name}.{macro.name}"  # type: ignore[attr-defined]
        possible_macro_calls.append(func_name)
    else:  # this is only for tests/unit/test_macro_calls.py
        if macro_namespace:
            packages = [macro_namespace]
        else:
            packages = []
        for package_name in packages:
            possible_macro_calls.append(f"{package_name}.{func_name}")

    return possible_macro_calls


def statically_parse_ref_or_source(expression: str) -> Union[RefArgs, List[str]]:
    """
    Returns a RefArgs or List[str] object, corresponding to ref or source respectively, given an input jinja expression.

    input: str representing how input node is referenced in tested model sql
        * examples:
        - "ref('my_model_a')"
        - "ref('my_model_a', version=3)"
        - "ref('package', 'my_model_a', version=3)"
        - "source('my_source_schema', 'my_source_name')"

    If input is not a well-formed jinja ref or source expression, a ParsingError is raised.
    """
    ref_or_source: Union[RefArgs, List[str]]

    try:
        statically_parsed = py_extract_from_source(f"{{{{ {expression} }}}}")
    except ExtractionError:
        raise ParsingError(f"Invalid jinja expression: {expression}")

    if statically_parsed.get("refs"):
        raw_ref = list(statically_parsed["refs"])[0]
        ref_or_source = RefArgs(
            package=raw_ref.get("package"),
            name=raw_ref.get("name"),
            version=raw_ref.get("version"),
        )
    elif statically_parsed.get("sources"):
        source_name, source_table_name = list(statically_parsed["sources"])[0]
        ref_or_source = [source_name, source_table_name]
    else:
        raise ParsingError(f"Invalid ref or source expression: {expression}")

    return ref_or_source


def statically_parse_unrendered_config(string: str) -> Optional[Dict[str, Any]]:
    """
    Given a string with jinja, extract an unrendered config call.
    If no config call is present, returns None.

    For example, given:
    "{{ config(materialized=env_var('DBT_TEST_STATE_MODIFIED')) }}\nselect 1 as id"
    returns: {'materialized': "Keyword(key='materialized', value=Call(node=Name(name='env_var', ctx='load'), args=[Const(value='DBT_TEST_STATE_MODIFIED')], kwargs=[], dyn_args=None, dyn_kwargs=None))"}

    No config call:
    "select 1 as id"
    returns: None
    """
    # Return early to avoid creating jinja environemt if no config call in input string
    if "config(" not in string:
        return None

    # set 'capture_macros' to capture undefined
    env = get_environment(None, capture_macros=True)

    global _TESTING_MACRO_CACHE
    if test_caching_enabled() and _TESTING_MACRO_CACHE and string in _TESTING_MACRO_CACHE:
        parsed = _TESTING_MACRO_CACHE.get(string, None)
        func_calls = getattr(parsed, "_dbt_cached_calls")
    else:
        parsed = env.parse(string)
        func_calls = tuple(parsed.find_all(jinja2.nodes.Call))

    config_func_calls = list(
        filter(
            lambda f: hasattr(f, "node") and hasattr(f.node, "name") and f.node.name == "config",
            func_calls,
        )
    )
    # There should only be one {{ config(...) }} call per input
    config_func_call = config_func_calls[0] if config_func_calls else None

    if not config_func_call:
        return None

    unrendered_config = {}
    for kwarg in config_func_call.kwargs:
        unrendered_config[kwarg.key] = construct_static_kwarg_value(kwarg)

    return unrendered_config


def construct_static_kwarg_value(kwarg) -> str:
    # Instead of trying to re-assemble complex kwarg value, simply stringify the value.
    # This is still useful to be able to detect changes in unrendered configs, even if it is
    # not an exact representation of the user input.
    return str(kwarg)


def statically_extract_sql_header(source: str) -> Optional[str]:
    """
    Extract the unrendered template from a {% call set_sql_header(config) %} block.
    Returns the template string that should be re-rendered at runtime, or None if no
    set_sql_header block is found OR if the template contains unsupported Jinja constructs.

    This is needed to fix issue #2793 where ref(), source(), etc. in sql_header
    resolve incorrectly at parse time. By extracting and storing the unrendered template,
    we can re-render it at runtime with the correct context.

    Similar to statically_parse_unrendered_config(), but for CallBlock nodes instead
    of Call nodes.

    For example, given:
    "{% call set_sql_header(config) %}
        select * from {{ ref('my_model') }};
    {% endcall %}
    select 1 as id"

    returns: "select * from {{ ref('my_model') }};"

    No set_sql_header block:
    "select 1 as id"
    returns: None

    Unsupported Jinja construct (e.g., {% for %} loop):
    "{% call set_sql_header(config) %}
        {% for item in items %}
            select * from {{ ref(item) }};
        {% endfor %}
    {% endcall %}"
    returns: None (triggers fallback to parse-time rendering)

    Note: If None is returned due to unsupported constructs, the sql_header will be
    rendered at parse time, which may cause ref(), source(), and this to resolve
    incorrectly. Users should simplify their sql_header or report the issue.
    """
    # Return early to avoid creating jinja environment if no set_sql_header in source
    if "set_sql_header" not in source:
        return None

    # Parse the source using Jinja2 AST
    env = get_environment(None, capture_macros=True)
    try:
        parsed = env.parse(source)
    except Exception:
        # If parsing fails, return None rather than raising
        return None

    # Find all CallBlock nodes ({% call ... %}...{% endcall %})
    call_blocks = list(parsed.find_all(jinja2.nodes.CallBlock))

    for call_block in call_blocks:
        # Check if this is a call to set_sql_header
        if (
            hasattr(call_block.call, "node")
            and hasattr(call_block.call.node, "name")
            and call_block.call.node.name == "set_sql_header"
        ):
            # Extract the body content by reconstructing from AST nodes
            # The body is the template between {% call ... %} and {% endcall %}
            template_parts = []

            unsupported_node_found = False

            def extract_template_from_nodes(nodes):
                """Recursively extract template string from AST nodes.

                Currently only supports to most common node types.

                Returns False if an unsupported node type is encountered.
                """
                nonlocal unsupported_node_found

                for node in nodes:
                    # Early exit if we've hit an unsupported node
                    if unsupported_node_found:
                        return

                    if isinstance(node, jinja2.nodes.Output):
                        # Output nodes contain the actual template content
                        if hasattr(node, "nodes"):
                            extract_template_from_nodes(node.nodes)
                    elif isinstance(node, jinja2.nodes.TemplateData):
                        # Raw text/whitespace in the template
                        template_parts.append(node.data)
                    elif isinstance(node, jinja2.nodes.Call):
                        # Function call like {{ ref('model') }}
                        template_parts.append("{{ ")
                        template_parts.append(_reconstruct_jinja_call(node))
                        template_parts.append(" }}")
                    elif isinstance(node, jinja2.nodes.Name):
                        # Variable reference like {{ my_var }}
                        template_parts.append("{{ ")
                        template_parts.append(node.name)
                        template_parts.append(" }}")
                    elif isinstance(node, jinja2.nodes.Getattr):
                        # Attribute access like {{ obj.attr }}
                        template_parts.append("{{ ")
                        template_parts.append(_reconstruct_getattr(node))
                        template_parts.append(" }}")
                    elif isinstance(node, jinja2.nodes.If):
                        # {% if ... %} blocks
                        template_parts.append("{% if ")
                        template_parts.append(_reconstruct_test(node.test))
                        template_parts.append(" %}")
                        extract_template_from_nodes(node.body)
                        if node.else_:
                            template_parts.append("{% else %}")
                            extract_template_from_nodes(node.else_)
                        template_parts.append("{% endif %}")
                    elif isinstance(node, jinja2.nodes.Compare):
                        # Comparison like {% if a > b %}
                        template_parts.append(_reconstruct_comparison(node))
                    elif isinstance(node, (jinja2.nodes.And, jinja2.nodes.Or)):
                        # Boolean operators
                        template_parts.append(_reconstruct_boolean_op(node))
                    elif isinstance(node, jinja2.nodes.Not):
                        # Negation
                        template_parts.append("not ")
                        template_parts.append(_reconstruct_test(node.node))
                    else:
                        # Unsupported node type - we can't reliably reconstruct this template.
                        # This triggers fallback to parse-time rendering (existing behavior).
                        #
                        # Known unsupported constructs that trigger this:
                        # - {% for %} loops
                        # - {{ value | filter }} filters
                        # - {% set var = value %} assignments
                        # - Complex expressions
                        #
                        # If ref(), source(), or this are used within sql_header and we hit this,
                        # they will resolve incorrectly at parse time, potentially causing
                        # "relation does not exist" errors at runtime.
                        #
                        # Users experiencing this should:
                        # 1. Simplify their sql_header to use only supported constructs
                        # 2. Or report the issue so we can add support for the construct
                        #
                        # Supported: Output, TemplateData, Call, Name, Getattr, If, Compare, And, Or, Not
                        # Node type encountered: {type(node).__name__}
                        unsupported_node_found = True
                        return

            def _reconstruct_jinja_call(call_node):
                """Reconstruct a Jinja function call from AST"""
                nonlocal unsupported_node_found

                if not hasattr(call_node, "node"):
                    unsupported_node_found = True
                    return ""

                # Get function name
                func_parts = []
                if isinstance(call_node.node, jinja2.nodes.Name):
                    func_parts.append(call_node.node.name)
                elif isinstance(call_node.node, jinja2.nodes.Getattr):
                    func_parts.append(_reconstruct_getattr(call_node.node))
                else:
                    # Unknown function node type - trigger fallback
                    unsupported_node_found = True
                    return ""

                # Reconstruct arguments
                args = []
                for arg in call_node.args:
                    if isinstance(arg, jinja2.nodes.Const):
                        # String/number literal
                        args.append(repr(arg.value))
                    elif isinstance(arg, jinja2.nodes.Name):
                        # Variable reference
                        args.append(arg.name)
                    elif isinstance(arg, jinja2.nodes.Call):
                        # Nested function call
                        args.append(_reconstruct_jinja_call(arg))
                    else:
                        # Unknown argument type - trigger fallback
                        unsupported_node_found = True
                        return ""

                # Reconstruct keyword arguments
                for kwarg in call_node.kwargs:
                    key = kwarg.key
                    if isinstance(kwarg.value, jinja2.nodes.Const):
                        args.append(f"{key}={repr(kwarg.value.value)}")
                    elif isinstance(kwarg.value, jinja2.nodes.Name):
                        args.append(f"{key}={kwarg.value.name}")
                    else:
                        # Unknown kwarg value type - trigger fallback
                        unsupported_node_found = True
                        return ""

                func_parts.append(f"({', '.join(args)})")
                return "".join(func_parts)

            def _reconstruct_getattr(node):
                """Reconstruct attribute access like obj.attr"""
                nonlocal unsupported_node_found

                if isinstance(node.node, jinja2.nodes.Name):
                    return f"{node.node.name}.{node.attr}"
                elif isinstance(node.node, jinja2.nodes.Getattr):
                    return f"{_reconstruct_getattr(node.node)}.{node.attr}"
                else:
                    # Unknown node type - trigger fallback
                    unsupported_node_found = True
                    return ""

            def _reconstruct_comparison(comp_node):
                """Reconstruct comparison expressions like {{ a > b }}"""
                nonlocal unsupported_node_found

                # Comparisons have: expr (left side), ops (list of Operand objects)
                # Each Operand has: op (operator type), expr (right side expression)
                parts = []

                # Start with the left expression
                if isinstance(comp_node.expr, jinja2.nodes.Name):
                    parts.append(comp_node.expr.name)
                elif isinstance(comp_node.expr, jinja2.nodes.Call):
                    parts.append(_reconstruct_jinja_call(comp_node.expr))
                elif isinstance(comp_node.expr, jinja2.nodes.Const):
                    parts.append(repr(comp_node.expr.value))
                else:
                    # Unknown left expression type - trigger fallback
                    unsupported_node_found = True
                    return ""

                # Add operators and operands
                for operand in comp_node.ops:
                    # operand has .op and .expr
                    op_map = {
                        "eq": "==",
                        "ne": "!=",
                        "lt": "<",
                        "lteq": "<=",
                        "gt": ">",
                        "gteq": ">=",
                        "in": "in",
                        "notin": "not in",
                    }
                    op_str = op_map.get(operand.op, operand.op)
                    parts.append(f" {op_str} ")

                    # Add the right side expression
                    if isinstance(operand.expr, jinja2.nodes.Name):
                        parts.append(operand.expr.name)
                    elif isinstance(operand.expr, jinja2.nodes.Call):
                        parts.append(_reconstruct_jinja_call(operand.expr))
                    elif isinstance(operand.expr, jinja2.nodes.Const):
                        parts.append(repr(operand.expr.value))
                    else:
                        # Unknown right expression type - trigger fallback
                        unsupported_node_found = True
                        return ""

                return "".join(parts)

            def _reconstruct_boolean_op(bool_node):
                """Reconstruct boolean operators like {{ a and b }}"""
                nonlocal unsupported_node_found

                op_name = "and" if isinstance(bool_node, jinja2.nodes.And) else "or"
                parts = []

                # And/Or nodes have 'left' and 'right' attributes
                def add_operand(operand):
                    nonlocal unsupported_node_found

                    if isinstance(operand, jinja2.nodes.Name):
                        parts.append(operand.name)
                    elif isinstance(operand, jinja2.nodes.Call):
                        parts.append(_reconstruct_jinja_call(operand))
                    elif isinstance(operand, jinja2.nodes.Compare):
                        parts.append(_reconstruct_comparison(operand))
                    elif isinstance(operand, jinja2.nodes.Not):
                        parts.append("not ")
                        if isinstance(operand.node, jinja2.nodes.Name):
                            parts.append(operand.node.name)
                        elif isinstance(operand.node, jinja2.nodes.Call):
                            parts.append(_reconstruct_jinja_call(operand.node))
                        else:
                            # Unknown Not operand type - trigger fallback
                            unsupported_node_found = True
                    elif isinstance(operand, (jinja2.nodes.And, jinja2.nodes.Or)):
                        # Nested boolean operators
                        parts.append("(")
                        parts.append(_reconstruct_boolean_op(operand))
                        parts.append(")")
                    else:
                        # Unknown operand type - trigger fallback
                        unsupported_node_found = True

                add_operand(bool_node.left)
                parts.append(f" {op_name} ")
                add_operand(bool_node.right)

                return "".join(parts)

            def _reconstruct_test(test_node):
                """Reconstruct test expressions for {% if %} blocks"""
                nonlocal unsupported_node_found

                if isinstance(test_node, jinja2.nodes.Call):
                    return _reconstruct_jinja_call(test_node)
                elif isinstance(test_node, jinja2.nodes.Name):
                    return test_node.name
                elif isinstance(test_node, jinja2.nodes.Compare):
                    return _reconstruct_comparison(test_node)
                elif isinstance(test_node, (jinja2.nodes.And, jinja2.nodes.Or)):
                    return _reconstruct_boolean_op(test_node)
                elif isinstance(test_node, jinja2.nodes.Not):
                    result = "not "
                    if isinstance(test_node.node, jinja2.nodes.Name):
                        result += test_node.node.name
                    elif isinstance(test_node.node, jinja2.nodes.Call):
                        result += _reconstruct_jinja_call(test_node.node)
                    else:
                        # Unknown Not operand type - trigger fallback
                        unsupported_node_found = True
                        return ""
                    return result
                else:
                    # Unknown test type - trigger fallback
                    unsupported_node_found = True
                    return ""

            # Extract template from the CallBlock body
            extract_template_from_nodes(call_block.body)

            # If we encountered an unsupported node type, return None
            # This causes fallback to parse-time rendering (existing behavior)
            if unsupported_node_found:
                return None

            # Join and strip the result
            template = "".join(template_parts).strip()
            return template if template else None

    return None
