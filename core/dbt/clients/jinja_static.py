import jinja2
from dbt.clients.jinja import get_environment
from dbt.exceptions import raise_compiler_error


def statically_extract_macro_calls(string, ctx, db_wrapper=None):
    # set 'capture_macros' to capture undefined
    env = get_environment(None, capture_macros=True)
    parsed = env.parse(string)

    standard_calls = ["source", "ref", "config"]
    possible_macro_calls = []
    for func_call in parsed.find_all(jinja2.nodes.Call):
        func_name = None
        if hasattr(func_call, "node") and hasattr(func_call.node, "name"):
            func_name = func_call.node.name
        else:
            # func_call for dbt_utils.current_timestamp macro
            # Call(
            #   node=Getattr(
            #     node=Name(
            #       name='dbt_utils',
            #       ctx='load'
            #     ),
            #     attr='current_timestamp',
            #     ctx='load
            #   ),
            #   args=[],
            #   kwargs=[],
            #   dyn_args=None,
            #   dyn_kwargs=None
            # )
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
                    raise_compiler_error(
                        f"The macro_name parameter ({kwarg.value.value}) "
                        "to adapter.dispatch was not a string"
                    )
            elif kwarg.key == "macro_namespace":
                # This will remain to enable static resolution
                kwarg_type = type(kwarg.value).__name__
                if kwarg_type == "Const":
                    macro_namespace = kwarg.value.value
                else:
                    raise_compiler_error(
                        "The macro_namespace parameter to adapter.dispatch "
                        f"is a {kwarg_type}, not a string"
                    )

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
    else:  # this is only for test/unit/test_macro_calls.py
        if macro_namespace:
            packages = [macro_namespace]
        else:
            packages = []
        for package_name in packages:
            possible_macro_calls.append(f"{package_name}.{func_name}")

    return possible_macro_calls
