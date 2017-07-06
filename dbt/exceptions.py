from dbt.compat import basestring


class Exception(BaseException):
    pass


class InternalException(Exception):
    pass


class RuntimeException(RuntimeError, Exception):
    pass


class ValidationException(RuntimeException):
    pass


class CompilationException(RuntimeException):
    pass


class NotImplementedException(Exception):
    pass


class ProgrammingException(Exception):
    pass


class FailedToConnectException(Exception):
    pass


def raise_compiler_error(node, msg):
    name = '<Unknown>'

    if node is None:
        name = '<None>'
    elif isinstance(node, basestring):
        name = node
    elif isinstance(node, dict):
        name = node.get('name')
        node_type = node.get('resource_type')

        if node_type == 'macro':
            name = node.get('path')
    else:
        name = node.nice_name

    raise CompilationException(
        "! Compilation error while compiling model {}:\n! {}\n"
        .format(name, msg))


def ref_invalid_args(model, args):
    raise_compiler_error(
        model,
        "ref() takes at most two arguments ({} given)".format(
            len(args)))


def ref_bad_context(model, target_model_name, target_model_package):
    ref_string = "{{ ref('" + target_model_name + "') }}"

    if target_model_package is not None:
        ref_string = ("{{ ref('" + target_model_package +
                      "', '" + target_model_name + "') }}")

    base_error_msg = """dbt was unable to infer all dependencies for the model "{model_name}".
This typically happens when ref() is placed within a conditional block.

To fix this, add the following hint to the top of the model "{model_name}":

-- depends_on: {ref_string}"""
    error_msg = base_error_msg.format(
        model_name=model['name'],
        model_path=model['path'],
        ref_string=ref_string
    )
    raise_compiler_error(
        model, error_msg)


def ref_target_not_found(model, target_model_name, target_model_package):
    target_package_string = ''

    if target_model_package is not None:
        target_package_string = "in package '{}' ".format(target_model_package)

    raise_compiler_error(
        model,
        "Model '{}' depends on model '{}' {}which was not found."
        .format(model.get('unique_id'),
                target_model_name,
                target_package_string))


def ref_disabled_dependency(model, target_model):
    raise_compiler_error(
        model,
        "Model '{}' depends on model '{}' which is disabled in "
        "the project config".format(model.get('unique_id'),
                                    target_model.get('unique_id')))


def dependency_not_found(model, target_model_name):
    raise_compiler_error(
        model,
        "'{}' depends on '{}' which is not in the graph!"
        .format(model.get('unique_id'), target_model_name))


def macro_not_found(model, target_macro_id):
    raise_compiler_error(
        model,
        "'{}' references macro '{}' which is not defined!"
        .format(model.get('unique_id'), target_macro_id))


def missing_sql_where(model):
    raise_compiler_error(
        model,
        "Model '{}' is materialized as 'incremental', but does not have a "
        "sql_where defined in its config.".format(model.get('unique_id')))


def bad_package_spec(repo, spec, error_message):
    raise RuntimeException(
        "Error checking out spec='{}' for repo {}\n{}".format(
            spec, repo, error_message))
