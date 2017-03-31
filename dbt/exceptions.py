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


def ref_bad_context(model):
    raise_compiler_error(
        model,
        ("ref() was used in an invalid context (probably in a "
         "{% raw %} tag, or macro"))


def ref_target_not_found(model, target_model_name):
    raise_compiler_error(
        model,
        "Model '{}' depends on model '{}' which was not found."
        .format(model.get('unique_id'), target_model_name))


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
