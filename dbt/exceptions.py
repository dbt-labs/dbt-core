from dbt.compat import basestring


class Exception(BaseException):
    pass


class InternalException(Exception):
    pass


class RuntimeException(RuntimeError, Exception):
    def __init__(self, msg, node=None):
        self.stack = []
        self.node = node
        self.msg = msg

    @property
    def type(self):
        return 'Runtime'

    def node_to_string(self, node):
        return "{} {} ({})".format(
            node.get('resource_type'),
            node.get('name', 'unknown'),
            node.get('original_file_path'))

    def process_stack(self):
        lines = []
        stack = self.stack + [self.node]
        first = True

        if len(stack) > 1:
            lines.append("")

            for item in stack:
                msg = 'called by'

                if first:
                    msg = 'in'
                    first = False

                lines.append("> {} {}".format(
                    msg,
                    self.node_to_string(item)))

        return lines

    def __str__(self, prefix="! "):
        node_string = ""

        if self.node is not None:
            node_string = " in {}".format(self.node_to_string(self.node))

        lines = ["{}{}".format(self.type + ' Error',
                               node_string)] + \
            self.msg.split("\n")

        lines += self.process_stack()

        return lines[0] + "\n" + "\n".join(
            ["  " + line for line in lines[1:]])


class DatabaseException(RuntimeException):

    def process_stack(self):
        lines = []

        if self.node is not None and self.node.get('build_path'):
            lines.append(
                "compiled SQL at {}".format(self.node.get('build_path')))

        return lines + RuntimeException.process_stack(self)

    @property
    def type(self):
        return 'Database'


class CompilationException(RuntimeException):
    @property
    def type(self):
        return 'Compilation'


class ValidationException(RuntimeException):
    pass


class ParsingException(Exception):
    pass


class DependencyException(Exception):
    pass


class VersionsNotCompatibleException(ParsingException):
    def __init__(self, msg=None):
        self.msg = msg


class NotImplementedException(Exception):
    pass


class FailedToConnectException(DatabaseException):
    pass


from dbt.utils import get_materialization  # noqa


def raise_compiler_error(msg, node=None):
    raise CompilationException(msg, node)


def raise_database_error(msg, node=None):
    raise DatabaseException(msg, node)


def raise_dependency_error(msg):
    raise DependencyException(msg)


def ref_invalid_args(model, args):
    raise_compiler_error(
        "ref() takes at most two arguments ({} given)".format(len(args)),
        model)


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
    raise_compiler_error(error_msg, model)


def ref_target_not_found(model, target_model_name, target_model_package):
    target_package_string = ''

    if target_model_package is not None:
        target_package_string = "in package '{}' ".format(target_model_package)

    raise_compiler_error(
        "Model '{}' depends on model '{}' {}which was not found."
        .format(model.get('unique_id'),
                target_model_name,
                target_package_string),
        model)


def ref_disabled_dependency(model, target_model):
    raise_compiler_error(
        "Model '{}' depends on model '{}' which is disabled in "
        "the project config".format(model.get('unique_id'),
                                    target_model.get('unique_id')),
        model)


def dependency_not_found(model, target_model_name):
    raise_compiler_error(
        "'{}' depends on '{}' which is not in the graph!"
        .format(model.get('unique_id'), target_model_name),
        model)


def macro_not_found(model, target_macro_id):
    raise_compiler_error(
        model,
        "'{}' references macro '{}' which is not defined!"
        .format(model.get('unique_id'), target_macro_id))


def materialization_not_available(model, adapter_type):
    materialization = get_materialization(model)

    raise_compiler_error(
        "Materialization '{}' is not available for {}!"
        .format(materialization, adapter_type),
        model)


def missing_materialization(model, adapter_type):
    materialization = get_materialization(model)

    valid_types = "'default'"

    if adapter_type != 'default':
        valid_types = "'default' and '{}'".format(adapter_type)

    raise_compiler_error(
        "No materialization '{}' was found for adapter {}! (searched types {})"
        .format(materialization, adapter_type, valid_types),
        model)


def bad_package_spec(repo, spec, error_message):
    raise InternalException(
        "Error checking out spec='{}' for repo {}\n{}".format(
            spec, repo, error_message))


def missing_config(model, name):
    raise_compiler_error(
        "Model '{}' does not define a required config parameter '{}'."
        .format(model.get('unique_id'), name),
        model)


def missing_relation(relation_name, model=None):
    raise_compiler_error(
        "Relation {} not found!".format(relation_name),
        model)


def package_not_found(package_name):
    raise_dependency_error(
        "Package {} was not found in the package index".format(package_name))


def package_version_not_found(package_name, version_range, available_versions):
    base_msg = ('Could not find a matching version for package {}\n'
                '  Requested range: {}\n'
                '  Available versions: {}')

    raise_dependency_error(base_msg.format(package_name,
                                           version_range,
                                           available_versions))


def incompatible_versions(package_name, versions):
    version_list = ", ".join(versions)

    raise VersionsNotCompatibleException(
        'Could not find a satisfactory version of {}:\n'
        'Specified versions: [{}]'
        .format(package_name, version_list))


def invalid_materialization_argument(name, argument):
    raise_compiler_error(
        "materialization '{}' received unknown argument '{}'."
        .format(name, argument))
