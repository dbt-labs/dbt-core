from dbt.compat import basestring
from dbt.logger import GLOBAL_LOGGER as logger
import re


class Exception(BaseException):
    pass


class MacroReturn(BaseException):
    """
    Hack of all hacks
    """

    def __init__(self, value):
        self.value = value


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
        if node is None:
            return "<Unknown>"

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

        if hasattr(self.msg, 'split'):
            split_msg = self.msg.split("\n")
        else:
            split_msg = basestring(self.msg).split("\n")

        lines = ["{}{}".format(self.type + ' Error',
                               node_string)] + split_msg

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


class SemverException(Exception):
    def __init__(self, msg=None):
        self.msg = msg


class VersionsNotCompatibleException(SemverException):
    pass


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
    # This explicitly references model['name'], instead of model['alias'], for
    # better error messages. Ex. If models foo_users and bar_users are aliased
    # to 'users', in their respective schemas, then you would want to see
    # 'bar_users' in your error messge instead of just 'users'.
    error_msg = base_error_msg.format(
        model_name=model['name'],
        model_path=model['path'],
        ref_string=ref_string
    )
    raise_compiler_error(error_msg, model)


def get_target_not_found_msg(model, target_model_name, target_model_package):
    target_package_string = ''

    if target_model_package is not None:
        target_package_string = "in package '{}' ".format(target_model_package)

    return ("Model '{}' depends on model '{}' {}which was not found or is"
            " disabled".format(model.get('unique_id'),
                               target_model_name,
                               target_package_string))


def ref_target_not_found(model, target_model_name, target_model_package):
    msg = get_target_not_found_msg(model, target_model_name,
                                   target_model_package)
    raise_compiler_error(msg, model)


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


def missing_relation(relation, model=None):
    raise_compiler_error(
        "Relation {} not found!".format(relation),
        model)


def relation_wrong_type(relation, expected_type, model=None):
    raise_compiler_error(
        ('Trying to create {expected_type} {relation}, '
         'but it currently exists as a {current_type}. Either '
         'drop {relation} manually, or run dbt with '
         '`--full-refresh` and dbt will drop it for you.')
        .format(relation=relation,
                current_type=relation.type,
                expected_type=expected_type),
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


def invalid_materialization_argument(name, argument):
    raise_compiler_error(
        "materialization '{}' received unknown argument '{}'."
        .format(name, argument))


def system_error(operation_name):
    raise_compiler_error(
        "dbt encountered an error when attempting to {}. "
        "If this error persists, please create an issue at: \n\n"
        "https://github.com/fishtown-analytics/dbt"
        .format(operation_name))


class RegistryException(Exception):
    pass


def raise_dep_not_found(node, node_description, required_pkg):
    raise_compiler_error(
        'Error while parsing {}.\nThe required package "{}" was not found. '
        'Is the package installed?\nHint: You may need to run '
        '`dbt deps`.'.format(node_description, required_pkg), node=node)


def multiple_matching_relations(kwargs, matches):
    raise_compiler_error(
        'get_relation returned more than one relation with the given args. '
        'Please specify a database or schema to narrow down the result set.'
        '\n{}\n\n{}'
        .format(kwargs, matches))


def get_relation_returned_multiple_results(kwargs, matches):
    multiple_matching_relations(kwargs, matches)


def approximate_relation_match(target, relation):
    raise_compiler_error(
        'When searching for a relation, dbt found an approximate match. '
        'Instead of guessing \nwhich relation to use, dbt will move on. '
        'Please delete {relation}, or rename it to be less ambiguous.'
        '\nSearched for: {target}\nFound: {relation}'
        .format(target=target,
                relation=relation))


def raise_duplicate_resource_name(node_1, node_2):
    duped_name = node_1['name']

    raise_compiler_error(
        'dbt found two resources with the name "{}". Since these resources '
        'have the same name,\ndbt will be unable to find the correct resource '
        'when ref("{}") is used. To fix this,\nchange the name of one of '
        'these resources:\n- {} ({})\n- {} ({})'.format(
            duped_name,
            duped_name,
            node_1['unique_id'], node_1['original_file_path'],
            node_2['unique_id'], node_2['original_file_path']))


def raise_ambiguous_alias(node_1, node_2):
    duped_name = "{}.{}".format(node_1['schema'], node_1['alias'])

    raise_compiler_error(
        'dbt found two resources with the database representation "{}".\ndbt '
        'cannot create two resources with identical database representations. '
        'To fix this,\nchange the "schema" or "alias" configuration of one of '
        'these resources:\n- {} ({})\n- {} ({})'.format(
            duped_name,
            node_1['unique_id'], node_1['original_file_path'],
            node_2['unique_id'], node_2['original_file_path']))
