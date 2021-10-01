import builtins
import functools
from typing import NoReturn, Optional, Mapping, Any

from dbt.logger import GLOBAL_LOGGER as logger
from dbt.node_types import NodeType
from dbt import flags
from dbt.ui import line_wrap_message, warning_tag

import dbt.dataclass_schema


def validator_error_message(exc):
    """Given a dbt.dataclass_schema.ValidationError (which is basically a
    jsonschema.ValidationError), return the relevant parts as a string
    """
    if not isinstance(exc, dbt.dataclass_schema.ValidationError):
        return str(exc)
    path = "[%s]" % "][".join(map(repr, exc.relative_path))
    return 'at path {}: {}'.format(path, exc.message)


class Exception(builtins.Exception):
    CODE = -32000
    MESSAGE = "Server Error"

    def data(self):
        # if overriding, make sure the result is json-serializable.
        return {
            'type': self.__class__.__name__,
            'message': str(self),
        }


class MacroReturn(builtins.BaseException):
    """
    Hack of all hacks
    """

    def __init__(self, value):
        self.value = value


class InternalException(Exception):
    pass


class RuntimeException(RuntimeError, Exception):
    CODE = 10001
    MESSAGE = "Runtime error"

    def __init__(self, msg, node=None):
        self.stack = []
        self.node = node
        self.msg = msg

    def add_node(self, node=None):
        if node is not None and node is not self.node:
            if self.node is not None:
                self.stack.append(self.node)
            self.node = node

    @property
    def type(self):
        return 'Runtime'

    def node_to_string(self, node):
        if node is None:
            return "<Unknown>"
        if not hasattr(node, 'name'):
            # we probably failed to parse a block, so we can't know the name
            return '{} ({})'.format(
                node.resource_type,
                node.original_file_path
            )

        if hasattr(node, 'contents'):
            # handle FileBlocks. They aren't really nodes but we want to render
            # out the path we know at least. This indicates an error during
            # block parsing.
            return '{}'.format(node.path.original_file_path)
        return "{} {} ({})".format(
            node.resource_type,
            node.name,
            node.original_file_path)

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
            split_msg = str(self.msg).split("\n")

        lines = ["{}{}".format(self.type + ' Error',
                               node_string)] + split_msg

        lines += self.process_stack()

        return lines[0] + "\n" + "\n".join(
            ["  " + line for line in lines[1:]])

    def data(self):
        result = Exception.data(self)
        if self.node is None:
            return result

        result.update({
            'raw_sql': self.node.raw_sql,
            # the node isn't always compiled, but if it is, include that!
            'compiled_sql': getattr(self.node, 'compiled_sql', None),
        })
        return result


class RPCFailureResult(RuntimeException):
    CODE = 10002
    MESSAGE = "RPC execution error"


class RPCTimeoutException(RuntimeException):
    CODE = 10008
    MESSAGE = 'RPC timeout error'

    def __init__(self, timeout):
        super().__init__(self.MESSAGE)
        self.timeout = timeout

    def data(self):
        result = super().data()
        result.update({
            'timeout': self.timeout,
            'message': 'RPC timed out after {}s'.format(self.timeout),
        })
        return result


class RPCKilledException(RuntimeException):
    CODE = 10009
    MESSAGE = 'RPC process killed'

    def __init__(self, signum):
        self.signum = signum
        self.message = 'RPC process killed by signal {}'.format(self.signum)
        super().__init__(self.message)

    def data(self):
        return {
            'signum': self.signum,
            'message': self.message,
        }


class RPCCompiling(RuntimeException):
    CODE = 10010
    MESSAGE = (
        'RPC server is compiling the project, call the "status" method for'
        ' compile status'
    )

    def __init__(self, msg=None, node=None):
        if msg is None:
            msg = 'compile in progress'
        super().__init__(msg, node)


class RPCLoadException(RuntimeException):
    CODE = 10011
    MESSAGE = (
        'RPC server failed to compile project, call the "status" method for'
        ' compile status'
    )

    def __init__(self, cause):
        self.cause = cause
        self.message = '{}: {}'.format(self.MESSAGE, self.cause['message'])
        super().__init__(self.message)

    def data(self):
        return {
            'cause': self.cause,
            'message': self.message
        }


class DatabaseException(RuntimeException):
    CODE = 10003
    MESSAGE = "Database Error"

    def process_stack(self):
        lines = []

        if hasattr(self.node, 'build_path') and self.node.build_path:
            lines.append("compiled SQL at {}".format(self.node.build_path))

        return lines + RuntimeException.process_stack(self)

    @property
    def type(self):
        return 'Database'


class CompilationException(RuntimeException):
    CODE = 10004
    MESSAGE = "Compilation Error"

    @property
    def type(self):
        return 'Compilation'


class RecursionException(RuntimeException):
    pass


class ValidationException(RuntimeException):
    CODE = 10005
    MESSAGE = "Validation Error"


class JSONValidationException(ValidationException):
    def __init__(self, typename, errors):
        self.typename = typename
        self.errors = errors
        self.errors_message = ', '.join(errors)
        msg = 'Invalid arguments passed to "{}" instance: {}'.format(
            self.typename, self.errors_message
        )
        super().__init__(msg)

    def __reduce__(self):
        # see https://stackoverflow.com/a/36342588 for why this is necessary
        return (JSONValidationException, (self.typename, self.errors))


class IncompatibleSchemaException(RuntimeException):
    def __init__(self, expected: str, found: Optional[str]):
        self.expected = expected
        self.found = found
        self.filename = 'input file'

        super().__init__(self.get_message())

    def add_filename(self, filename: str):
        self.filename = filename
        self.msg = self.get_message()

    def get_message(self) -> str:
        found_str = 'nothing'
        if self.found is not None:
            found_str = f'"{self.found}"'

        msg = (
            f'Expected a schema version of "{self.expected}" in '
            f'{self.filename}, but found {found_str}. Are you running with a '
            f'different version of dbt?'
        )
        return msg

    CODE = 10014
    MESSAGE = "Incompatible Schema"


class JinjaRenderingException(CompilationException):
    pass


class UndefinedMacroException(CompilationException):

    def __str__(self, prefix='! ') -> str:
        msg = super().__str__(prefix)
        return f'{msg}. This can happen when calling a macro that does ' \
            'not exist. Check for typos and/or install package dependencies ' \
            'with "dbt deps".'


class UnknownAsyncIDException(Exception):
    CODE = 10012
    MESSAGE = 'RPC server got an unknown async ID'

    def __init__(self, task_id):
        self.task_id = task_id

    def __str__(self):
        return '{}: {}'.format(self.MESSAGE, self.task_id)


class AliasException(ValidationException):
    pass


class DependencyException(Exception):
    # this can happen due to raise_dependency_error and its callers
    CODE = 10006
    MESSAGE = "Dependency Error"


class DbtConfigError(RuntimeException):
    CODE = 10007
    MESSAGE = "DBT Configuration Error"

    def __init__(
        self, message, project=None, result_type='invalid_project', path=None
    ):
        self.project = project
        super().__init__(message)
        self.result_type = result_type
        self.path = path

    def __str__(self, prefix='! ') -> str:
        msg = super().__str__(prefix)
        if self.path is None:
            return msg
        else:
            return f'{msg}\n\nError encountered in {self.path}'


class FailFastException(RuntimeException):
    CODE = 10013
    MESSAGE = 'FailFast Error'

    def __init__(self, message, result=None, node=None):
        super().__init__(msg=message, node=node)
        self.result = result

    @property
    def type(self):
        return 'FailFast'


class DbtProjectError(DbtConfigError):
    pass


class DbtSelectorsError(DbtConfigError):
    pass


class DbtProfileError(DbtConfigError):
    pass


class SemverException(Exception):
    def __init__(self, msg=None):
        self.msg = msg
        if msg is not None:
            super().__init__(msg)
        else:
            super().__init__()


class VersionsNotCompatibleException(SemverException):
    pass


class NotImplementedException(Exception):
    pass


class FailedToConnectException(DatabaseException):
    pass


class CommandError(RuntimeException):
    def __init__(self, cwd, cmd, message='Error running command'):
        super().__init__(message)
        self.cwd = cwd
        self.cmd = cmd
        self.args = (cwd, cmd, message)

    def __str__(self):
        if len(self.cmd) == 0:
            return '{}: No arguments given'.format(self.msg)
        return '{}: "{}"'.format(self.msg, self.cmd[0])


class ExecutableError(CommandError):
    def __init__(self, cwd, cmd, message):
        super().__init__(cwd, cmd, message)


class WorkingDirectoryError(CommandError):
    def __init__(self, cwd, cmd, message):
        super().__init__(cwd, cmd, message)

    def __str__(self):
        return '{}: "{}"'.format(self.msg, self.cwd)


class CommandResultError(CommandError):
    def __init__(self, cwd, cmd, returncode, stdout, stderr,
                 message='Got a non-zero returncode'):
        super().__init__(cwd, cmd, message)
        self.returncode = returncode
        self.stdout = stdout
        self.stderr = stderr
        self.args = (cwd, cmd, returncode, stdout, stderr, message)

    def __str__(self):
        return '{} running: {}'.format(self.msg, self.cmd)


class InvalidConnectionException(RuntimeException):
    def __init__(self, thread_id, known, node=None):
        self.thread_id = thread_id
        self.known = known
        super().__init__(
            msg='connection never acquired for thread {}, have {}'
            .format(self.thread_id, self.known)
        )


class InvalidSelectorException(RuntimeException):
    def __init__(self, name: str):
        self.name = name
        super().__init__(name)


def raise_compiler_error(msg, node=None) -> NoReturn:
    raise CompilationException(msg, node)


def raise_database_error(msg, node=None) -> NoReturn:
    raise DatabaseException(msg, node)


def raise_dependency_error(msg) -> NoReturn:
    raise DependencyException(msg)


def invalid_type_error(method_name, arg_name, got_value, expected_type,
                       version='0.13.0') -> NoReturn:
    """Raise a CompilationException when an adapter method available to macros
    has changed.
    """
    got_type = type(got_value)
    msg = ("As of {version}, 'adapter.{method_name}' expects argument "
           "'{arg_name}' to be of type '{expected_type}', instead got "
           "{got_value} ({got_type})")
    raise_compiler_error(msg.format(version=version, method_name=method_name,
                         arg_name=arg_name, expected_type=expected_type,
                         got_value=got_value, got_type=got_type))


def ref_invalid_args(model, args) -> NoReturn:
    raise_compiler_error(
        "ref() takes at most two arguments ({} given)".format(len(args)),
        model)


def ref_bad_context(model, args) -> NoReturn:
    ref_args = ', '.join("'{}'".format(a) for a in args)
    ref_string = '{{{{ ref({}) }}}}'.format(ref_args)

    base_error_msg = """dbt was unable to infer all dependencies for the model "{model_name}".
This typically happens when ref() is placed within a conditional block.

To fix this, add the following hint to the top of the model "{model_name}":

-- depends_on: {ref_string}"""
    # This explicitly references model['name'], instead of model['alias'], for
    # better error messages. Ex. If models foo_users and bar_users are aliased
    # to 'users', in their respective schemas, then you would want to see
    # 'bar_users' in your error messge instead of just 'users'.
    if isinstance(model, dict):  # TODO: remove this path
        model_name = model['name']
        model_path = model['path']
    else:
        model_name = model.name
        model_path = model.path
    error_msg = base_error_msg.format(
        model_name=model_name,
        model_path=model_path,
        ref_string=ref_string
    )
    raise_compiler_error(error_msg, model)


def doc_invalid_args(model, args) -> NoReturn:
    raise_compiler_error(
        "doc() takes at most two arguments ({} given)".format(len(args)),
        model)


def doc_target_not_found(
    model, target_doc_name: str, target_doc_package: Optional[str]
) -> NoReturn:
    target_package_string = ''

    if target_doc_package is not None:
        target_package_string = "in package '{}' ".format(target_doc_package)

    msg = (
        "Documentation for '{}' depends on doc '{}' {} which was not found"
    ).format(
        model.unique_id,
        target_doc_name,
        target_package_string
    )
    raise_compiler_error(msg, model)


def _get_target_failure_msg(
    model, target_name: str, target_model_package: Optional[str],
    include_path: bool, reason: str, target_kind: str
) -> str:
    target_package_string = ''
    if target_model_package is not None:
        target_package_string = "in package '{}' ".format(target_model_package)

    source_path_string = ''
    if include_path:
        source_path_string = ' ({})'.format(model.original_file_path)

    return "{} '{}'{} depends on a {} named '{}' {}which {}".format(
        model.resource_type.title(),
        model.unique_id,
        source_path_string,
        target_kind,
        target_name,
        target_package_string,
        reason
    )


def get_target_not_found_or_disabled_msg(
    model, target_model_name: str, target_model_package: Optional[str],
    disabled: Optional[bool] = None,
) -> str:
    if disabled is None:
        reason = 'was not found or is disabled'
    elif disabled is True:
        reason = 'is disabled'
    else:
        reason = 'was not found'
    return _get_target_failure_msg(
        model, target_model_name, target_model_package, include_path=True,
        reason=reason, target_kind='node'
    )


def ref_target_not_found(
    model,
    target_model_name: str,
    target_model_package: Optional[str],
    disabled: Optional[bool] = None,
) -> NoReturn:
    msg = get_target_not_found_or_disabled_msg(
        model, target_model_name, target_model_package, disabled
    )
    raise_compiler_error(msg, model)


def get_source_not_found_or_disabled_msg(
    model,
    target_name: str,
    target_table_name: str,
    disabled: Optional[bool] = None,
) -> str:
    full_name = f'{target_name}.{target_table_name}'
    if disabled is None:
        reason = 'was not found or is disabled'
    elif disabled is True:
        reason = 'is disabled'
    else:
        reason = 'was not found'
    return _get_target_failure_msg(
        model, full_name, None, include_path=True,
        reason=reason, target_kind='source'
    )


def source_target_not_found(
    model,
    target_name: str,
    target_table_name: str,
    disabled: Optional[bool] = None
) -> NoReturn:
    msg = get_source_not_found_or_disabled_msg(
        model, target_name, target_table_name, disabled
    )
    raise_compiler_error(msg, model)


def ref_disabled_dependency(model, target_model):
    raise_compiler_error(
        "Model '{}' depends on model '{}' which is disabled in "
        "the project config".format(model.unique_id,
                                    target_model.unique_id),
        model)


def dependency_not_found(model, target_model_name):
    raise_compiler_error(
        "'{}' depends on '{}' which is not in the graph!"
        .format(model.unique_id, target_model_name),
        model)


def macro_not_found(model, target_macro_id):
    raise_compiler_error(
        model,
        "'{}' references macro '{}' which is not defined!"
        .format(model.unique_id, target_macro_id))


def materialization_not_available(model, adapter_type):
    materialization = model.get_materialization()

    raise_compiler_error(
        "Materialization '{}' is not available for {}!"
        .format(materialization, adapter_type),
        model)


def missing_materialization(model, adapter_type):
    materialization = model.get_materialization()

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


def raise_cache_inconsistent(message):
    raise InternalException('Cache inconsistency detected: {}'.format(message))


def missing_config(model, name):
    raise_compiler_error(
        "Model '{}' does not define a required config parameter '{}'."
        .format(model.unique_id, name),
        model)


def missing_relation(relation, model=None):
    raise_compiler_error(
        "Relation {} not found!".format(relation),
        model)


def raise_dataclass_not_dict(obj):
    msg = (
        'The object ("{obj}") was used as a dictionary. This '
        'capability has been removed from objects of this type.'
    )
    raise_compiler_error(msg)


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
        "https://github.com/dbt-labs/dbt"
        .format(operation_name))


class ConnectionException(Exception):
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


def raise_duplicate_macro_name(node_1, node_2, namespace) -> NoReturn:
    duped_name = node_1.name
    if node_1.package_name != node_2.package_name:
        extra = (
            ' ("{}" and "{}" are both in the "{}" namespace)'
            .format(node_1.package_name, node_2.package_name, namespace)
        )
    else:
        extra = ''

    raise_compiler_error(
        'dbt found two macros with the name "{}" in the namespace "{}"{}. '
        'Since these macros have the same name and exist in the same '
        'namespace, dbt will be unable to decide which to call. To fix this, '
        'change the name of one of these macros:\n- {} ({})\n- {} ({})'
        .format(
            duped_name, namespace, extra,
            node_1.unique_id, node_1.original_file_path,
            node_2.unique_id, node_2.original_file_path
        )
    )


def raise_duplicate_resource_name(node_1, node_2):
    duped_name = node_1.name

    if node_1.resource_type in NodeType.refable():
        get_func = 'ref("{}")'.format(duped_name)
    elif node_1.resource_type == NodeType.Source:
        duped_name = node_1.get_full_source_name()
        get_func = node_1.get_source_representation()
    elif node_1.resource_type == NodeType.Documentation:
        get_func = 'doc("{}")'.format(duped_name)
    elif node_1.resource_type == NodeType.Test and 'schema' in node_1.tags:
        return
    else:
        get_func = '"{}"'.format(duped_name)

    raise_compiler_error(
        'dbt found two resources with the name "{}". Since these resources '
        'have the same name,\ndbt will be unable to find the correct resource '
        'when {} is used. To fix this,\nchange the name of one of '
        'these resources:\n- {} ({})\n- {} ({})'.format(
            duped_name,
            get_func,
            node_1.unique_id, node_1.original_file_path,
            node_2.unique_id, node_2.original_file_path))


def raise_ambiguous_alias(node_1, node_2, duped_name=None):
    if duped_name is None:
        duped_name = f"{node_1.database}.{node_1.schema}.{node_1.alias}"

    raise_compiler_error(
        'dbt found two resources with the database representation "{}".\ndbt '
        'cannot create two resources with identical database representations. '
        'To fix this,\nchange the configuration of one of these resources:'
        '\n- {} ({})\n- {} ({})'.format(
            duped_name,
            node_1.unique_id, node_1.original_file_path,
            node_2.unique_id, node_2.original_file_path))


def raise_ambiguous_catalog_match(unique_id, match_1, match_2):

    def get_match_string(match):
        return "{}.{}".format(
            match.get('metadata', {}).get('schema'),
            match.get('metadata', {}).get('name'))

    raise_compiler_error(
        'dbt found two relations in your warehouse with similar database '
        'identifiers. dbt\nis unable to determine which of these relations '
        'was created by the model "{unique_id}".\nIn order for dbt to '
        'correctly generate the catalog, one of the following relations must '
        'be deleted or renamed:\n\n - {match_1_s}\n - {match_2_s}'.format(
            unique_id=unique_id,
            match_1_s=get_match_string(match_1),
            match_2_s=get_match_string(match_2),
        ))


def raise_patch_targets_not_found(patches):
    patch_list = '\n\t'.join(
        'model {} (referenced in path {})'.format(p.name, p.original_file_path)
        for p in patches.values()
    )
    raise_compiler_error(
        'dbt could not find models for the following patches:\n\t{}'.format(
            patch_list
        )
    )


def _fix_dupe_msg(path_1: str, path_2: str, name: str, type_name: str) -> str:
    if path_1 == path_2:
        return (
            f'remove one of the {type_name} entries for {name} in this file:\n'
            f' - {path_1!s}\n'
        )
    else:
        return (
            f'remove the {type_name} entry for {name} in one of these files:\n'
            f' - {path_1!s}\n{path_2!s}'
        )


def raise_duplicate_patch_name(patch_1, existing_patch_path):
    name = patch_1.name
    fix = _fix_dupe_msg(
        patch_1.original_file_path,
        existing_patch_path,
        name,
        'resource',
    )
    raise_compiler_error(
        f'dbt found two schema.yml entries for the same resource named '
        f'{name}. Resources and their associated columns may only be '
        f'described a single time. To fix this, {fix}'
    )


def raise_duplicate_macro_patch_name(patch_1, existing_patch_path):
    package_name = patch_1.package_name
    name = patch_1.name
    fix = _fix_dupe_msg(
        patch_1.original_file_path,
        existing_patch_path,
        name,
        'macros'
    )
    raise_compiler_error(
        f'dbt found two schema.yml entries for the same macro in package '
        f'{package_name} named {name}. Macros may only be described a single '
        f'time. To fix this, {fix}'
    )


def raise_duplicate_source_patch_name(patch_1, patch_2):
    name = f'{patch_1.overrides}.{patch_1.name}'
    fix = _fix_dupe_msg(
        patch_1.path,
        patch_2.path,
        name,
        'sources',
    )
    raise_compiler_error(
        f'dbt found two schema.yml entries for the same source named '
        f'{patch_1.name} in package {patch_1.overrides}. Sources may only be '
        f'overridden a single time. To fix this, {fix}'
    )


def raise_invalid_schema_yml_version(path, issue):
    raise_compiler_error(
        'The schema file at {} is invalid because {}. Please consult the '
        'documentation for more information on schema.yml syntax:\n\n'
        'https://docs.getdbt.com/docs/schemayml-files'
        .format(path, issue)
    )


def raise_unrecognized_credentials_type(typename, supported_types):
    raise_compiler_error(
        'Unrecognized credentials type "{}" - supported types are ({})'
        .format(typename, ', '.join('"{}"'.format(t) for t in supported_types))
    )


def warn_invalid_patch(patch, resource_type):
    msg = line_wrap_message(
        f'''\
        '{patch.name}' is a {resource_type} node, but it is
        specified in the {patch.yaml_key} section of
        {patch.original_file_path}.
        To fix this error, place the `{patch.name}`
        specification under the {resource_type.pluralize()} key instead.
        '''
    )
    warn_or_error(msg, log_fmt=warning_tag('{}'))


def raise_not_implemented(msg):
    raise NotImplementedException(
        "ERROR: {}"
        .format(msg))


def raise_duplicate_alias(
    kwargs: Mapping[str, Any], aliases: Mapping[str, str], canonical_key: str
) -> NoReturn:
    # dupe found: go through the dict so we can have a nice-ish error
    key_names = ', '.join(
        "{}".format(k) for k in kwargs if
        aliases.get(k) == canonical_key
    )

    raise AliasException(
        f'Got duplicate keys: ({key_names}) all map to "{canonical_key}"'
    )


def warn_or_error(msg, node=None, log_fmt=None):
    if flags.WARN_ERROR:
        raise_compiler_error(msg, node)
    else:
        if log_fmt is not None:
            msg = log_fmt.format(msg)
        logger.warning(msg)


def warn_or_raise(exc, log_fmt=None):
    if flags.WARN_ERROR:
        raise exc
    else:
        msg = str(exc)
        if log_fmt is not None:
            msg = log_fmt.format(msg)
        logger.warning(msg)


def warn(msg, node=None):
    # there's no reason to expose log_fmt to macros - it's only useful for
    # handling colors
    warn_or_error(msg, node=node)
    return ""


# Update this when a new function should be added to the
# dbt context's `exceptions` key!
CONTEXT_EXPORTS = {
    fn.__name__: fn
    for fn in
    [
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
        raise_invalid_schema_yml_version,
        raise_not_implemented,
        relation_wrong_type,
    ]
}


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
    return {
        name: wrap(export) for name, export in CONTEXT_EXPORTS.items()
    }
