from typing import List, Mapping, Any

from dbt.common.exceptions import CompilationError, DbtDatabaseError
from dbt.common.ui import line_wrap_message


class MissingConfigError(CompilationError):
    def __init__(self, unique_id: str, name: str):
        self.unique_id = unique_id
        self.name = name
        msg = (
            f"Model '{self.unique_id}' does not define a required config parameter '{self.name}'."
        )
        super().__init__(msg=msg)


class MultipleDatabasesNotAllowedError(CompilationError):
    def __init__(self, databases):
        self.databases = databases
        super().__init__(msg=self.get_message())

    def get_message(self) -> str:
        msg = str(self.databases)
        return msg


class ApproximateMatchError(CompilationError):
    def __init__(self, target, relation):
        self.target = target
        self.relation = relation
        super().__init__(msg=self.get_message())

    def get_message(self) -> str:
        msg = (
            "When searching for a relation, dbt found an approximate match. "
            "Instead of guessing \nwhich relation to use, dbt will move on. "
            f"Please delete {self.relation}, or rename it to be less ambiguous."
            f"\nSearched for: {self.target}\nFound: {self.relation}"
        )

        return msg


class SnapshotTargetIncompleteError(CompilationError):
    def __init__(self, extra: List, missing: List):
        self.extra = extra
        self.missing = missing
        super().__init__(msg=self.get_message())

    def get_message(self) -> str:
        msg = (
            'Snapshot target has ("{}") but not ("{}") - is it an '
            "unmigrated previous version archive?".format(
                '", "'.join(self.extra), '", "'.join(self.missing)
            )
        )
        return msg


class DuplicateMacroInPackageError(CompilationError):
    def __init__(self, macro, macro_mapping: Mapping):
        self.macro = macro
        self.macro_mapping = macro_mapping
        super().__init__(msg=self.get_message())

    def get_message(self) -> str:
        other_path = self.macro_mapping[self.macro.unique_id].original_file_path
        # subtract 2 for the "Compilation Error" indent
        # note that the line wrap eats newlines, so if you want newlines,
        # this is the result :(
        msg = line_wrap_message(
            f"""\
            dbt found two macros named "{self.macro.name}" in the project
            "{self.macro.package_name}".


            To fix this error, rename or remove one of the following
            macros:

                - {self.macro.original_file_path}

                - {other_path}
            """,
            subtract=2,
        )
        return msg


class DuplicateMaterializationNameError(CompilationError):
    def __init__(self, macro, other_macro):
        self.macro = macro
        self.other_macro = other_macro
        super().__init__(msg=self.get_message())

    def get_message(self) -> str:
        macro_name = self.macro.name
        macro_package_name = self.macro.package_name
        other_package_name = self.other_macro.macro.package_name

        msg = (
            f"Found two materializations with the name {macro_name} (packages "
            f"{macro_package_name} and {other_package_name}). dbt cannot resolve "
            "this ambiguity"
        )
        return msg


class ColumnTypeMissingError(CompilationError):
    def __init__(self, column_names: List):
        self.column_names = column_names
        super().__init__(msg=self.get_message())

    def get_message(self) -> str:
        msg = (
            "Contracted models require data_type to be defined for each column. "
            "Please ensure that the column name and data_type are defined within "
            f"the YAML configuration for the {self.column_names} column(s)."
        )
        return msg


class MacroNotFoundError(CompilationError):
    def __init__(self, node, target_macro_id: str):
        self.node = node
        self.target_macro_id = target_macro_id
        msg = f"'{self.node.unique_id}' references macro '{self.target_macro_id}' which is not defined!"

        super().__init__(msg=msg)


class MissingMaterializationError(CompilationError):
    def __init__(self, materialization, adapter_type):
        self.materialization = materialization
        self.adapter_type = adapter_type
        super().__init__(msg=self.get_message())

    def get_message(self) -> str:
        valid_types = "'default'"

        if self.adapter_type != "default":
            valid_types = f"'default' and '{self.adapter_type}'"

        msg = f"No materialization '{self.materialization}' was found for adapter {self.adapter_type}! (searched types {valid_types})"
        return msg


class SnapshotTargetNotSnapshotTableError(CompilationError):
    def __init__(self, missing: List):
        self.missing = missing
        super().__init__(msg=self.get_message())

    def get_message(self) -> str:
        msg = 'Snapshot target is not a snapshot table (missing "{}")'.format(
            '", "'.join(self.missing)
        )
        return msg


class NullRelationDropAttemptedError(CompilationError):
    def __init__(self, name: str):
        self.name = name
        self.msg = f"Attempted to drop a null relation for {self.name}"
        super().__init__(msg=self.msg)


class NullRelationCacheAttemptedError(CompilationError):
    def __init__(self, name: str):
        self.name = name
        self.msg = f"Attempted to cache a null relation for {self.name}"
        super().__init__(msg=self.msg)


class RelationTypeNullError(CompilationError):
    def __init__(self, relation):
        self.relation = relation
        self.msg = f"Tried to drop relation {self.relation}, but its type is null."
        super().__init__(msg=self.msg)


class MaterializationNotAvailableError(CompilationError):
    def __init__(self, materialization, adapter_type: str):
        self.materialization = materialization
        self.adapter_type = adapter_type
        super().__init__(msg=self.get_message())

    def get_message(self) -> str:
        msg = f"Materialization '{self.materialization}' is not available for {self.adapter_type}!"
        return msg


class RelationReturnedMultipleResultsError(CompilationError):
    def __init__(self, kwargs: Mapping[str, Any], matches: List):
        self.kwargs = kwargs
        self.matches = matches
        super().__init__(msg=self.get_message())

    def get_message(self) -> str:
        msg = (
            "get_relation returned more than one relation with the given args. "
            "Please specify a database or schema to narrow down the result set."
            f"\n{self.kwargs}\n\n{self.matches}"
        )
        return msg


class UnexpectedNonTimestampError(DbtDatabaseError):
    def __init__(self, field_name: str, source, dt: Any):
        self.field_name = field_name
        self.source = source
        self.type_name = type(dt).__name__
        msg = (
            f"Expected a timestamp value when querying field '{self.field_name}' of table "
            f"{self.source} but received value of type '{self.type_name}' instead"
        )
        super().__init__(msg)


class RenameToNoneAttemptedError(CompilationError):
    def __init__(self, src_name: str, dst_name: str, name: str):
        self.src_name = src_name
        self.dst_name = dst_name
        self.name = name
        self.msg = f"Attempted to rename {self.src_name} to {self.dst_name} for {self.name}"
        super().__init__(msg=self.msg)


class QuoteConfigTypeError(CompilationError):
    def __init__(self, quote_config: Any):
        self.quote_config = quote_config
        super().__init__(msg=self.get_message())

    def get_message(self) -> str:
        msg = (
            'The seed configuration value of "quote_columns" has an '
            f"invalid type {type(self.quote_config)}"
        )
        return msg


class RelationWrongTypeError(CompilationError):
    def __init__(self, relation, expected_type, model=None):
        self.relation = relation
        self.expected_type = expected_type
        self.model = model
        super().__init__(msg=self.get_message())

    def get_message(self) -> str:
        msg = (
            f"Trying to create {self.expected_type} {self.relation}, "
            f"but it currently exists as a {self.relation.type}. Either "
            f"drop {self.relation} manually, or run dbt with "
            "`--full-refresh` and dbt will drop it for you."
        )

        return msg
