from dataclasses import field, Field, dataclass
from enum import Enum
from typing import (
    Any, List, Optional, Dict, MutableMapping, Union, Type, NewType, Tuple,
    TypeVar
)

# TODO: patch+upgrade hologram to avoid this jsonschema import
import jsonschema  # type: ignore

# This is protected, but we really do want to reuse this logic, and the cache!
# It would be nice to move the custom error picking stuff into hologram!
from hologram import _validate_schema
from hologram import JsonSchemaMixin, ValidationError
from hologram.helpers import StrEnum, register_pattern

from dbt.contracts.graph.unparsed import AdditionalPropertiesAllowed
from dbt.exceptions import CompilationException, InternalException
from dbt.contracts.util import Replaceable, list_str
from dbt import hooks
from dbt.node_types import NodeType


def _get_meta_value(cls: Type[Enum], fld: Field, key: str, default: Any):
    # a metadata field might exist. If it does, it might have a matching key.
    # If it has both, make sure the value is valid and return it. If it
    # doesn't, return the default.
    if fld.metadata:
        value = fld.metadata.get(key, default)
    else:
        value = default

    try:
        return cls(value)
    except ValueError as exc:
        raise InternalException(
            f'Invalid {cls} value: {value}'
        ) from exc


def _set_meta_value(
    obj: Enum, key: str, existing: Optional[Dict[str, Any]] = None
) -> Dict[str, Any]:
    if existing is None:
        result = {}
    else:
        result = existing.copy()
    result.update({key: obj})
    return result


MERGE_KEY = 'merge'


class MergeBehavior(Enum):
    Append = 1
    Update = 2
    Clobber = 3

    @classmethod
    def from_field(cls, fld: Field) -> 'MergeBehavior':
        return _get_meta_value(cls, fld, MERGE_KEY, cls.Clobber)

    def meta(self, existing: Optional[Dict[str, Any]] = None):
        return _set_meta_value(self, MERGE_KEY, existing)


SHOW_HIDE_KEY = 'show_hide'


class ShowBehavior(Enum):
    Show = 1
    Hide = 2

    @classmethod
    def from_field(cls, fld: Field) -> 'ShowBehavior':
        return _get_meta_value(cls, fld, SHOW_HIDE_KEY, cls.Show)

    def meta(self, existing: Optional[Dict[str, Any]] = None):
        return _set_meta_value(self, SHOW_HIDE_KEY, existing)


def _listify(value: Any) -> List:
    if isinstance(value, list):
        return value[:]
    else:
        return [value]


def _merge_field_value(
    merge_behavior: MergeBehavior,
    self_value: Any,
    other_value: Any,
):
    if merge_behavior == MergeBehavior.Clobber:
        return other_value
    elif merge_behavior == MergeBehavior.Append:
        return _listify(self_value) + _listify(other_value)
    elif merge_behavior == MergeBehavior.Update:
        if not isinstance(self_value, dict):
            raise InternalException(f'expected dict, got {self_value}')
        if not isinstance(other_value, dict):
            raise InternalException(f'expected dict, got {other_value}')
        value = self_value.copy()
        value.update(other_value)
        return value
    else:
        raise InternalException(
            f'Got an invalid merge_behavior: {merge_behavior}'
        )


def insensitive_patterns(*patterns: str):
    lowercased = []
    for pattern in patterns:
        lowercased.append(
            ''.join('[{}{}]'.format(s.upper(), s.lower()) for s in pattern)
        )
    return '^({})$'.format('|'.join(lowercased))


Severity = NewType('Severity', str)

register_pattern(Severity, insensitive_patterns('warn', 'error'))


class SnapshotStrategy(StrEnum):
    Timestamp = 'timestamp'
    Check = 'check'


class All(StrEnum):
    All = 'all'


@dataclass
class Hook(JsonSchemaMixin, Replaceable):
    sql: str
    transaction: bool = True
    index: Optional[int] = None


T = TypeVar('T', bound='BaseConfig')


@dataclass
class BaseConfig(
    AdditionalPropertiesAllowed, Replaceable, MutableMapping[str, Any]
):
    # Implement MutableMapping so this config will behave as some macros expect
    # during parsing (notably, syntax like `{{ node.config['schema'] }}`)
    def __getitem__(self, key):
        """Handle parse-time use of `config` as a dictionary, making the extra
        values available during parsing.
        """
        if hasattr(self, key):
            return getattr(self, key)
        else:
            return self._extra[key]

    def __setitem__(self, key, value):
        if hasattr(self, key):
            setattr(self, key, value)
        else:
            self._extra[key] = value

    def __delitem__(self, key):
        if hasattr(self, key):
            msg = (
                'Error, tried to delete config key "{}": Cannot delete '
                'built-in keys'
            ).format(key)
            raise CompilationException(msg)
        else:
            del self._extra[key]

    def __iter__(self):
        for fld, _ in self._get_fields():
            yield fld.name

        for key in self._extra:
            yield key

    def __len__(self):
        return len(self._get_fields()) + len(self._extra)

    @classmethod
    def _extract_dict(
        cls, src: Dict[str, Any], data: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Find all the items in data that match a target_field on this class,
        and merge them with the data found in `src` for target_field, using the
        field's specified merge behavior. Matching items will be removed from
        `data` (but _not_ `src`!).

        Returns a dict with the merge results.

        That means this method mutates its input! Any remaining values in data
        were not merged.
        """
        result = {}

        for fld, target_field in cls._get_fields():
            if target_field not in data:
                continue

            data_attr = data.pop(target_field)
            if target_field not in src:
                result[target_field] = data_attr
                continue

            merge_behavior = MergeBehavior.from_field(fld)
            self_attr = src[target_field]

            result[target_field] = _merge_field_value(
                merge_behavior=merge_behavior,
                self_value=self_attr,
                other_value=data_attr,
            )
        return result

    def to_dict(
        self,
        omit_none: bool = True,
        validate: bool = False,
        *,
        omit_hidden: bool = True,
    ) -> Dict[str, Any]:
        result = super().to_dict(omit_none=omit_none, validate=validate)
        if omit_hidden and not omit_none:
            for fld, target_field in self._get_fields():
                if target_field not in result:
                    continue

                # if the field is not None, preserve it regardless of the
                # setting. This is in line with existing behavior, but isn't
                # an endorsement of it!
                if result[target_field] is not None:
                    continue

                show_behavior = ShowBehavior.from_field(fld)
                if show_behavior == ShowBehavior.Hide:
                    del result[target_field]
        return result

    def update_from(
        self: T, data: Dict[str, Any], adapter_type: str, validate: bool = True
    ) -> T:
        """Given a dict of keys, update the current config from them, validate
        it, and return a new config with the updated values
        """
        # sadly, this is a circular import
        from dbt.adapters.factory import get_config_class_by_name
        dct = self.to_dict(omit_none=False, validate=False, omit_hidden=False)

        adapter_config_cls = get_config_class_by_name(adapter_type)

        self_merged = self._extract_dict(dct, data)
        dct.update(self_merged)

        adapter_merged = adapter_config_cls._extract_dict(dct, data)
        dct.update(adapter_merged)

        # any remaining fields must be "clobber"
        dct.update(data)

        # any validation failures must have come from the update
        return self.from_dict(dct, validate=validate)

    def finalize_and_validate(self: T) -> T:
        # from_dict will validate for us
        dct = self.to_dict(omit_none=False, validate=False)
        return self.from_dict(dct)


@dataclass
class SourceConfig(BaseConfig):
    enabled: bool = True


@dataclass
class NodeConfig(BaseConfig):
    enabled: bool = True
    materialized: str = 'view'
    persist_docs: Dict[str, Any] = field(default_factory=dict)
    post_hook: List[Hook] = field(
        default_factory=list,
        metadata=MergeBehavior.Append.meta(),
    )
    pre_hook: List[Hook] = field(
        default_factory=list,
        metadata=MergeBehavior.Append.meta(),
    )
    vars: Dict[str, Any] = field(
        default_factory=dict,
        metadata=MergeBehavior.Update.meta(),
    )
    quoting: Dict[str, Any] = field(
        default_factory=dict,
        metadata=MergeBehavior.Update.meta(),
    )
    # This is actually only used by seeds. Should it be available to others?
    # That would be a breaking change!
    column_types: Dict[str, Any] = field(
        default_factory=dict,
        metadata=MergeBehavior.Update.meta(),
    )
    # these fields are all config-only (they're ultimately applied to the node)
    alias: Optional[str] = field(
        default=None,
        metadata=ShowBehavior.Hide.meta(),
    )
    schema: Optional[str] = field(
        default=None,
        metadata=ShowBehavior.Hide.meta(),
    )
    database: Optional[str] = field(
        default=None,
        metadata=ShowBehavior.Hide.meta(),
    )
    tags: Union[List[str], str] = field(
        default_factory=list_str,
        # TODO: hide this one?
        metadata=MergeBehavior.Append.meta(),
    )
    full_refresh: Optional[bool] = None

    @classmethod
    def from_dict(cls, data, validate=True):
        for key in hooks.ModelHookType:
            if key in data:
                data[key] = [hooks.get_hook_dict(h) for h in data[key]]
        return super().from_dict(data, validate=validate)

    @classmethod
    def field_mapping(cls):
        return {'post_hook': 'post-hook', 'pre_hook': 'pre-hook'}


@dataclass
class SeedConfig(NodeConfig):
    materialized: str = 'seed'
    quote_columns: Optional[bool] = None


@dataclass
class TestConfig(NodeConfig):
    severity: Severity = Severity('ERROR')


SnapshotVariants = Union[
    'TimestampSnapshotConfig',
    'CheckSnapshotConfig',
    'GenericSnapshotConfig',
]


def _relevance_without_strategy(error: jsonschema.ValidationError):
    # calculate the 'relevance' of an error the normal jsonschema way, except
    # if the validator is in the 'strategy' field and its conflicting with the
    # 'enum'. This suppresses `"'timestamp' is not one of ['check']` and such
    if 'strategy' in error.path and error.validator in {'enum', 'not'}:
        length = 1
    else:
        length = -len(error.path)
    validator = error.validator
    return length, validator not in {'anyOf', 'oneOf'}


@dataclass
class SnapshotWrapper(JsonSchemaMixin):
    """This is a little wrapper to let us serialize/deserialize the
    SnapshotVariants union.
    """
    config: SnapshotVariants  # mypy: ignore

    @classmethod
    def validate(cls, data: Any):
        schema = _validate_schema(cls)
        validator = jsonschema.Draft7Validator(schema)
        error = jsonschema.exceptions.best_match(
            validator.iter_errors(data),
            key=_relevance_without_strategy,
        )
        if error is not None:
            raise ValidationError.create_from(error) from error


@dataclass
class EmptySnapshotConfig(NodeConfig):
    materialized: str = 'snapshot'


@dataclass(init=False)
class SnapshotConfig(EmptySnapshotConfig):
    unique_key: str = field(init=False, metadata=dict(init_required=True))
    target_schema: str = field(init=False, metadata=dict(init_required=True))
    target_database: Optional[str] = None

    def __init__(
        self,
        unique_key: str,
        target_schema: str,
        target_database: Optional[str] = None,
        **kwargs
    ) -> None:
        self.unique_key = unique_key
        self.target_schema = target_schema
        self.target_database = target_database
        # kwargs['materialized'] = materialized
        super().__init__(**kwargs)

    # type hacks...
    @classmethod
    def _get_fields(cls) -> List[Tuple[Field, str]]:  # type: ignore
        fields: List[Tuple[Field, str]] = []
        for old_field, name in super()._get_fields():
            new_field = old_field
            # tell hologram we're really an initvar
            if old_field.metadata and old_field.metadata.get('init_required'):
                new_field = field(init=True, metadata=old_field.metadata)
                new_field.name = old_field.name
                new_field.type = old_field.type
                new_field._field_type = old_field._field_type  # type: ignore
            fields.append((new_field, name))
        return fields

    def finalize_and_validate(self: 'SnapshotConfig') -> SnapshotVariants:
        data = self.to_dict()
        return SnapshotWrapper.from_dict({'config': data}).config


@dataclass(init=False)
class GenericSnapshotConfig(SnapshotConfig):
    strategy: str = field(init=False, metadata=dict(init_required=True))

    def __init__(self, strategy: str, **kwargs) -> None:
        self.strategy = strategy
        super().__init__(**kwargs)

    @classmethod
    def _collect_json_schema(
        cls, definitions: Dict[str, Any]
    ) -> Dict[str, Any]:
        # this is the method you want to override in hologram if you want
        # to do clever things about the json schema and have classes that
        # contain instances of your JsonSchemaMixin respect the change.
        schema = super()._collect_json_schema(definitions)

        # Instead of just the strategy we'd calculate normally, say
        # "this strategy except none of our specialization strategies".
        strategies = [schema['properties']['strategy']]
        for specialization in (TimestampSnapshotConfig, CheckSnapshotConfig):
            strategies.append(
                {'not': specialization.json_schema()['properties']['strategy']}
            )

        schema['properties']['strategy'] = {
            'allOf': strategies
        }
        return schema


@dataclass(init=False)
class TimestampSnapshotConfig(SnapshotConfig):
    strategy: str = field(
        init=False,
        metadata=dict(
            restrict=[str(SnapshotStrategy.Timestamp)],
            init_required=True,
        ),
    )
    updated_at: str = field(init=False, metadata=dict(init_required=True))

    def __init__(
        self, strategy: str, updated_at: str, **kwargs
    ) -> None:
        self.strategy = strategy
        self.updated_at = updated_at
        super().__init__(**kwargs)


@dataclass(init=False)
class CheckSnapshotConfig(SnapshotConfig):
    strategy: str = field(
        init=False,
        metadata=dict(
            restrict=[str(SnapshotStrategy.Check)],
            init_required=True,
        ),
    )
    # TODO: is there a way to get this to accept tuples of strings? Adding
    # `Tuple[str, ...]` to the list of types results in this:
    # ['email'] is valid under each of {'type': 'array', 'items':
    # {'type': 'string'}}, {'type': 'array', 'items': {'type': 'string'}}
    # but without it, parsing gets upset about values like `('email',)`
    # maybe hologram itself should support this behavior? It's not like tuples
    # are meaningful in json
    check_cols: Union[All, List[str]] = field(
        init=False,
        metadata=dict(init_required=True),
    )

    def __init__(
        self, strategy: str, check_cols: Union[All, List[str]],
        **kwargs
    ) -> None:
        self.strategy = strategy
        self.check_cols = check_cols
        super().__init__(**kwargs)


RESOURCE_TYPES: Dict[NodeType, Type[BaseConfig]] = {
    NodeType.Source: SourceConfig,
    NodeType.Seed: SeedConfig,
    NodeType.Test: TestConfig,
    NodeType.Model: NodeConfig,
    NodeType.Snapshot: SnapshotConfig,
}


# base resource types are like resource types, except nothing has mandatory
# configs.
BASE_RESOURCE_TYPES: Dict[NodeType, Type[BaseConfig]] = RESOURCE_TYPES.copy()
BASE_RESOURCE_TYPES.update({
    NodeType.Snapshot: EmptySnapshotConfig
})


def get_config_for(resource_type: NodeType, base=False) -> Type[BaseConfig]:
    if base:
        lookup = BASE_RESOURCE_TYPES
    else:
        lookup = RESOURCE_TYPES
    return lookup.get(resource_type, NodeConfig)
