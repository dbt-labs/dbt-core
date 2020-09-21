import dataclasses
from typing import List, Tuple, ClassVar, Type, TypeVar, Dict, Any

from dbt.clients.system import write_json, read_json
from dbt.exceptions import RuntimeException, IncompatibleSchemaException
from dbt.version import __version__
from hologram import JsonSchemaMixin

MacroKey = Tuple[str, str]
SourceKey = Tuple[str, str]


def list_str() -> List[str]:
    """Mypy gets upset about stuff like:

    from dataclasses import dataclass, field
    from typing import Optional, List

    @dataclass
    class Foo:
        x: Optional[List[str]] = field(default_factory=list)


    Because `list` could be any kind of list, I guess
    """
    return []


class Replaceable:
    def replace(self, **kwargs):
        return dataclasses.replace(self, **kwargs)


class Mergeable(Replaceable):
    def merged(self, *args):
        """Perform a shallow merge, where the last non-None write wins. This is
        intended to merge dataclasses that are a collection of optional values.
        """
        replacements = {}
        cls = type(self)
        for arg in args:
            for field in dataclasses.fields(cls):
                value = getattr(arg, field.name)
                if value is not None:
                    replacements[field.name] = value

        return self.replace(**replacements)


class Writable:
    def write(self, path: str, omit_none: bool = False):
        write_json(path, self.to_dict(omit_none=omit_none))  # type: ignore


class AdditionalPropertiesMixin:
    """Make this class an extensible property.

    The underlying class definition must include a type definition for a field
    named '_extra' that is of type `Dict[str, Any]`.
    """
    ADDITIONAL_PROPERTIES = True

    @classmethod
    def from_dict(cls, data, validate=True):
        self = super().from_dict(data=data, validate=validate)
        keys = self.to_dict(validate=False, omit_none=False)
        for key, value in data.items():
            if key not in keys:
                self.extra[key] = value
        return self

    def to_dict(self, omit_none=True, validate=False):
        data = super().to_dict(omit_none=omit_none, validate=validate)
        data.update(self.extra)
        return data

    def replace(self, **kwargs):
        dct = self.to_dict(omit_none=False, validate=False)
        dct.update(kwargs)
        return self.from_dict(dct)

    @property
    def extra(self):
        return self._extra


class Readable:
    @classmethod
    def read(cls, path: str):
        try:
            data = read_json(path)
        except (EnvironmentError, ValueError) as exc:
            raise RuntimeException(
                f'Could not read {cls.__name__} at "{path}" as JSON: {exc}'
            ) from exc

        return cls.from_dict(data)  # type: ignore


T = TypeVar('T', bound='VersionedSchema')


BASE_SCHEMAS_URL = 'https://schemas.getdbt.com/dbt/{name}/v{version}.json'


@dataclasses.dataclass
class SchemaVersion:
    name: str
    version: int

    def __str__(self) -> str:
        return BASE_SCHEMAS_URL.format(
            name=self.name,
            version=self.version,
        )


DBT_VERSION_KEY = 'dbt_version'
SCHEMA_VERSION_KEY = 'dbt_schema_version'


@dataclasses.dataclass
class VersionedSchema(JsonSchemaMixin, Readable, Writable):
    dbt_schema_version: ClassVar[SchemaVersion]

    def to_dict(
        self, omit_none: bool = True, validate: bool = False
    ) -> Dict[str, Any]:
        dct = super().to_dict(omit_none=omit_none, validate=validate)
        dct[SCHEMA_VERSION_KEY] = str(self.dbt_schema_version)
        dct[DBT_VERSION_KEY] = __version__
        return dct

    @classmethod
    def from_dict(
        cls: Type[T], data: Dict[str, Any], validate: bool = True
    ) -> T:
        if validate:
            expected = str(cls.dbt_schema_version)
            found = data.get(SCHEMA_VERSION_KEY)
            if found != expected:
                raise IncompatibleSchemaException(expected, found)

        return super().from_dict(data=data, validate=validate)

    @classmethod
    def _collect_json_schema(
        cls, definitions: Dict[str, Any]
    ) -> Dict[str, Any]:
        result = super()._collect_json_schema(definitions)
        result['properties'][SCHEMA_VERSION_KEY] = {
            'const': str(cls.dbt_schema_version)
        }
        result['properties'][DBT_VERSION_KEY] = {'type': 'string'}
        result['required'].extend([SCHEMA_VERSION_KEY, DBT_VERSION_KEY])
        return result

    @classmethod
    def json_schema(cls, embeddable: bool = False) -> Dict[str, Any]:
        result = super().json_schema(embeddable=embeddable)
        # it would be nice to do this in hologram!
        # in the schema itself, include the version url as $id
        if not embeddable:
            result['$id'] = str(cls.dbt_schema_version)
        return result
