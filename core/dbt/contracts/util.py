import dataclasses
from datetime import datetime
from typing import List, Tuple, ClassVar, Type, TypeVar, Dict, Any, Optional

from dbt.clients.system import write_json, read_json
from dbt.exceptions import (
    DbtInternalError,
    DbtRuntimeError,
    IncompatibleSchemaError,
)
from dbt.version import __version__

from dbt.events.functions import get_invocation_id, get_metadata_vars
from dbt.dataclass_schema import dbtClassMixin

from dbt.dataclass_schema import (
    ValidatedStringMixin,
    ValidationError,
)
from mashumaro.jsonschema import build_json_schema
from mashumaro.jsonschema.dialects import DRAFT_2020_12
import functools


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
    def write(self, path: str):
        write_json(path, self.to_dict(omit_none=False))  # type: ignore


class AdditionalPropertiesMixin:
    """Make this class an extensible property.

    The underlying class definition must include a type definition for a field
    named '_extra' that is of type `Dict[str, Any]`.
    """

    ADDITIONAL_PROPERTIES = True

    # This takes attributes in the dictionary that are
    # not in the class definitions and puts them in an
    # _extra dict in the class
    @classmethod
    def __pre_deserialize__(cls, data):
        # dir() did not work because fields with
        # metadata settings are not found
        # The original version of this would create the
        # object first and then update extra with the
        # extra keys, but that won't work here, so
        # we're copying the dict so we don't insert the
        # _extra in the original data. This also requires
        # that Mashumaro actually build the '_extra' field
        cls_keys = cls._get_field_names()
        new_dict = {}
        for key, value in data.items():
            # The pre-hook/post-hook mess hasn't been converted yet... That happens in
            # the super().__pre_deserialize__ below...
            if key not in cls_keys and key not in ["_extra", "pre-hook", "post-hook"]:
                if "_extra" not in new_dict:
                    new_dict["_extra"] = {}
                new_dict["_extra"][key] = value
            else:
                new_dict[key] = value
        data = new_dict
        data = super().__pre_deserialize__(data)
        return data

    def __post_serialize__(self, dct):
        data = super().__post_serialize__(dct)
        data.update(self.extra)
        if "_extra" in data:
            del data["_extra"]
        return data

    def replace(self, **kwargs):
        dct = self.to_dict(omit_none=False)
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
            raise DbtRuntimeError(
                f'Could not read {cls.__name__} at "{path}" as JSON: {exc}'
            ) from exc

        return cls.from_dict(data)  # type: ignore


BASE_SCHEMAS_URL = "https://schemas.getdbt.com/"
SCHEMA_PATH = "dbt/{name}/v{version}.json"


@dataclasses.dataclass
class SchemaVersion:
    name: str
    version: int

    @property
    def path(self) -> str:
        return SCHEMA_PATH.format(name=self.name, version=self.version)

    def __str__(self) -> str:
        return BASE_SCHEMAS_URL + self.path


# This is used in the ManifestMetadata, RunResultsMetadata, RunOperationResultMetadata,
# FreshnessMetadata, and CatalogMetadata classes
@dataclasses.dataclass
class BaseArtifactMetadata(dbtClassMixin):
    dbt_schema_version: str
    dbt_version: str = __version__
    generated_at: datetime = dataclasses.field(default_factory=datetime.utcnow)
    invocation_id: Optional[str] = dataclasses.field(default_factory=get_invocation_id)
    env: Dict[str, str] = dataclasses.field(default_factory=get_metadata_vars)

    def __post_serialize__(self, dct):
        dct = super().__post_serialize__(dct)
        if dct["generated_at"] and dct["generated_at"].endswith("+00:00"):
            dct["generated_at"] = dct["generated_at"].replace("+00:00", "") + "Z"
        return dct


# This is used as a class decorator to set the schema_version in the
# 'dbt_schema_version' class attribute. (It's copied into the metadata objects.)
# Name attributes of SchemaVersion in classes with the 'schema_version' decorator:
#   manifest
#   run-results
#   run-operation-result
#   sources
#   catalog
#   remote-compile-result
#   remote-execution-result
#   remote-run-result
def schema_version(name: str, version: int):
    def inner(cls: Type[VersionedSchema]):
        cls.dbt_schema_version = SchemaVersion(
            name=name,
            version=version,
        )
        return cls

    return inner


# This is used in the ArtifactMixin and RemoteResult classes
@dataclasses.dataclass
class VersionedSchema(dbtClassMixin):
    dbt_schema_version: ClassVar[SchemaVersion]

    @classmethod
    @functools.lru_cache
    def json_schema(cls) -> Dict[str, Any]:
        json_schema_obj = build_json_schema(cls, dialect=DRAFT_2020_12, with_dialect_uri=True)
        json_schema = json_schema_obj.to_dict()
        json_schema["$id"] = str(cls.dbt_schema_version)
        return json_schema

    @classmethod
    def is_compatible_version(cls, schema_version):
        compatible_versions = [str(cls.dbt_schema_version)]
        if hasattr(cls, "compatible_previous_versions"):
            for name, version in cls.compatible_previous_versions():
                compatible_versions.append(str(SchemaVersion(name, version)))
        return str(schema_version) in compatible_versions

    @classmethod
    def read_and_check_versions(cls, path: str):
        try:
            data = read_json(path)
        except (EnvironmentError, ValueError) as exc:
            raise DbtRuntimeError(
                f'Could not read {cls.__name__} at "{path}" as JSON: {exc}'
            ) from exc

        # Check metadata version. There is a class variable 'dbt_schema_version', but
        # that doesn't show up in artifacts, where it only exists in the 'metadata'
        # dictionary.
        if hasattr(cls, "dbt_schema_version"):
            if "metadata" in data and "dbt_schema_version" in data["metadata"]:
                previous_schema_version = data["metadata"]["dbt_schema_version"]
                # cls.dbt_schema_version is a SchemaVersion object
                if not cls.is_compatible_version(previous_schema_version):
                    raise IncompatibleSchemaError(
                        expected=str(cls.dbt_schema_version),
                        found=previous_schema_version,
                    )

        return cls.upgrade_schema_version(data)

    @classmethod
    def upgrade_schema_version(cls, data):
        """This will modify the data (dictionary) passed in to match the current
        artifact schema code, if necessary. This is the default method, which
        just returns the instantiated object via from_dict."""
        return cls.from_dict(data)


T = TypeVar("T", bound="ArtifactMixin")


# metadata should really be a Generic[T_M] where T_M is a TypeVar bound to
# BaseArtifactMetadata. Unfortunately this isn't possible due to a mypy issue:
# https://github.com/python/mypy/issues/7520
# This is used in the WritableManifest, RunResultsArtifact, RunOperationResultsArtifact,
# and CatalogArtifact
@dataclasses.dataclass(init=False)
class ArtifactMixin(VersionedSchema, Writable, Readable):
    metadata: BaseArtifactMetadata

    @classmethod
    def validate(cls, data):
        super().validate(data)
        if cls.dbt_schema_version is None:
            raise DbtInternalError("Cannot call from_dict with no schema version!")


def get_artifact_schema_version(dct: dict) -> int:
    schema_version = dct.get("metadata", {}).get("dbt_schema_version", None)
    if not schema_version:
        raise ValueError("Artifact is missing schema version")

    # schema_version is in this format: https://schemas.getdbt.com/dbt/manifest/v10.json
    # What the code below is doing:
    # 1. Split on "/" – v10.json
    # 2. Split on "." – v10
    # 3. Skip first character – 10
    # 4. Convert to int
    # TODO: If this gets more complicated, turn into a regex
    return int(schema_version.split("/")[-1].split(".")[0][1:])


class Identifier(ValidatedStringMixin):
    """Our definition of a valid Identifier is the same as what's valid for an unquoted database table name.

    That is:
    1. It can contain a-z, A-Z, 0-9, and _
    1. It cannot start with a number
    """

    ValidationRegex = r"^[^\d\W]\w*$"

    @classmethod
    def is_valid(cls, value: Any) -> bool:
        if not isinstance(value, str):
            return False

        try:
            cls.validate(value)
        except ValidationError:
            return False

        return True
