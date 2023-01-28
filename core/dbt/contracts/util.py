import dataclasses
from datetime import datetime
from typing import List, Tuple, ClassVar, Type, TypeVar, Dict, Any, Optional

from dbt.clients.system import write_json, read_json
from dbt import deprecations
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
    register_pattern,
)


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
            if key not in cls_keys and key != "_extra":
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


def get_manifest_schema_version(dct: dict) -> int:
    schema_version = dct.get("metadata", {}).get("dbt_schema_version", None)
    if not schema_version:
        raise ValueError("Manifest doesn't have schema version")
    return int(schema_version.split(".")[-2][-1])


# we renamed these properties in v1.3
# this method allows us to be nice to the early adopters
def rename_metric_attr(data: dict, raise_deprecation_warning: bool = False) -> dict:
    metric_name = data["name"]
    if raise_deprecation_warning and (
        "sql" in data.keys()
        or "type" in data.keys()
        or data.get("calculation_method") == "expression"
    ):
        deprecations.warn("metric-attr-renamed", metric_name=metric_name)
    duplicated_attribute_msg = """\n
The metric '{}' contains both the deprecated metric property '{}'
and the up-to-date metric property '{}'. Please remove the deprecated property.
"""
    if "sql" in data.keys():
        if "expression" in data.keys():
            raise ValidationError(
                duplicated_attribute_msg.format(metric_name, "sql", "expression")
            )
        else:
            data["expression"] = data.pop("sql")
    if "type" in data.keys():
        if "calculation_method" in data.keys():
            raise ValidationError(
                duplicated_attribute_msg.format(metric_name, "type", "calculation_method")
            )
        else:
            calculation_method = data.pop("type")
            data["calculation_method"] = calculation_method
    # we also changed "type: expression" -> "calculation_method: derived"
    if data.get("calculation_method") == "expression":
        data["calculation_method"] = "derived"
    return data


def rename_sql_attr(node_content: dict) -> dict:
    if "raw_sql" in node_content:
        node_content["raw_code"] = node_content.pop("raw_sql")
    if "compiled_sql" in node_content:
        node_content["compiled_code"] = node_content.pop("compiled_sql")
    node_content["language"] = "sql"
    return node_content


def upgrade_node_content(node_content):
    rename_sql_attr(node_content)
    if node_content["resource_type"] != "seed" and "root_path" in node_content:
        del node_content["root_path"]


def upgrade_seed_content(node_content):
    # Remove compilation related attributes
    for attr_name in (
        "language",
        "refs",
        "sources",
        "metrics",
        "depends_on",
        "compiled_path",
        "compiled",
        "compiled_code",
        "extra_ctes_injected",
        "extra_ctes",
        "relation_name",
    ):
        if attr_name in node_content:
            del node_content[attr_name]


def upgrade_manifest_json(manifest: dict) -> dict:
    for node_content in manifest.get("nodes", {}).values():
        upgrade_node_content(node_content)
        if node_content["resource_type"] == "seed":
            upgrade_seed_content(node_content)
    for disabled in manifest.get("disabled", {}).values():
        # There can be multiple disabled nodes for the same unique_id
        # so make sure all the nodes get the attr renamed
        for node_content in disabled:
            upgrade_node_content(node_content)
            if node_content["resource_type"] == "seed":
                upgrade_seed_content(node_content)
    for metric_content in manifest.get("metrics", {}).values():
        # handle attr renames + value translation ("expression" -> "derived")
        metric_content = rename_metric_attr(metric_content)
        if "root_path" in metric_content:
            del metric_content["root_path"]
    for exposure_content in manifest.get("exposures", {}).values():
        if "root_path" in exposure_content:
            del exposure_content["root_path"]
    for source_content in manifest.get("sources", {}).values():
        if "root_path" in source_content:
            del source_content["root_path"]
    for macro_content in manifest.get("macros", {}).values():
        if "root_path" in macro_content:
            del macro_content["root_path"]
    for doc_content in manifest.get("docs", {}).values():
        if "root_path" in doc_content:
            del doc_content["root_path"]
        doc_content["resource_type"] = "doc"
    return manifest


# This is used in the ArtifactMixin and RemoteResult classes
@dataclasses.dataclass
class VersionedSchema(dbtClassMixin):
    dbt_schema_version: ClassVar[SchemaVersion]

    @classmethod
    def json_schema(cls, embeddable: bool = False) -> Dict[str, Any]:
        result = super().json_schema(embeddable=embeddable)
        if not embeddable:
            result["$id"] = str(cls.dbt_schema_version)
        return result

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
        if get_manifest_schema_version(data) <= 7:
            data = upgrade_manifest_json(data)
        return cls.from_dict(data)  # type: ignore


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


class Identifier(ValidatedStringMixin):
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


register_pattern(Identifier, r"^[^\d\W]\w*$")
