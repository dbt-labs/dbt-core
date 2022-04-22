from dbt.contracts.util import Replaceable, Mergeable, list_str
from dbt.contracts.connection import QueryComment, UserConfigContract
from dbt.helper_types import NoValue
from dbt.dataclass_schema import (
    dbtClassMixin,
    ValidationError,
    HyphenatedDbtClassMixin,
    ExtensibleDbtClassMixin,
    register_pattern,
    ValidatedStringMixin,
)
from dataclasses import dataclass, field
from typing import Optional, List, Dict, Union, Any
from mashumaro.types import SerializableType

PIN_PACKAGE_URL = (
    "https://docs.getdbt.com/docs/package-management#section-specifying-package-versions"  # noqa
)
DEFAULT_SEND_ANONYMOUS_USAGE_STATS = True


class Name(ValidatedStringMixin):
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


register_pattern(Name, r"^[^\d\W]\w*$")


class SemverString(str, SerializableType):
    def _serialize(self) -> str:
        return self

    @classmethod
    def _deserialize(cls, value: str) -> "SemverString":
        return SemverString(value)


# this supports full semver,
# but also allows for 2 group version numbers, (allows '1.0').
register_pattern(
    SemverString,
    r"^(0|[1-9]\d*)\.(0|[1-9]\d*)(\.(0|[1-9]\d*)(?:-((?:0|[1-9]\d*|\d*[a-zA-Z-][0-9a-zA-Z-]*)(?:\.(?:0|[1-9]\d*|\d*[a-zA-Z-][0-9a-zA-Z-]*))*))?(?:\+([0-9a-zA-Z-]+(?:\.[0-9a-zA-Z-]+)*))?)?$",  # noqa
)


@dataclass
class Quoting(dbtClassMixin, Mergeable):
    schema: Optional[bool] = None
    database: Optional[bool] = None
    project: Optional[bool] = None
    identifier: Optional[bool] = None


@dataclass
class Package(Replaceable, HyphenatedDbtClassMixin):
    pass


@dataclass
class LocalPackage(Package):
    local: str


# `float` also allows `int`, according to PEP484 (and jsonschema!)
RawVersion = Union[str, float]


@dataclass
class GitPackage(Package):
    git: str
    revision: Optional[RawVersion] = None
    warn_unpinned: Optional[bool] = None
    subdirectory: Optional[str] = None

    def get_revisions(self) -> List[str]:
        if self.revision is None:
            return []
        else:
            return [str(self.revision)]


@dataclass
class RegistryPackage(Package):
    package: str
    version: Union[RawVersion, List[RawVersion]]
    install_prerelease: Optional[bool] = False

    def get_versions(self) -> List[str]:
        if isinstance(self.version, list):
            return [str(v) for v in self.version]
        else:
            return [str(self.version)]


PackageSpec = Union[LocalPackage, GitPackage, RegistryPackage]


@dataclass
class PackageConfig(dbtClassMixin, Replaceable):
    packages: List[PackageSpec]


@dataclass
class ProjectPackageMetadata:
    name: str
    packages: List[PackageSpec]

    @classmethod
    def from_project(cls, project):
        return cls(name=project.project_name, packages=project.packages.packages)


@dataclass
class Downloads(ExtensibleDbtClassMixin, Replaceable):
    tarball: str


@dataclass
class RegistryPackageMetadata(
    ExtensibleDbtClassMixin,
    ProjectPackageMetadata,
):
    downloads: Downloads


# A list of all the reserved words that packages may not have as names.
BANNED_PROJECT_NAMES = {
    "_sql_results",
    "adapter",
    "api",
    "column",
    "config",
    "context",
    "database",
    "env",
    "env_var",
    "exceptions",
    "execute",
    "flags",
    "fromjson",
    "fromyaml",
    "graph",
    "invocation_id",
    "load_agate_table",
    "load_result",
    "log",
    "model",
    "modules",
    "post_hooks",
    "pre_hooks",
    "ref",
    "render",
    "return",
    "run_started_at",
    "schema",
    "source",
    "sql",
    "sql_now",
    "store_result",
    "store_raw_result",
    "target",
    "this",
    "tojson",
    "toyaml",
    "try_or_compiler_error",
    "var",
    "write",
}


@dataclass
class Project(HyphenatedDbtClassMixin, Replaceable):
    name: Name
    version: Union[SemverString, float]
    config_version: int
    project_root: Optional[str] = None
    source_paths: Optional[List[str]] = None
    model_paths: Optional[List[str]] = None
    macro_paths: Optional[List[str]] = None
    data_paths: Optional[List[str]] = None
    seed_paths: Optional[List[str]] = None
    test_paths: Optional[List[str]] = None
    analysis_paths: Optional[List[str]] = None
    docs_paths: Optional[List[str]] = None
    asset_paths: Optional[List[str]] = None
    target_path: Optional[str] = None
    snapshot_paths: Optional[List[str]] = None
    clean_targets: Optional[List[str]] = None
    profile: Optional[str] = None
    log_path: Optional[str] = None
    packages_install_path: Optional[str] = None
    quoting: Optional[Quoting] = None
    on_run_start: Optional[List[str]] = field(default_factory=list_str)
    on_run_end: Optional[List[str]] = field(default_factory=list_str)
    require_dbt_version: Optional[Union[List[str], str]] = None
    dispatch: List[Dict[str, Any]] = field(default_factory=list)
    models: Dict[str, Any] = field(default_factory=dict)
    seeds: Dict[str, Any] = field(default_factory=dict)
    snapshots: Dict[str, Any] = field(default_factory=dict)
    analyses: Dict[str, Any] = field(default_factory=dict)
    sources: Dict[str, Any] = field(default_factory=dict)
    tests: Dict[str, Any] = field(default_factory=dict)
    vars: Optional[Dict[str, Any]] = field(
        default=None,
        metadata=dict(
            description="map project names to their vars override dicts",
        ),
    )
    packages: List[PackageSpec] = field(default_factory=list)
    query_comment: Optional[Union[QueryComment, NoValue, str]] = NoValue()

    @classmethod
    def validate(cls, data):
        super().validate(data)
        if data["name"] in BANNED_PROJECT_NAMES:
            raise ValidationError(f"Invalid project name: {data['name']} is a reserved word")
        # validate dispatch config
        if "dispatch" in data and data["dispatch"]:
            entries = data["dispatch"]
            for entry in entries:
                if (
                    "macro_namespace" not in entry
                    or "search_order" not in entry
                    or not isinstance(entry["search_order"], list)
                ):
                    raise ValidationError(f"Invalid project dispatch config: {entry}")


@dataclass
class UserConfig(ExtensibleDbtClassMixin, Replaceable, UserConfigContract):
    send_anonymous_usage_stats: bool = DEFAULT_SEND_ANONYMOUS_USAGE_STATS
    use_colors: Optional[bool] = None
    partial_parse: Optional[bool] = None
    printer_width: Optional[int] = None
    write_json: Optional[bool] = None
    warn_error: Optional[bool] = None
    log_format: Optional[str] = None
    debug: Optional[bool] = None
    version_check: Optional[bool] = None
    fail_fast: Optional[bool] = None
    use_experimental_parser: Optional[bool] = None
    static_parser: Optional[bool] = None
    indirect_selection: Optional[str] = None
    cache_selected_only: Optional[bool] = None


@dataclass
class ProfileConfig(HyphenatedDbtClassMixin, Replaceable):
    profile_name: str = field(metadata={"preserve_underscore": True})
    target_name: str = field(metadata={"preserve_underscore": True})
    user_config: UserConfig = field(metadata={"preserve_underscore": True})
    threads: int
    # TODO: make this a dynamic union of some kind?
    credentials: Optional[Dict[str, Any]]


@dataclass
class ConfiguredQuoting(Quoting, Replaceable):
    identifier: bool = True
    schema: bool = True
    database: Optional[bool] = None
    project: Optional[bool] = None


@dataclass
class Configuration(Project, ProfileConfig):
    cli_vars: Dict[str, Any] = field(
        default_factory=dict,
        metadata={"preserve_underscore": True},
    )
    quoting: Optional[ConfiguredQuoting] = None


@dataclass
class ProjectList(dbtClassMixin):
    projects: Dict[str, Project]
