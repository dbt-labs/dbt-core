from copy import deepcopy
from dataclasses import dataclass, field
from itertools import chain
from typing import (
    List,
    Dict,
    Any,
    Optional,
    TypeVar,
    Union,
    Mapping,
)
from typing_extensions import Protocol, runtime_checkable

import os

from dbt.flags import get_flags
from dbt import deprecations
from dbt.clients.system import path_exists, resolve_path_from_base, load_file_contents
from dbt.clients.yaml_helper import load_yaml_text
from dbt.contracts.connection import QueryComment
from dbt.exceptions import (
    DbtProjectError,
    SemverError,
    ProjectContractBrokenError,
    ProjectContractError,
    DbtRuntimeError,
)
from dbt.graph import SelectionSpec
from dbt.helper_types import NoValue
from dbt.semver import VersionSpecifier, versions_compatible
from dbt.version import get_installed_version
from dbt.utils import MultiDict, md5
from dbt.node_types import NodeType
from dbt.config.selectors import SelectorDict
from dbt.contracts.project import (
    Project as ProjectContract,
    SemverString,
)
from dbt.contracts.project import PackageConfig, ProjectPackageMetadata
from dbt.dataclass_schema import ValidationError
from .renderer import DbtProjectYamlRenderer, PackageRenderer
from .selectors import (
    selector_config_from_data,
    selector_data_from_root,
    SelectorConfig,
)


INVALID_VERSION_ERROR = """\
This version of dbt is not supported with the '{package}' package.
  Installed version of dbt: {installed}
  Required version of dbt for '{package}': {version_spec}
Check for a different version of the '{package}' package, or run dbt again with \
--no-version-check
"""


IMPOSSIBLE_VERSION_ERROR = """\
The package version requirement can never be satisfied for the '{package}
package.
  Required versions of dbt for '{package}': {version_spec}
Check for a different version of the '{package}' package, or run dbt again with \
--no-version-check
"""

MALFORMED_PACKAGE_ERROR = """\
The packages.yml file in this project is malformed. Please double check
the contents of this file and fix any errors before retrying.

You can find more information on the syntax for this file here:
https://docs.getdbt.com/docs/package-management

Validator Error:
{error}
"""

MISSING_DBT_PROJECT_ERROR = """\
No dbt_project.yml found at expected path {path}
Verify that each entry within packages.yml (and their transitive dependencies) contains a file named dbt_project.yml
"""


@runtime_checkable
class IsFQNResource(Protocol):
    fqn: List[str]
    resource_type: NodeType
    package_name: str


def _load_yaml(path):
    contents = load_file_contents(path)
    return load_yaml_text(contents)


def package_data_from_root(project_root):
    package_filepath = resolve_path_from_base("packages.yml", project_root)

    if path_exists(package_filepath):
        packages_dict = _load_yaml(package_filepath)
    else:
        packages_dict = None
    return packages_dict


def package_config_from_data(packages_data: Dict[str, Any]):
    if not packages_data:
        packages_data = {"packages": []}

    try:
        PackageConfig.validate(packages_data)
        packages = PackageConfig.from_dict(packages_data)
    except ValidationError as e:
        raise DbtProjectError(MALFORMED_PACKAGE_ERROR.format(error=str(e.message))) from e
    return packages


def _parse_versions(versions: Union[List[str], str]) -> List[VersionSpecifier]:
    """Parse multiple versions as read from disk. The versions value may be any
    one of:
        - a single version string ('>0.12.1')
        - a single string specifying multiple comma-separated versions
            ('>0.11.1,<=0.12.2')
        - an array of single-version strings (['>0.11.1', '<=0.12.2'])

    Regardless, this will return a list of VersionSpecifiers
    """
    if isinstance(versions, str):
        versions = versions.split(",")
    return [VersionSpecifier.from_version_string(v) for v in versions]


def _all_source_paths(
    model_paths: List[str],
    seed_paths: List[str],
    snapshot_paths: List[str],
    analysis_paths: List[str],
    macro_paths: List[str],
) -> List[str]:
    paths = chain(model_paths, seed_paths, snapshot_paths, analysis_paths, macro_paths)
    # Strip trailing slashes since the path is the same even though the name is not
    stripped_paths = map(lambda s: s.rstrip("/"), paths)
    return list(set(stripped_paths))


T = TypeVar("T")


def flag_or(flag: Optional[T], value: Optional[T], default: T) -> T:
    if flag is None:
        return value_or(value, default)
    else:
        return flag


def value_or(value: Optional[T], default: T) -> T:
    if value is None:
        return default
    else:
        return value


def load_raw_project(project_root: str) -> Dict[str, Any]:

    project_root = os.path.normpath(project_root)
    project_yaml_filepath = os.path.join(project_root, "dbt_project.yml")

    # get the project.yml contents
    if not path_exists(project_yaml_filepath):
        raise DbtProjectError(MISSING_DBT_PROJECT_ERROR.format(path=project_yaml_filepath))

    project_dict = _load_yaml(project_yaml_filepath)

    if not isinstance(project_dict, dict):
        raise DbtProjectError("dbt_project.yml does not parse to a dictionary")

    return project_dict


def _query_comment_from_cfg(
    cfg_query_comment: Union[QueryComment, NoValue, str, None]
) -> QueryComment:
    if not cfg_query_comment:
        return QueryComment(comment="")

    if isinstance(cfg_query_comment, str):
        return QueryComment(comment=cfg_query_comment)

    if isinstance(cfg_query_comment, NoValue):
        return QueryComment()

    return cfg_query_comment


def validate_version(dbt_version: List[VersionSpecifier], project_name: str):
    """Ensure this package works with the installed version of dbt."""
    installed = get_installed_version()
    if not versions_compatible(*dbt_version):
        msg = IMPOSSIBLE_VERSION_ERROR.format(
            package=project_name, version_spec=[x.to_version_string() for x in dbt_version]
        )
        raise DbtProjectError(msg)

    if not versions_compatible(installed, *dbt_version):
        msg = INVALID_VERSION_ERROR.format(
            package=project_name,
            installed=installed.to_version_string(),
            version_spec=[x.to_version_string() for x in dbt_version],
        )
        raise DbtProjectError(msg)


def _get_required_version(
    project_dict: Dict[str, Any],
    verify_version: bool,
) -> List[VersionSpecifier]:
    dbt_raw_version: Union[List[str], str] = ">=0.0.0"
    required = project_dict.get("require-dbt-version")
    if required is not None:
        dbt_raw_version = required

    try:
        dbt_version = _parse_versions(dbt_raw_version)
    except SemverError as e:
        raise DbtProjectError(str(e)) from e

    if verify_version:
        # no name is also an error that we want to raise
        if "name" not in project_dict:
            raise DbtProjectError(
                'Required "name" field not present in project',
            )
        validate_version(dbt_version, project_dict["name"])

    return dbt_version


@dataclass
class RenderComponents:
    project_dict: Dict[str, Any] = field(metadata=dict(description="The project dictionary"))
    packages_dict: Dict[str, Any] = field(metadata=dict(description="The packages dictionary"))
    selectors_dict: Dict[str, Any] = field(metadata=dict(description="The selectors dictionary"))


@dataclass
class PartialProject(RenderComponents):
    profile_name: Optional[str] = field(
        metadata=dict(description="The unrendered profile name in the project, if set")
    )
    project_name: Optional[str] = field(
        metadata=dict(
            description=(
                "The name of the project. This should always be set and will not be rendered"
            )
        )
    )
    project_root: str = field(
        metadata=dict(description="The root directory of the project"),
    )
    verify_version: bool = field(
        metadata=dict(description=("If True, verify the dbt version matches the required version"))
    )

    def render_profile_name(self, renderer) -> Optional[str]:
        if self.profile_name is None:
            return None
        return renderer.render_value(self.profile_name)

    def get_rendered(
        self,
        renderer: DbtProjectYamlRenderer,
    ) -> RenderComponents:

        rendered_project = renderer.render_project(self.project_dict, self.project_root)
        rendered_packages = renderer.render_packages(self.packages_dict)
        rendered_selectors = renderer.render_selectors(self.selectors_dict)

        return RenderComponents(
            project_dict=rendered_project,
            packages_dict=rendered_packages,
            selectors_dict=rendered_selectors,
        )

    # Called by 'collect_parts' in RuntimeConfig
    def render(self, renderer: DbtProjectYamlRenderer) -> "Project":
        try:
            rendered = self.get_rendered(renderer)
            return self.create_project(rendered)
        except DbtProjectError as exc:
            if exc.path is None:
                exc.path = os.path.join(self.project_root, "dbt_project.yml")
            raise

    def render_package_metadata(self, renderer: PackageRenderer) -> ProjectPackageMetadata:
        packages_data = renderer.render_data(self.packages_dict)
        packages_config = package_config_from_data(packages_data)
        if not self.project_name:
            raise DbtProjectError("Package dbt_project.yml must have a name!")
        return ProjectPackageMetadata(self.project_name, packages_config.packages)

    def check_config_path(
        self, project_dict, deprecated_path, expected_path=None, default_value=None
    ):
        if deprecated_path in project_dict:
            if expected_path in project_dict:
                msg = (
                    "{deprecated_path} and {expected_path} cannot both be defined. The "
                    "`{deprecated_path}` config has been deprecated in favor of `{expected_path}`. "
                    "Please update your `dbt_project.yml` configuration to reflect this "
                    "change."
                )
                raise DbtProjectError(
                    msg.format(deprecated_path=deprecated_path, expected_path=expected_path)
                )
            # this field is no longer supported, but many projects may specify it with the default value
            # if so, let's only raise this deprecation warning if they set a custom value
            if not default_value or project_dict[deprecated_path] != default_value:
                kwargs = {"deprecated_path": deprecated_path}
                if expected_path:
                    kwargs.update({"exp_path": expected_path})
                deprecations.warn(f"project-config-{deprecated_path}", **kwargs)

    def create_project(self, rendered: RenderComponents) -> "Project":
        unrendered = RenderComponents(
            project_dict=self.project_dict,
            packages_dict=self.packages_dict,
            selectors_dict=self.selectors_dict,
        )
        dbt_version = _get_required_version(
            rendered.project_dict,
            verify_version=self.verify_version,
        )

        self.check_config_path(rendered.project_dict, "source-paths", "model-paths")
        self.check_config_path(rendered.project_dict, "data-paths", "seed-paths")
        self.check_config_path(rendered.project_dict, "log-path", default_value="logs")
        self.check_config_path(rendered.project_dict, "target-path", default_value="target")

        try:
            ProjectContract.validate(rendered.project_dict)
            cfg = ProjectContract.from_dict(rendered.project_dict)
        except ValidationError as e:
            raise ProjectContractError(e) from e
        # name/version are required in the Project definition, so we can assume
        # they are present
        name = cfg.name
        version = cfg.version
        # this is added at project_dict parse time and should always be here
        # once we see it.
        if cfg.project_root is None:
            raise DbtProjectError("cfg must have a project root!")
        else:
            project_root = cfg.project_root
        # this is only optional in the sense that if it's not present, it needs
        # to have been a cli argument.
        profile_name = cfg.profile
        # these are all the defaults

        # `source_paths` is deprecated but still allowed. Copy it into
        # `model_paths` to simlify logic throughout the rest of the system.
        model_paths: List[str] = value_or(
            cfg.model_paths if "model-paths" in rendered.project_dict else cfg.source_paths,
            ["models"],
        )
        macro_paths: List[str] = value_or(cfg.macro_paths, ["macros"])
        # `data_paths` is deprecated but still allowed. Copy it into
        # `seed_paths` to simlify logic throughout the rest of the system.
        seed_paths: List[str] = value_or(
            cfg.seed_paths if "seed-paths" in rendered.project_dict else cfg.data_paths, ["seeds"]
        )
        test_paths: List[str] = value_or(cfg.test_paths, ["tests"])
        analysis_paths: List[str] = value_or(cfg.analysis_paths, ["analyses"])
        snapshot_paths: List[str] = value_or(cfg.snapshot_paths, ["snapshots"])

        all_source_paths: List[str] = _all_source_paths(
            model_paths, seed_paths, snapshot_paths, analysis_paths, macro_paths
        )

        docs_paths: List[str] = value_or(cfg.docs_paths, all_source_paths)
        asset_paths: List[str] = value_or(cfg.asset_paths, [])
        flags = get_flags()

        flag_target_path = str(flags.TARGET_PATH) if flags.TARGET_PATH else None
        target_path: str = flag_or(flag_target_path, cfg.target_path, "target")
        log_path: str = str(flags.LOG_PATH)

        clean_targets: List[str] = value_or(cfg.clean_targets, [target_path])
        packages_install_path: str = value_or(cfg.packages_install_path, "dbt_packages")
        # in the default case we'll populate this once we know the adapter type
        # It would be nice to just pass along a Quoting here, but that would
        # break many things
        quoting: Dict[str, Any] = {}
        if cfg.quoting is not None:
            quoting = cfg.quoting.to_dict(omit_none=True)

        dispatch: List[Dict[str, Any]]
        models: Dict[str, Any]
        seeds: Dict[str, Any]
        snapshots: Dict[str, Any]
        sources: Dict[str, Any]
        tests: Dict[str, Any]
        metrics: Dict[str, Any]
        exposures: Dict[str, Any]
        vars_value: VarProvider

        dispatch = cfg.dispatch
        models = cfg.models
        seeds = cfg.seeds
        snapshots = cfg.snapshots
        sources = cfg.sources
        tests = cfg.tests
        metrics = cfg.metrics
        exposures = cfg.exposures
        if cfg.vars is None:
            vars_dict: Dict[str, Any] = {}
        else:
            vars_dict = cfg.vars

        vars_value = VarProvider(vars_dict)
        # There will never be any project_env_vars when it's first created
        project_env_vars: Dict[str, Any] = {}
        on_run_start: List[str] = value_or(cfg.on_run_start, [])
        on_run_end: List[str] = value_or(cfg.on_run_end, [])

        query_comment = _query_comment_from_cfg(cfg.query_comment)

        packages = package_config_from_data(rendered.packages_dict)
        selectors = selector_config_from_data(rendered.selectors_dict)
        manifest_selectors: Dict[str, Any] = {}
        if rendered.selectors_dict and rendered.selectors_dict["selectors"]:
            # this is a dict with a single key 'selectors' pointing to a list
            # of dicts.
            manifest_selectors = SelectorDict.parse_from_selectors_list(
                rendered.selectors_dict["selectors"]
            )
        project = Project(
            project_name=name,
            version=version,
            project_root=project_root,
            profile_name=profile_name,
            model_paths=model_paths,
            macro_paths=macro_paths,
            seed_paths=seed_paths,
            test_paths=test_paths,
            analysis_paths=analysis_paths,
            docs_paths=docs_paths,
            asset_paths=asset_paths,
            target_path=target_path,
            snapshot_paths=snapshot_paths,
            clean_targets=clean_targets,
            log_path=log_path,
            packages_install_path=packages_install_path,
            quoting=quoting,
            models=models,
            on_run_start=on_run_start,
            on_run_end=on_run_end,
            dispatch=dispatch,
            seeds=seeds,
            snapshots=snapshots,
            dbt_version=dbt_version,
            packages=packages,
            manifest_selectors=manifest_selectors,
            selectors=selectors,
            query_comment=query_comment,
            sources=sources,
            tests=tests,
            metrics=metrics,
            exposures=exposures,
            vars=vars_value,
            config_version=cfg.config_version,
            unrendered=unrendered,
            project_env_vars=project_env_vars,
        )
        # sanity check - this means an internal issue
        project.validate()
        return project

    @classmethod
    def from_dicts(
        cls,
        project_root: str,
        project_dict: Dict[str, Any],
        packages_dict: Dict[str, Any],
        selectors_dict: Dict[str, Any],
        *,
        verify_version: bool = False,
    ):
        """Construct a partial project from its constituent dicts."""
        project_name = project_dict.get("name")
        profile_name = project_dict.get("profile")

        return cls(
            profile_name=profile_name,
            project_name=project_name,
            project_root=project_root,
            project_dict=project_dict,
            packages_dict=packages_dict,
            selectors_dict=selectors_dict,
            verify_version=verify_version,
        )

    @classmethod
    def from_project_root(
        cls, project_root: str, *, verify_version: bool = False
    ) -> "PartialProject":
        project_root = os.path.normpath(project_root)
        project_dict = load_raw_project(project_root)
        packages_dict = package_data_from_root(project_root)
        selectors_dict = selector_data_from_root(project_root)
        return cls.from_dicts(
            project_root=project_root,
            project_dict=project_dict,
            selectors_dict=selectors_dict,
            packages_dict=packages_dict,
            verify_version=verify_version,
        )


class VarProvider:
    """Var providers are tied to a particular Project."""

    def __init__(self, vars: Dict[str, Dict[str, Any]]) -> None:
        self.vars = vars

    def vars_for(self, node: IsFQNResource, adapter_type: str) -> Mapping[str, Any]:
        # in v2, vars are only either project or globally scoped
        merged = MultiDict([self.vars])
        merged.add(self.vars.get(node.package_name, {}))
        return merged

    def to_dict(self):
        return self.vars


# The Project class is included in RuntimeConfig, so any attribute
# additions must also be set where the RuntimeConfig class is created
@dataclass
class Project:
    project_name: str
    version: Optional[Union[SemverString, float]]
    project_root: str
    profile_name: Optional[str]
    model_paths: List[str]
    macro_paths: List[str]
    seed_paths: List[str]
    test_paths: List[str]
    analysis_paths: List[str]
    docs_paths: List[str]
    asset_paths: List[str]
    target_path: str
    snapshot_paths: List[str]
    clean_targets: List[str]
    log_path: str
    packages_install_path: str
    quoting: Dict[str, Any]
    models: Dict[str, Any]
    on_run_start: List[str]
    on_run_end: List[str]
    dispatch: List[Dict[str, Any]]
    seeds: Dict[str, Any]
    snapshots: Dict[str, Any]
    sources: Dict[str, Any]
    tests: Dict[str, Any]
    metrics: Dict[str, Any]
    exposures: Dict[str, Any]
    vars: VarProvider
    dbt_version: List[VersionSpecifier]
    packages: Dict[str, Any]
    manifest_selectors: Dict[str, Any]
    selectors: SelectorConfig
    query_comment: QueryComment
    config_version: int
    unrendered: RenderComponents
    project_env_vars: Dict[str, Any]

    @property
    def all_source_paths(self) -> List[str]:
        return _all_source_paths(
            self.model_paths,
            self.seed_paths,
            self.snapshot_paths,
            self.analysis_paths,
            self.macro_paths,
        )

    @property
    def generic_test_paths(self):
        generic_test_paths = []
        for test_path in self.test_paths:
            generic_test_paths.append(os.path.join(test_path, "generic"))
        return generic_test_paths

    def __str__(self):
        cfg = self.to_project_config(with_packages=True)
        return str(cfg)

    def __eq__(self, other):
        if not (isinstance(other, self.__class__) and isinstance(self, other.__class__)):
            return False
        return self.to_project_config(with_packages=True) == other.to_project_config(
            with_packages=True
        )

    def to_project_config(self, with_packages=False):
        """Return a dict representation of the config that could be written to
        disk with `yaml.safe_dump` to get this configuration.

        :param with_packages bool: If True, include the serialized packages
            file in the root.
        :returns dict: The serialized profile.
        """
        result = deepcopy(
            {
                "name": self.project_name,
                "version": self.version,
                "project-root": self.project_root,
                "profile": self.profile_name,
                "model-paths": self.model_paths,
                "macro-paths": self.macro_paths,
                "seed-paths": self.seed_paths,
                "test-paths": self.test_paths,
                "analysis-paths": self.analysis_paths,
                "docs-paths": self.docs_paths,
                "asset-paths": self.asset_paths,
                "target-path": self.target_path,
                "snapshot-paths": self.snapshot_paths,
                "clean-targets": self.clean_targets,
                "log-path": self.log_path,
                "quoting": self.quoting,
                "models": self.models,
                "on-run-start": self.on_run_start,
                "on-run-end": self.on_run_end,
                "dispatch": self.dispatch,
                "seeds": self.seeds,
                "snapshots": self.snapshots,
                "sources": self.sources,
                "tests": self.tests,
                "metrics": self.metrics,
                "exposures": self.exposures,
                "vars": self.vars.to_dict(),
                "require-dbt-version": [v.to_version_string() for v in self.dbt_version],
                "config-version": self.config_version,
            }
        )
        if self.query_comment:
            result["query-comment"] = self.query_comment.to_dict(omit_none=True)

        if with_packages:
            result.update(self.packages.to_dict(omit_none=True))

        return result

    def validate(self):
        try:
            ProjectContract.validate(self.to_project_config())
        except ValidationError as e:
            raise ProjectContractBrokenError(e) from e

    @classmethod
    def partial_load(cls, project_root: str, *, verify_version: bool = False) -> PartialProject:
        return PartialProject.from_project_root(
            project_root,
            verify_version=verify_version,
        )

    @classmethod
    def from_project_root(
        cls,
        project_root: str,
        renderer: DbtProjectYamlRenderer,
        *,
        verify_version: bool = False,
    ) -> "Project":
        partial = PartialProject.from_project_root(project_root, verify_version=verify_version)
        return partial.render(renderer)

    def hashed_name(self):
        return md5(self.project_name)

    def get_selector(self, name: str) -> Union[SelectionSpec, bool]:
        if name not in self.selectors:
            raise DbtRuntimeError(
                f"Could not find selector named {name}, expected one of {list(self.selectors)}"
            )
        return self.selectors[name]["definition"]

    def get_default_selector_name(self) -> Union[str, None]:
        """This function fetch the default selector to use on `dbt run` (if any)
        :return: either a selector if default is set or None
        :rtype: Union[SelectionSpec, None]
        """
        for selector_name, selector in self.selectors.items():
            if selector["default"] is True:
                return selector_name

        return None

    def get_macro_search_order(self, macro_namespace: str):
        for dispatch_entry in self.dispatch:
            if dispatch_entry["macro_namespace"] == macro_namespace:
                return dispatch_entry["search_order"]
        return None

    @property
    def project_target_path(self):
        # If target_path is absolute, project_root will not be included
        return os.path.join(self.project_root, self.target_path)
