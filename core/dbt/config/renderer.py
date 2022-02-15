from typing import Dict, Any, Tuple, Optional, Union, Callable

from dbt.clients.jinja import get_rendered, catch_jinja
from dbt.context.target import TargetContext
from dbt.context.secret import SecretContext
from dbt.context.base import BaseContext
from dbt.contracts.connection import HasCredentials
from dbt.exceptions import DbtProjectError, CompilationException, RecursionException
from dbt.utils import deep_map_render


Keypath = Tuple[Union[str, int], ...]


class BaseRenderer:
    def __init__(self, context: Dict[str, Any]) -> None:
        self.context = context

    @property
    def name(self):
        return "Rendering"

    def should_render_keypath(self, keypath: Keypath) -> bool:
        return True

    def render_entry(self, value: Any, keypath: Keypath) -> Any:
        if not self.should_render_keypath(keypath):
            return value

        return self.render_value(value, keypath)

    def render_value(self, value: Any, keypath: Optional[Keypath] = None) -> Any:
        # keypath is ignored.
        # if it wasn't read as a string, ignore it
        if not isinstance(value, str):
            return value
        try:
            with catch_jinja():
                return get_rendered(value, self.context, native=True)
        except CompilationException as exc:
            msg = f"Could not render {value}: {exc.msg}"
            raise CompilationException(msg) from exc

    def render_data(self, data: Dict[str, Any]) -> Dict[str, Any]:
        try:
            return deep_map_render(self.render_entry, data)
        except RecursionException:
            raise DbtProjectError(
                f"Cycle detected: {self.name} input has a reference to itself", project=data
            )


def _list_if_none(value):
    if value is None:
        value = []
    return value


def _dict_if_none(value):
    if value is None:
        value = {}
    return value


def _list_if_none_or_string(value):
    value = _list_if_none(value)
    if isinstance(value, str):
        return [value]
    return value


class ProjectPostprocessor(Dict[Keypath, Callable[[Any], Any]]):
    def __init__(self):
        super().__init__()

        self[("on-run-start",)] = _list_if_none_or_string
        self[("on-run-end",)] = _list_if_none_or_string

        for k in ("models", "seeds", "snapshots"):
            self[(k,)] = _dict_if_none
            self[(k, "vars")] = _dict_if_none
            self[(k, "pre-hook")] = _list_if_none_or_string
            self[(k, "post-hook")] = _list_if_none_or_string
        self[("seeds", "column_types")] = _dict_if_none

    def postprocess(self, value: Any, key: Keypath) -> Any:
        if key in self:
            handler = self[key]
            return handler(value)

        return value


class DbtProjectYamlRenderer(BaseRenderer):
    _KEYPATH_HANDLERS = ProjectPostprocessor()

    def __init__(
        self, profile: Optional[HasCredentials] = None, cli_vars: Optional[Dict[str, Any]] = None
    ) -> None:
        # Generate contexts here because we want to save the context
        # object in order to retrieve the env_vars. This is almost always
        # a TargetContext, but in the debug task we want a project
        # even when we don't have a profile.
        if cli_vars is None:
            cli_vars = {}
        if profile:
            self.ctx_obj = TargetContext(profile, cli_vars)
        else:
            self.ctx_obj = BaseContext(cli_vars)  # type:ignore
        context = self.ctx_obj.to_dict()
        super().__init__(context)

    @property
    def name(self):
        "Project config"

    def get_package_renderer(self) -> BaseRenderer:
        return PackageRenderer(self.context)

    def get_selector_renderer(self) -> BaseRenderer:
        return SelectorRenderer(self.context)

    def render_project(
        self,
        project: Dict[str, Any],
        project_root: str,
    ) -> Dict[str, Any]:
        """Render the project and insert the project root after rendering."""
        rendered_project = self.render_data(project)
        rendered_project["project-root"] = project_root
        return rendered_project

    def render_packages(self, packages: Dict[str, Any]):
        """Render the given packages dict"""
        package_renderer = self.get_package_renderer()
        return package_renderer.render_data(packages)

    def render_selectors(self, selectors: Dict[str, Any]):
        selector_renderer = self.get_selector_renderer()
        return selector_renderer.render_data(selectors)

    def render_entry(self, value: Any, keypath: Keypath) -> Any:
        result = super().render_entry(value, keypath)
        return self._KEYPATH_HANDLERS.postprocess(result, keypath)

    def should_render_keypath(self, keypath: Keypath) -> bool:
        if not keypath:
            return True

        first = keypath[0]
        # run hooks are not rendered
        if first in {"on-run-start", "on-run-end", "query-comment"}:
            return False

        # don't render vars blocks until runtime
        if first == "vars":
            return False

        if first in {"seeds", "models", "snapshots", "tests"}:
            keypath_parts = {(k.lstrip("+ ") if isinstance(k, str) else k) for k in keypath}
            # model-level hooks
            if "pre-hook" in keypath_parts or "post-hook" in keypath_parts:
                return False

        return True


class SelectorRenderer(BaseRenderer):
    @property
    def name(self):
        return "Selector config"


class SecretRenderer(BaseRenderer):
    def __init__(self, cli_vars: Optional[Dict[str, Any]] = None) -> None:
        # Generate contexts here because we want to save the context
        # object in order to retrieve the env_vars.
        if cli_vars is None:
            cli_vars = {}
        self.ctx_obj = SecretContext(cli_vars)
        context = self.ctx_obj.to_dict()
        super().__init__(context)

    @property
    def name(self):
        return "Secret"


class ProfileRenderer(SecretRenderer):
    @property
    def name(self):
        return "Profile"


class PackageRenderer(SecretRenderer):
    @property
    def name(self):
        return "Packages config"
