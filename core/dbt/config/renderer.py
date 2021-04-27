from typing import Dict, Any, Tuple, Optional, Union, Callable

from dbt.clients.jinja import get_rendered, catch_jinja

from dbt.exceptions import (
    DbtProjectError, CompilationException, RecursionException
)
from dbt.node_types import NodeType
from dbt.utils import deep_map


Keypath = Tuple[Union[str, int], ...]


class BaseRenderer:
    def __init__(self, context: Dict[str, Any]) -> None:
        self.context = context

    @property
    def name(self):
        return 'Rendering'

    def should_render_keypath(self, keypath: Keypath) -> bool:
        return True

    def render_entry(self, value: Any, keypath: Keypath) -> Any:
        if not self.should_render_keypath(keypath):
            return value

        return self.render_value(value, keypath)

    def render_value(
        self, value: Any, keypath: Optional[Keypath] = None
    ) -> Any:
        # keypath is ignored.
        # if it wasn't read as a string, ignore it
        if not isinstance(value, str):
            return value
        try:
            with catch_jinja():
                return get_rendered(value, self.context, native=True)
        except CompilationException as exc:
            msg = f'Could not render {value}: {exc.msg}'
            raise CompilationException(msg) from exc

    def render_data(
        self, data: Dict[str, Any]
    ) -> Dict[str, Any]:
        try:
            return deep_map(self.render_entry, data)
        except RecursionException:
            raise DbtProjectError(
                f'Cycle detected: {self.name} input has a reference to itself',
                project=data
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

        self[('on-run-start',)] = _list_if_none_or_string
        self[('on-run-end',)] = _list_if_none_or_string

        for k in ('models', 'seeds', 'snapshots'):
            self[(k,)] = _dict_if_none
            self[(k, 'vars')] = _dict_if_none
            self[(k, 'pre-hook')] = _list_if_none_or_string
            self[(k, 'post-hook')] = _list_if_none_or_string
        self[('seeds', 'column_types')] = _dict_if_none

    def postprocess(self, value: Any, key: Keypath) -> Any:
        if key in self:
            handler = self[key]
            return handler(value)

        return value


class DbtProjectYamlRenderer(BaseRenderer):
    _KEYPATH_HANDLERS = ProjectPostprocessor()

    @property
    def name(self):
        'Project config'

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
        rendered_project['project-root'] = project_root
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
        if first in {'on-run-start', 'on-run-end', 'query-comment'}:
            return False

        # don't render vars blocks until runtime
        if first == 'vars':
            return False

        if first in {'seeds', 'models', 'snapshots', 'tests'}:
            keypath_parts = {
                (k.lstrip('+') if isinstance(k, str) else k)
                for k in keypath
            }
            # model-level hooks
            if 'pre-hook' in keypath_parts or 'post-hook' in keypath_parts:
                return False

        return True


class ProfileRenderer(BaseRenderer):
    @property
    def name(self):
        'Profile'


class SchemaYamlRenderer(BaseRenderer):
    DOCUMENTABLE_NODES = frozenset(
        n.pluralize() for n in NodeType.documentable()
    )

    @property
    def name(self):
        return 'Rendering yaml'

    def _is_norender_key(self, keypath: Keypath) -> bool:
        """
        models:
            - name: blah
            - description: blah
              tests: ...
            - columns:
                - name:
                - description: blah
                  tests: ...

        Return True if it's tests or description - those aren't rendered
        """
        if len(keypath) >= 2 and keypath[1] in ('tests', 'description'):
            return True

        if (
            len(keypath) >= 4 and
            keypath[1] == 'columns' and
            keypath[3] in ('tests', 'description')
        ):
            return True

        return False

    # don't render descriptions or test keyword arguments
    def should_render_keypath(self, keypath: Keypath) -> bool:
        if len(keypath) < 2:
            return True

        if keypath[0] not in self.DOCUMENTABLE_NODES:
            return True

        if len(keypath) < 3:
            return True

        if keypath[0] == NodeType.Source.pluralize():
            if keypath[2] == 'description':
                return False
            if keypath[2] == 'tables':
                if self._is_norender_key(keypath[3:]):
                    return False
        elif keypath[0] == NodeType.Macro.pluralize():
            if keypath[2] == 'arguments':
                if self._is_norender_key(keypath[3:]):
                    return False
            elif self._is_norender_key(keypath[1:]):
                return False
        else:  # keypath[0] in self.DOCUMENTABLE_NODES:
            if self._is_norender_key(keypath[1:]):
                return False
        return True


class PackageRenderer(BaseRenderer):
    @property
    def name(self):
        return 'Packages config'


class SelectorRenderer(BaseRenderer):
    @property
    def name(self):
        return 'Selector config'
