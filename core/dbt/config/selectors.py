from pathlib import Path
from typing import Dict, Any, Optional

from hologram import ValidationError

from .renderer import SelectorRenderer

from dbt.clients.system import (
    load_file_contents,
    path_exists,
    resolve_path_from_base,
)
from dbt.clients.yaml_helper import load_yaml_text
from dbt.contracts.selection import SelectorFile
from dbt.exceptions import DbtSelectorsError, RuntimeException
from dbt.graph import parse_from_selectors_definition, SelectionSpec

MALFORMED_SELECTOR_ERROR = """\
The selectors.yml file in this project is malformed. Please double check
the contents of this file and fix any errors before retrying.

You can find more information on the syntax for this file here:
https://docs.getdbt.com/docs/package-management

Validator Error:
{error}
"""


class SelectorConfig(Dict[str, SelectionSpec]):
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'SelectorConfig':
        try:
            selector_file = SelectorFile.from_dict(data)
            selectors = parse_from_selectors_definition(selector_file)
        except (ValidationError, RuntimeException) as exc:
            raise DbtSelectorsError(
                f'Could not read selector file data: {exc}',
                result_type='invalid_selector',
            ) from exc

        return cls(selectors)

    @classmethod
    def render_from_dict(
        cls,
        data: Dict[str, Any],
        renderer: SelectorRenderer,
    ) -> 'SelectorConfig':
        try:
            rendered = renderer.render_data(data)
        except (ValidationError, RuntimeException) as exc:
            raise DbtSelectorsError(
                f'Could not render selector data: {exc}',
                result_type='invalid_selector',
            ) from exc
        return cls.from_dict(rendered)

    @classmethod
    def from_path(
        cls, path: Path, renderer: SelectorRenderer,
    ) -> 'SelectorConfig':
        try:
            data = load_yaml_text(load_file_contents(str(path)))
        except (ValidationError, RuntimeException) as exc:
            raise DbtSelectorsError(
                f'Could not read selector file: {exc}',
                result_type='invalid_selector',
                path=path,
            ) from exc

        try:
            return cls.render_from_dict(data, renderer)
        except DbtSelectorsError as exc:
            exc.path = path
            raise


def selector_data_from_root(project_root: str) -> Dict[str, Any]:
    selector_filepath = resolve_path_from_base(
        'selectors.yml', project_root
    )

    if path_exists(selector_filepath):
        selectors_dict = load_yaml_text(load_file_contents(selector_filepath))
    else:
        selectors_dict = None
    return selectors_dict


def selector_config_from_data(
    selectors_data: Optional[Dict[str, Any]]
) -> SelectorConfig:
    if selectors_data is None:
        selectors_data = {'selectors': []}

    try:
        selectors = SelectorConfig.from_dict(selectors_data)
    except ValidationError as e:
        raise DbtSelectorsError(
            MALFORMED_SELECTOR_ERROR.format(error=str(e.message)),
            result_type='invalid_selector',
        ) from e
    return selectors
