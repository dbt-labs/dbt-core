from pathlib import Path
from typing import Dict, Any
from dbt.clients.yaml_helper import (  # noqa: F401
    yaml, Loader, Dumper, load_yaml_text
)
from dbt.dataclass_schema import ValidationError

from .renderer import SelectorRenderer

from dbt.clients.system import (
    load_file_contents,
    path_exists,
    resolve_path_from_base,
)
from dbt.contracts.selection import SelectorFile
from dbt.exceptions import DbtSelectorsError, RuntimeException
from dbt.graph import parse_from_selectors_definition, SelectionSpec
from dbt.graph.selector_spec import SelectionCriteria

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
    def selectors_from_dict(cls, data: Dict[str, Any]) -> 'SelectorConfig':
        try:
            SelectorFile.validate(data)
            selector_file = SelectorFile.from_dict(data)
            selectors = parse_from_selectors_definition(selector_file)
        except ValidationError as exc:
            yaml_sel_cfg = yaml.dump(exc.instance)
            raise DbtSelectorsError(
                f"Could not parse selector file data: \n{yaml_sel_cfg}\n"
                f"Valid root-level selector definitions: "
                f"union, intersection, string, dictionary. No lists. "
                f"\nhttps://docs.getdbt.com/reference/node-selection/"
                f"yaml-selectors",
                result_type='invalid_selector'
            ) from exc
        except RuntimeException as exc:
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
        return cls.selectors_from_dict(rendered)

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
    selectors_data: Dict[str, Any]
) -> SelectorConfig:
    if not selectors_data:
        selectors_data = {'selectors': []}

    try:
        selectors = SelectorConfig.selectors_from_dict(selectors_data)
    except ValidationError as e:
        raise DbtSelectorsError(
            MALFORMED_SELECTOR_ERROR.format(error=str(e.message)),
            result_type='invalid_selector',
        ) from e
    return selectors


# These are utilities to clean up the dictionary created from
# selectors.yml by turning the cli-string format entries into
# normalized dictionary entries. It parallels the flow in
# dbt/graph/cli.py. If changes are made there, it might
# be necessary to make changes here. Ideally it would be
# good to combine the two flows into one at some point.
class SelectorDict:

    @classmethod
    def parse_dict_definition(cls, definition):
        key = list(definition)[0]
        value = definition[key]
        if isinstance(value, list):
            new_values = []
            for sel_def in value:
                new_value = cls.parse_from_definition(sel_def)
                new_values.append(new_value)
            value = new_values
        if key == 'exclude':
            definition = {key: value}
        elif len(definition) == 1:
            definition = {'method': key, 'value': value}
        return definition

    @classmethod
    def parse_a_definition(cls, def_type, definition):
        # this definition must be a list
        new_dict = {def_type: []}
        for sel_def in definition[def_type]:
            if isinstance(sel_def, dict):
                sel_def = cls.parse_from_definition(sel_def)
                new_dict[def_type].append(sel_def)
            elif isinstance(sel_def, str):
                sel_def = SelectionCriteria.dict_from_single_spec(sel_def)
                new_dict[def_type].append(sel_def)
            else:
                new_dict[def_type].append(sel_def)
        return new_dict

    @classmethod
    def parse_from_definition(cls, definition):
        if isinstance(definition, str):
            definition = SelectionCriteria.dict_from_single_spec(definition)
        elif 'union' in definition:
            definition = cls.parse_a_definition('union', definition)
        elif 'intersection' in definition:
            definition = cls.parse_a_definition('intersection', definition)
        elif isinstance(definition, dict):
            definition = cls.parse_dict_definition(definition)
        return definition

    # This is the normal entrypoint of this code. Give it the
    # list of selectors generated from the selectors.yml file.
    @classmethod
    def parse_from_selectors_list(cls, selectors):
        selector_dict = {}
        for selector in selectors:
            sel_name = selector['name']
            selector_dict[sel_name] = selector
            definition = cls.parse_from_definition(selector['definition'])
            selector_dict[sel_name]['definition'] = definition
        return selector_dict
