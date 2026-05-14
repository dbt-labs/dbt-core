import os
from copy import deepcopy
from typing import Any, Dict, List, Optional

from dbt.artifacts.resources import (
    Catalog,
    CatalogV2,
    CatalogWriteIntegrationConfig,
    V2TableFormat,
)
from dbt.clients.yaml_helper import load_yaml_text
from dbt.config.renderer import SecretRenderer
from dbt.constants import CATALOGS_FILE_NAME
from dbt.exceptions import YamlLoadError
from dbt_common.clients.system import load_file_contents
from dbt_common.exceptions import CompilationError, DbtValidationError


def load_catalogs_yml(project_dir: str, project_name: str) -> Dict[str, Any]:
    path = os.path.join(project_dir, CATALOGS_FILE_NAME)

    if os.path.isfile(path):
        try:
            contents = load_file_contents(path, strip=False)
            yaml_content = load_yaml_text(contents)

            if not yaml_content:
                raise DbtValidationError(f"The file at {path} is empty")

            return yaml_content
        except DbtValidationError as e:
            raise YamlLoadError(project_name=project_name, path=CATALOGS_FILE_NAME, exc=e)

    return {}


def load_single_catalog(raw_catalog: Dict[str, Any], renderer: SecretRenderer) -> Catalog:
    try:
        rendered_catalog = renderer.render_data(raw_catalog)
    except CompilationError as exc:
        raise DbtValidationError(str(exc)) from exc

    Catalog.validate(rendered_catalog)

    write_integrations = []
    write_integration_names = set()

    for raw_integration in rendered_catalog.get("write_integrations", []):
        if raw_integration["name"] in write_integration_names:
            raise DbtValidationError(
                f"Catalog '{rendered_catalog['name']}' cannot have multiple 'write_integrations' with the same name: '{raw_integration['name']}'."
            )

        # We're going to let the adapter validate the integration config
        write_integrations.append(
            CatalogWriteIntegrationConfig(**raw_integration, catalog_name=raw_catalog["name"])
        )
        write_integration_names.add(raw_integration["name"])

    # Validate + set default active_write_integration if unset
    active_write_integration = rendered_catalog.get("active_write_integration")
    valid_write_integration_names = [integration.name for integration in write_integrations]

    if not active_write_integration:
        if len(valid_write_integration_names) == 1:
            active_write_integration = write_integrations[0].name
        else:
            raise DbtValidationError(
                f"Catalog '{rendered_catalog['name']}' must specify an 'active_write_integration' when multiple 'write_integrations' are provided."
            )
    else:
        if active_write_integration not in valid_write_integration_names:
            raise DbtValidationError(
                f"Catalog '{rendered_catalog['name']}' must specify an 'active_write_integration' from its set of defined 'write_integrations': {valid_write_integration_names}. Got: '{active_write_integration}'."
            )

    return Catalog(
        name=raw_catalog["name"],
        active_write_integration=active_write_integration,
        write_integrations=write_integrations,
    )


def load_catalogs(project_dir: str, project_name: str, cli_vars: Dict[str, Any]) -> List[Catalog]:
    raw_catalogs = load_catalogs_yml(project_dir, project_name).get("catalogs", [])
    catalogs_renderer = SecretRenderer(cli_vars)

    return [load_single_catalog(raw_catalog, catalogs_renderer) for raw_catalog in raw_catalogs]


def get_active_write_integration(catalog: Catalog) -> Optional[CatalogWriteIntegrationConfig]:
    for integration in catalog.write_integrations:
        if integration.name == catalog.active_write_integration:
            active_integration = deepcopy(integration)
            active_integration.catalog_name = active_integration.name
            active_integration.name = catalog.name
            return active_integration

    return None


# ===== catalogs.yml v2 =====

_VALID_V2_TABLE_FORMATS = {t.value for t in V2TableFormat}
_VALID_TOP_LEVEL_KEYS = {"name", "type", "table_format", "config"}


def load_catalogs_v2(
    project_dir: str, project_name: str, cli_vars: Dict[str, Any]
) -> List[CatalogV2]:
    raw_yaml = load_catalogs_yml(project_dir, project_name)
    if not raw_yaml:
        return []

    if "iceberg_catalogs" in raw_yaml:
        raise DbtValidationError("v2 catalogs.yml uses 'catalogs', not 'iceberg_catalogs'")

    unknown_file_keys = set(raw_yaml.keys()) - {"catalogs"}
    if unknown_file_keys:
        raise DbtValidationError(
            f"Unknown top-level keys in catalogs.yml: {sorted(unknown_file_keys)}. "
            f"Only 'catalogs' is allowed"
        )

    raw_catalogs = raw_yaml.get("catalogs", [])
    if not isinstance(raw_catalogs, list):
        raise DbtValidationError(
            f"'catalogs' in catalogs.yml must be a list, got {type(raw_catalogs).__name__}"
        )

    renderer = SecretRenderer(cli_vars)

    seen_names: set = set()
    catalogs: List[CatalogV2] = []

    for raw_catalog in raw_catalogs:
        catalog = load_single_catalog_v2(raw_catalog, renderer)
        if catalog.name in seen_names:
            raise DbtValidationError(f"Duplicate catalog name '{catalog.name}' in catalogs.yml")
        seen_names.add(catalog.name)
        catalogs.append(catalog)

    return catalogs


def load_single_catalog_v2(raw_catalog: Dict[str, Any], renderer: SecretRenderer) -> CatalogV2:
    if not isinstance(raw_catalog, dict):
        raise DbtValidationError(
            f"Each entry in catalogs.yml 'catalogs' must be a mapping, got {type(raw_catalog).__name__}"
        )

    try:
        rendered = renderer.render_data(raw_catalog)
    except CompilationError as exc:
        raise DbtValidationError(str(exc)) from exc

    unknown_keys = set(rendered.keys()) - _VALID_TOP_LEVEL_KEYS
    if unknown_keys:
        raise DbtValidationError(
            f"Unknown keys in catalog entry: {sorted(unknown_keys)}. "
            f"Allowed keys: {sorted(_VALID_TOP_LEVEL_KEYS)}"
        )

    for key in ("name", "type", "table_format", "config"):
        if key not in rendered:
            raise DbtValidationError(f"Missing required key '{key}' in catalog entry")

    name = rendered["name"]
    if not isinstance(name, str) or not name.strip():
        raise DbtValidationError("catalogs[].name must be a non-empty string")

    catalog_type = str(rendered["type"]).lower()

    raw_format = str(rendered["table_format"]).lower()
    if raw_format not in _VALID_V2_TABLE_FORMATS:
        raise DbtValidationError(
            f"Invalid table_format '{rendered['table_format']}'. "
            f"Must be one of {sorted(_VALID_V2_TABLE_FORMATS)}"
        )
    table_format = V2TableFormat(raw_format)

    config_raw = rendered["config"]
    if not isinstance(config_raw, dict):
        raise DbtValidationError(
            f"Catalog '{name}' config must be a mapping, got {type(config_raw).__name__}"
        )

    for platform, block in config_raw.items():
        if block is not None and not isinstance(block, dict):
            raise DbtValidationError(f"Catalog '{name}' config.{platform} must be a mapping")

    return CatalogV2(
        name=name.strip(),
        catalog_type=catalog_type,
        table_format=table_format,
        config={k: v for k, v in config_raw.items() if v is not None},
    )
