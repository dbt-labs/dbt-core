import os
from copy import deepcopy
from typing import Any, Dict, List, Optional

from dbt.artifacts.resources import Catalog, CatalogWriteIntegrationConfig
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
