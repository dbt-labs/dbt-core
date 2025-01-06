import os
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

from dbt.adapters.contracts.catalog import CatalogIntegrationType
from dbt.adapters.relation_configs.formats import TableFormat
from dbt.clients.yaml_helper import load_yaml_text
from dbt.config.renderer import SecretRenderer
from dbt_common.clients.system import load_file_contents
from dbt_common.dataclass_schema import dbtClassMixin
from dbt_common.exceptions import CompilationError, DbtValidationError


@dataclass
class CatalogIntegration(dbtClassMixin):
    name: str
    external_volume: str
    table_format: TableFormat
    catalog_type: CatalogIntegrationType


# satisfies dbt.adapters.protocol.CatalogIntegrationConfigProtocol
@dataclass
class AdapterCatalogIntegration:
    catalog_name: str
    integration_name: str
    table_format: str
    catalog_type: str
    external_volume: Optional[str]
    namespace: Optional[str]
    adapter_configs: Optional[Dict]


@dataclass
class Catalog(dbtClassMixin):
    name: str
    # If not specified, active_write_integration defaults to the integration in integrations if there is only one.
    active_write_integration: Optional[str] = None
    write_integrations: List[CatalogIntegration] = field(default_factory=list)

    @classmethod
    def render(
        cls, raw_catalog: Dict[str, Any], renderer: SecretRenderer, default_profile_name: str
    ) -> "Catalog":
        try:
            rendered_catalog = renderer.render_data(raw_catalog)
        except CompilationError:
            # TODO: better error
            raise

        cls.validate(rendered_catalog)

        write_integrations = []
        for raw_write_integration in rendered_catalog.get("write_integrations", []):
            CatalogIntegration.validate(raw_write_integration)
            write_integrations.append(CatalogIntegration.from_dict(raw_write_integration))

        # Validate + set default active_write_integration if unset
        active_write_integration = rendered_catalog.get("active_write_integration")
        valid_write_integration_names = [integration.name for integration in write_integrations]
        if (
            active_write_integration
            and active_write_integration not in valid_write_integration_names
        ):
            raise DbtValidationError(
                f"Catalog '{rendered_catalog['name']}' must specify a 'active_write_integration' from its set of defined 'write_integrations': {valid_write_integration_names}. Got: '{active_write_integration}'."
            )
        elif len(write_integrations) > 1 and not active_write_integration:
            raise DbtValidationError(
                f"Catalog '{rendered_catalog['name']}' must specify an 'active_write_integration' when multiple 'write_integrations' are provided."
            )
        elif not active_write_integration and len(write_integrations) == 1:
            active_write_integration = write_integrations[0].name

        return cls(
            name=raw_catalog["name"],
            active_write_integration=active_write_integration,
            write_integrations=write_integrations,
        )


@dataclass
class Catalogs(dbtClassMixin):
    catalogs: List[Catalog]

    @classmethod
    def load(cls, catalog_dir: str, profile: str, cli_vars: Dict[str, Any]) -> "Catalogs":
        catalogs = []

        raw_catalogs = cls._read_catalogs(catalog_dir)

        catalogs_renderer = SecretRenderer(cli_vars)
        for raw_catalog in raw_catalogs.get("catalogs", []):
            catalog = Catalog.render(raw_catalog, catalogs_renderer, profile)
            catalogs.append(catalog)

        return cls(catalogs=catalogs)

    def get_active_adapter_write_catalog_integrations(self):
        adapter_catalog_integrations: List[AdapterCatalogIntegration] = []

        for catalog in self.catalogs:
            active_write_integration = list(
                filter(
                    lambda c: c.name == catalog.active_write_integration,
                    catalog.write_integrations,
                )
            )[0]

            adapter_catalog_integrations.append(
                AdapterCatalogIntegration(
                    catalog_name=catalog.name,
                    integration_name=catalog.active_write_integration,
                    table_format=active_write_integration.table_format,
                    catalog_type=active_write_integration.catalog_type,
                    external_volume=active_write_integration.external_volume,
                    namespace=None,  # namespaces on write_integrations are not yet supported
                    adapter_configs={},  # configs on write_integrations not yet supported
                )
            )

        return adapter_catalog_integrations

    @classmethod
    def _read_catalogs(cls, catalog_dir: str) -> Dict[str, Any]:
        path = os.path.join(catalog_dir, "catalogs.yml")

        contents = None
        if os.path.isfile(path):
            try:
                contents = load_file_contents(path, strip=False)
                yaml_content = load_yaml_text(contents)
                if not yaml_content:
                    # msg = f"The catalogs.yml file at {path} is empty"
                    # TODO: better error
                    raise ValueError
                    # raise DbtProfileError(INVALID_PROFILE_MESSAGE.format(error_string=msg))
                return yaml_content
            # TODO: better error
            except DbtValidationError:
                # msg = INVALID_PROFILE_MESSAGE.format(error_string=e)
                # raise DbtValidationError(msg) from e
                raise

        return {}
