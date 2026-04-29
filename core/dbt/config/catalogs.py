import os
from copy import deepcopy
from typing import Any, Dict, List, Optional

from dbt.adapters.catalogs import get_catalog_config
from dbt.artifacts.resources import (
    Catalog,
    CatalogV2,
    CatalogV2PlatformConfig,
    CatalogWriteIntegrationConfig,
    V2CatalogType,
    V2TableFormat,
)
from dbt.clients.yaml_helper import load_yaml_text
from dbt.config.renderer import SecretRenderer
from dbt.constants import CATALOGS_FILE_NAME
from dbt.exceptions import YamlLoadError
from dbt_common.clients.system import load_file_contents
from dbt_common.dataclass_schema import ValidationError as SchemaValidationError
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

_VALID_V2_CATALOG_TYPES = {t.value for t in V2CatalogType}
_VALID_V2_TABLE_FORMATS = {t.value for t in V2TableFormat}
_VALID_PLATFORMS = {"snowflake", "databricks", "bigquery"}
_VALID_TOP_LEVEL_KEYS = {"name", "type", "table_format", "config"}

# Type → supported platforms
_TYPE_PLATFORMS: Dict[V2CatalogType, List[str]] = {
    V2CatalogType.HORIZON: ["snowflake"],
    V2CatalogType.GLUE: ["snowflake"],
    V2CatalogType.ICEBERG_REST: ["snowflake"],
    V2CatalogType.UNITY: ["snowflake", "databricks"],
    V2CatalogType.HIVE_METASTORE: ["databricks"],
    V2CatalogType.BIGLAKE_METASTORE: ["bigquery"],
}

# Type → required table_format
_TYPE_TABLE_FORMAT: Dict[V2CatalogType, V2TableFormat] = {
    V2CatalogType.HORIZON: V2TableFormat.ICEBERG,
    V2CatalogType.GLUE: V2TableFormat.ICEBERG,
    V2CatalogType.ICEBERG_REST: V2TableFormat.ICEBERG,
    V2CatalogType.UNITY: V2TableFormat.ICEBERG,
    V2CatalogType.HIVE_METASTORE: V2TableFormat.DEFAULT,
    V2CatalogType.BIGLAKE_METASTORE: V2TableFormat.ICEBERG,
}

# v2 type + adapter_type → v1 catalog_type string (matching fs bridge)
_V2_TO_V1_CATALOG_TYPE: Dict[tuple, str] = {
    (V2CatalogType.HORIZON, "snowflake"): "BUILT_IN",
    (V2CatalogType.GLUE, "snowflake"): "ICEBERG_REST",
    (V2CatalogType.ICEBERG_REST, "snowflake"): "ICEBERG_REST",
    (V2CatalogType.UNITY, "snowflake"): "ICEBERG_REST",
    (V2CatalogType.UNITY, "databricks"): "unity",
    (V2CatalogType.HIVE_METASTORE, "databricks"): "hive_metastore",
    (V2CatalogType.BIGLAKE_METASTORE, "bigquery"): "biglake_metastore",
}


def load_catalogs_v2(
    project_dir: str, project_name: str, cli_vars: Dict[str, Any]
) -> List[CatalogV2]:
    raw_yaml = load_catalogs_yml(project_dir, project_name)
    if not raw_yaml:
        return []

    # Reject legacy v1 key
    if "iceberg_catalogs" in raw_yaml:
        raise DbtValidationError("v2 catalogs.yml uses 'catalogs', not 'iceberg_catalogs'")

    # Reject unknown top-level keys
    unknown_file_keys = set(raw_yaml.keys()) - {"catalogs"}
    if unknown_file_keys:
        raise DbtValidationError(
            f"Unknown top-level keys in catalogs.yml: {sorted(unknown_file_keys)}. "
            f"Only 'catalogs' is allowed"
        )

    raw_catalogs = raw_yaml.get("catalogs", [])
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
    try:
        rendered = renderer.render_data(raw_catalog)
    except CompilationError as exc:
        raise DbtValidationError(str(exc)) from exc

    # Reject unknown top-level keys
    unknown_keys = set(rendered.keys()) - _VALID_TOP_LEVEL_KEYS
    if unknown_keys:
        raise DbtValidationError(
            f"Unknown keys in catalog entry: {sorted(unknown_keys)}. "
            f"Allowed keys: {sorted(_VALID_TOP_LEVEL_KEYS)}"
        )

    # Validate required keys
    for key in ("name", "type", "table_format", "config"):
        if key not in rendered:
            raise DbtValidationError(f"Missing required key '{key}' in catalog entry")

    name = rendered["name"]
    if not isinstance(name, str) or not name.strip():
        raise DbtValidationError("catalogs[].name must be a non-empty string")

    # Validate type
    raw_type = str(rendered["type"]).lower()
    if raw_type not in _VALID_V2_CATALOG_TYPES:
        raise DbtValidationError(
            f"Invalid catalog type '{rendered['type']}'. "
            f"Must be one of: {sorted(_VALID_V2_CATALOG_TYPES)}"
        )
    catalog_type = V2CatalogType(raw_type)

    # Validate table_format
    raw_format = str(rendered["table_format"]).lower()
    if raw_format not in _VALID_V2_TABLE_FORMATS:
        raise DbtValidationError(
            f"Invalid table_format '{rendered['table_format']}'. "
            f"Must be {sorted(_VALID_V2_TABLE_FORMATS)}"
        )
    table_format = V2TableFormat(raw_format)

    # Validate config is a dict with only known platform keys
    config_raw = rendered["config"]
    if not isinstance(config_raw, dict):
        raise DbtValidationError(
            f"Catalog '{name}' config must be a mapping, got {type(config_raw).__name__}"
        )

    unknown_platforms = set(config_raw.keys()) - _VALID_PLATFORMS
    if unknown_platforms:
        raise DbtValidationError(
            f"Catalog '{name}' config contains unknown platform keys: {sorted(unknown_platforms)}. "
            f"Allowed: {sorted(_VALID_PLATFORMS)}"
        )

    # Ensure platform blocks are dicts
    for platform in _VALID_PLATFORMS:
        block = config_raw.get(platform)
        if block is not None and not isinstance(block, dict):
            raise DbtValidationError(f"Catalog '{name}' config.{platform} must be a mapping")

    config = CatalogV2PlatformConfig(
        snowflake=config_raw.get("snowflake"),
        databricks=config_raw.get("databricks"),
        bigquery=config_raw.get("bigquery"),
    )

    return CatalogV2(
        name=name.strip(),
        catalog_type=catalog_type,
        table_format=table_format,
        config=config,
    )


def _get_platform_block(catalog: CatalogV2, platform: str) -> Optional[Dict[str, Any]]:
    return getattr(catalog.config, platform, None)


def _validate_platform_block(catalog: CatalogV2, platform: str) -> None:
    """Look up the registered v2 schema for (catalog_type, platform) and validate the block.

    Returns silently if no block was provided for this platform — the caller is responsible
    for enforcing required-platform rules. Raises if the block is present but no schema is
    registered (the adapter does not yet support v2 catalogs of this type).
    """
    block = _get_platform_block(catalog, platform)
    if block is None:
        return

    catalog_type = catalog.catalog_type.value
    config_class = get_catalog_config(catalog_type, platform)
    if config_class is None:
        raise DbtValidationError(
            f"Catalog '{catalog.name}' type '{catalog_type}' on platform '{platform}': "
            f"no v2 catalog schema registered. The adapter may not yet support v2 catalogs "
            f"of this type."
        )

    ctx = f"Catalog '{catalog.name}' {catalog_type}/{platform}"
    try:
        config_class.validate(block)
        config_class.from_dict(block)
    except SchemaValidationError as e:
        raise DbtValidationError(f"{ctx}: {e.message}")
    except DbtValidationError as e:
        raise DbtValidationError(f"{ctx}: {e}")


def validate_v2_catalog_for_platform(catalog: CatalogV2, adapter_type: str) -> None:
    """Phase 2 semantic validation: check type-specific platform support and field constraints."""
    ct = catalog.catalog_type
    name = catalog.name
    supported = _TYPE_PLATFORMS[ct]

    # Validate table_format matches what the type requires
    required_format = _TYPE_TABLE_FORMAT[ct]
    if catalog.table_format != required_format:
        raise DbtValidationError(
            f"Catalog '{name}' type '{ct.value}' requires table_format='{required_format.value}', "
            f"got '{catalog.table_format.value}'"
        )

    # Reject platform blocks not supported by this type
    for platform in _VALID_PLATFORMS:
        block = _get_platform_block(catalog, platform)
        if block is not None and platform not in supported:
            raise DbtValidationError(f"dbt does not support {platform} on the {ct.value} 'type'")

    # Required-platform rules: unity needs at least one supported platform; others need all of theirs
    if ct == V2CatalogType.UNITY:
        if all(_get_platform_block(catalog, p) is None for p in supported):
            raise DbtValidationError(
                f"Catalog '{name}' of type 'unity' requires at least one config block: "
                f"{' or '.join(supported)}"
            )
    else:
        for platform in supported:
            if _get_platform_block(catalog, platform) is None:
                raise DbtValidationError(
                    f"Catalog '{name}' type '{ct.value}' requires config.{platform}"
                )

    # Per-platform schema validation via the adapter-owned registry
    for platform in supported:
        _validate_platform_block(catalog, platform)


# v2 field name → v1 adapter_properties field name translations.
# The v2 spec uses cleaner names; the adapter expects v1 names.
_SNOWFLAKE_LINKED_FIELD_MAP: Dict[str, str] = {
    "catalog_database": "catalog_linked_database",
}

# v2 type → catalog_linked_database_type value for the adapter
_LINKED_DATABASE_TYPE: Dict[V2CatalogType, str] = {
    V2CatalogType.GLUE: "glue",
    V2CatalogType.UNITY: "unity",
}


def _translate_adapter_properties(
    properties: Dict[str, Any],
    field_map: Dict[str, str],
) -> Dict[str, Any]:
    """Rename keys in adapter_properties from v2 names to v1 adapter-expected names."""
    return {field_map.get(k, k): v for k, v in properties.items()}


def bridge_v2_catalog_to_integration(
    catalog: CatalogV2, adapter_type: str
) -> CatalogWriteIntegrationConfig:
    """Convert a validated v2 catalog into a CatalogWriteIntegrationConfig for the adapter."""
    ct = catalog.catalog_type
    key = (ct, adapter_type)

    v1_catalog_type = _V2_TO_V1_CATALOG_TYPE.get(key)
    if v1_catalog_type is None:
        raise DbtValidationError(
            f"Catalog '{catalog.name}' has type '{ct.value}'; "
            f"{adapter_type} does not support this catalog type"
        )

    platform_block = _get_platform_block(catalog, adapter_type) or {}

    # Extract fields that map to top-level CatalogWriteIntegrationConfig fields
    external_volume = platform_block.get("external_volume")
    file_format = platform_block.get("file_format")

    # Remaining fields go into adapter_properties
    top_level_fields = {"external_volume", "file_format"}
    adapter_properties = {k: v for k, v in platform_block.items() if k not in top_level_fields}

    # Translate v2 field names to v1 adapter-expected names for snowflake linked catalogs
    is_snowflake_linked = adapter_type == "snowflake" and ct in (
        V2CatalogType.GLUE,
        V2CatalogType.ICEBERG_REST,
        V2CatalogType.UNITY,
    )
    if is_snowflake_linked:
        adapter_properties = _translate_adapter_properties(
            adapter_properties, _SNOWFLAKE_LINKED_FIELD_MAP
        )
        # Add catalog_linked_database_type for glue/unity (adapter uses it for DDL)
        if ct in _LINKED_DATABASE_TYPE:
            adapter_properties["catalog_linked_database_type"] = _LINKED_DATABASE_TYPE[ct]

    return CatalogWriteIntegrationConfig(
        name=catalog.name,
        catalog_type=v1_catalog_type,
        catalog_name=catalog.name,
        table_format=(
            catalog.table_format.value.upper()
            if adapter_type == "snowflake"
            else catalog.table_format.value
        ),
        external_volume=str(external_volume) if external_volume is not None else None,
        file_format=str(file_format) if file_format is not None else None,
        adapter_properties=adapter_properties,
    )
