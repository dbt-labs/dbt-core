import os
from copy import deepcopy
from typing import Any, Dict, List, Optional, Set

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

# Allowed keys per (type, platform) — for unknown-key rejection
_ALLOWED_PLATFORM_KEYS: Dict[tuple, Set[str]] = {
    (V2CatalogType.HORIZON, "snowflake"): {
        "external_volume",
        "change_tracking",
        "data_retention_time_in_days",
        "max_data_extension_time_in_days",
        "storage_serialization_policy",
        "base_location_root",
    },
    (V2CatalogType.GLUE, "snowflake"): {
        "catalog_database",
        "auto_refresh",
        "max_data_extension_time_in_days",
        "target_file_size",
    },
    (V2CatalogType.ICEBERG_REST, "snowflake"): {
        "catalog_database",
        "auto_refresh",
        "max_data_extension_time_in_days",
        "target_file_size",
    },
    (V2CatalogType.UNITY, "snowflake"): {
        "catalog_database",
        "auto_refresh",
        "max_data_extension_time_in_days",
        "target_file_size",
    },
    (V2CatalogType.UNITY, "databricks"): {
        "file_format",
        "location_root",
        "use_uniform",
    },
    (V2CatalogType.HIVE_METASTORE, "databricks"): {
        "file_format",
    },
    (V2CatalogType.BIGLAKE_METASTORE, "bigquery"): {
        "external_volume",
        "file_format",
        "base_location_root",
    },
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

_VALID_STORAGE_SERIALIZATION_POLICIES = {"compatible", "optimized"}
_VALID_TARGET_FILE_SIZES = {"auto", "16mb", "32mb", "64mb", "128mb"}
_VALID_HIVE_FILE_FORMATS = {"delta", "parquet", "hudi"}


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

    seen_names: Set[str] = set()
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
            f"Must be one of: {sorted(_VALID_V2_TABLE_FORMATS)}"
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


def _require_non_empty_str(block: Dict[str, Any], key: str, context: str) -> str:
    val = block.get(key)
    if val is None:
        raise DbtValidationError(f"{context} requires '{key}'")
    val = str(val).strip()
    if not val:
        raise DbtValidationError(f"{context} '{key}' must be non-empty")
    return val


def _validate_optional_non_empty_str(block: Dict[str, Any], key: str, context: str) -> None:
    val = block.get(key)
    if val is not None and not str(val).strip():
        raise DbtValidationError(f"{context} '{key}' cannot be blank")


def _validate_optional_bool(block: Dict[str, Any], key: str, context: str) -> None:
    val = block.get(key)
    if val is None:
        return
    if isinstance(val, bool):
        return
    if isinstance(val, str) and val.strip().lower() in ("true", "false"):
        return
    raise DbtValidationError(f"{context} '{key}' must be a boolean")


def _validate_u32_range(block: Dict[str, Any], key: str, max_val: int, context: str) -> None:
    val = block.get(key)
    if val is None:
        return
    try:
        int_val = int(val)
    except (ValueError, TypeError):
        raise DbtValidationError(f"{context} '{key}' must be a non-negative integer")
    if int_val < 0 or int_val > max_val:
        raise DbtValidationError(f"{context} '{key}' must be in 0..={max_val}")


def _validate_enum_str(block: Dict[str, Any], key: str, allowed: Set[str], context: str) -> None:
    val = block.get(key)
    if val is None:
        return
    if str(val).strip().lower() not in allowed:
        raise DbtValidationError(
            f"{context} '{key}' value '{val}' is invalid. "
            f"Must be one of: {sorted(s.upper() for s in allowed)}"
        )


def _check_unknown_keys(block: Dict[str, Any], allowed: Set[str], context: str) -> None:
    unknown = set(block.keys()) - allowed
    if unknown:
        raise DbtValidationError(
            f"Unknown keys in {context}: {sorted(unknown)}. " f"Allowed: {sorted(allowed)}"
        )


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

    # Type-specific validation
    if ct == V2CatalogType.HORIZON:
        _validate_horizon(catalog)
    elif ct == V2CatalogType.GLUE:
        _validate_glue(catalog)
    elif ct == V2CatalogType.ICEBERG_REST:
        _validate_iceberg_rest(catalog)
    elif ct == V2CatalogType.UNITY:
        _validate_unity(catalog)
    elif ct == V2CatalogType.HIVE_METASTORE:
        _validate_hive_metastore(catalog)
    elif ct == V2CatalogType.BIGLAKE_METASTORE:
        _validate_biglake_metastore(catalog)


def _validate_horizon(catalog: CatalogV2) -> None:
    name = catalog.name
    ctx = f"Catalog '{name}' horizon/snowflake"
    snowflake = _get_platform_block(catalog, "snowflake")
    if snowflake is None:
        raise DbtValidationError(f"Catalog '{name}' type 'horizon' requires config.snowflake")

    allowed = _ALLOWED_PLATFORM_KEYS[(V2CatalogType.HORIZON, "snowflake")]
    _check_unknown_keys(snowflake, allowed, f"{ctx} config")

    _require_non_empty_str(snowflake, "external_volume", ctx)
    _validate_optional_non_empty_str(snowflake, "base_location_root", ctx)
    _validate_optional_bool(snowflake, "change_tracking", ctx)
    _validate_u32_range(snowflake, "data_retention_time_in_days", 90, ctx)
    _validate_u32_range(snowflake, "max_data_extension_time_in_days", 90, ctx)
    _validate_enum_str(
        snowflake,
        "storage_serialization_policy",
        _VALID_STORAGE_SERIALIZATION_POLICIES,
        ctx,
    )


def _validate_snowflake_linked(catalog: CatalogV2, type_name: str) -> None:
    """Shared validation for glue, iceberg_rest, and unity on snowflake."""
    name = catalog.name
    ctx = f"Catalog '{name}' {type_name}/snowflake"
    snowflake = _get_platform_block(catalog, "snowflake")
    if snowflake is None:
        raise DbtValidationError(f"Catalog '{name}' type '{type_name}' requires config.snowflake")

    ct = V2CatalogType(type_name) if type_name != "unity" else V2CatalogType.UNITY
    allowed = _ALLOWED_PLATFORM_KEYS.get((ct, "snowflake"))
    if allowed:
        _check_unknown_keys(snowflake, allowed, f"{ctx} config")

    _require_non_empty_str(snowflake, "catalog_database", ctx)
    _validate_optional_bool(snowflake, "auto_refresh", ctx)
    _validate_u32_range(snowflake, "max_data_extension_time_in_days", 90, ctx)
    _validate_enum_str(snowflake, "target_file_size", _VALID_TARGET_FILE_SIZES, ctx)


def _validate_glue(catalog: CatalogV2) -> None:
    _validate_snowflake_linked(catalog, "glue")


def _validate_iceberg_rest(catalog: CatalogV2) -> None:
    _validate_snowflake_linked(catalog, "iceberg_rest")


def _validate_unity(catalog: CatalogV2) -> None:
    name = catalog.name
    snowflake = _get_platform_block(catalog, "snowflake")
    databricks = _get_platform_block(catalog, "databricks")

    if snowflake is None and databricks is None:
        raise DbtValidationError(
            f"Catalog '{name}' of type 'unity' requires at least one config block: "
            f"snowflake or databricks"
        )

    if snowflake is not None:
        _validate_snowflake_linked(catalog, "unity")

    if databricks is not None:
        ctx = f"Catalog '{name}' unity/databricks"
        allowed = _ALLOWED_PLATFORM_KEYS[(V2CatalogType.UNITY, "databricks")]
        _check_unknown_keys(databricks, allowed, f"{ctx} config")

        file_format = _require_non_empty_str(databricks, "file_format", ctx)
        if file_format.lower() != "delta":
            raise DbtValidationError(f"{ctx} file_format must be 'delta'")

        _validate_optional_non_empty_str(databricks, "location_root", ctx)
        _validate_optional_bool(databricks, "use_uniform", ctx)


def _validate_hive_metastore(catalog: CatalogV2) -> None:
    name = catalog.name
    ctx = f"Catalog '{name}' hive_metastore/databricks"
    databricks = _get_platform_block(catalog, "databricks")
    if databricks is None:
        raise DbtValidationError(
            f"Catalog '{name}' type 'hive_metastore' requires config.databricks"
        )

    allowed = _ALLOWED_PLATFORM_KEYS[(V2CatalogType.HIVE_METASTORE, "databricks")]
    _check_unknown_keys(databricks, allowed, f"{ctx} config")

    file_format = _require_non_empty_str(databricks, "file_format", ctx)
    if file_format.lower() not in _VALID_HIVE_FILE_FORMATS:
        raise DbtValidationError(
            f"{ctx} file_format must be one of: {sorted(f.upper() for f in _VALID_HIVE_FILE_FORMATS)}"
        )


def _validate_biglake_metastore(catalog: CatalogV2) -> None:
    name = catalog.name
    ctx = f"Catalog '{name}' biglake_metastore/bigquery"
    bigquery = _get_platform_block(catalog, "bigquery")
    if bigquery is None:
        raise DbtValidationError(
            f"Catalog '{name}' type 'biglake_metastore' requires config.bigquery"
        )

    allowed = _ALLOWED_PLATFORM_KEYS[(V2CatalogType.BIGLAKE_METASTORE, "bigquery")]
    _check_unknown_keys(bigquery, allowed, f"{ctx} config")

    external_volume = _require_non_empty_str(bigquery, "external_volume", ctx)
    if not external_volume.startswith("gs://"):
        raise DbtValidationError(
            f"{ctx} 'external_volume' must be a path to a Cloud Storage bucket (gs://<bucket_name>)"
        )

    file_format = _require_non_empty_str(bigquery, "file_format", ctx)
    if file_format.lower() != "parquet":
        raise DbtValidationError(f"{ctx} file_format must be 'parquet'")

    _validate_optional_non_empty_str(bigquery, "base_location_root", ctx)


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
