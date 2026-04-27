from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, List, Optional

from dbt.adapters.catalogs import CatalogIntegrationConfig
from dbt_common.dataclass_schema import dbtClassMixin
from dbt_common.exceptions import DbtValidationError


@dataclass
class CatalogWriteIntegrationConfig(CatalogIntegrationConfig):
    name: str
    catalog_type: str
    external_volume: Optional[str] = None
    table_format: Optional[str] = None
    catalog_name: Optional[str] = None
    file_format: Optional[str] = None
    adapter_properties: Dict[str, Any] = field(default_factory=dict)


@dataclass
class Catalog(dbtClassMixin):
    name: str
    active_write_integration: Optional[str] = None
    write_integrations: List[CatalogWriteIntegrationConfig] = field(default_factory=list)


# ===== catalogs.yml v2 types =====


class V2CatalogType(str, Enum):
    HORIZON = "horizon"
    GLUE = "glue"
    ICEBERG_REST = "iceberg_rest"
    UNITY = "unity"
    HIVE_METASTORE = "hive_metastore"
    BIGLAKE_METASTORE = "biglake_metastore"


class V2TableFormat(str, Enum):
    DEFAULT = "default"
    ICEBERG = "iceberg"


@dataclass
class CatalogV2PlatformConfig:
    snowflake: Optional[Dict[str, Any]] = None
    databricks: Optional[Dict[str, Any]] = None
    bigquery: Optional[Dict[str, Any]] = None


@dataclass
class CatalogV2:
    name: str
    catalog_type: V2CatalogType
    table_format: V2TableFormat
    config: CatalogV2PlatformConfig


def _check_bool(field_name: str, val: Any) -> None:
    if not isinstance(val, bool):
        if not (isinstance(val, str) and val.strip().lower() in ("true", "false")):
            raise DbtValidationError(f"'{field_name}' must be a boolean")


def _check_int_range(field_name: str, val: Any, max_val: int) -> None:
    try:
        v = int(val)
    except (ValueError, TypeError):
        raise DbtValidationError(f"'{field_name}' must be a non-negative integer")
    if not (0 <= v <= max_val):
        raise DbtValidationError(f"'{field_name}' must be in 0..={max_val}")


def _check_enum(field_name: str, val: Any, allowed: set) -> None:
    if str(val).strip().lower() not in allowed:
        raise DbtValidationError(
            f"'{field_name}' value '{val}' is invalid. Must be one of: {sorted(allowed)}"
        )


@dataclass
class HorizonSnowflakeConfig:
    external_volume: str
    base_location_root: Optional[str] = None
    change_tracking: Optional[Any] = None
    data_retention_time_in_days: Optional[Any] = None
    max_data_extension_time_in_days: Optional[Any] = None
    storage_serialization_policy: Optional[str] = None

    def __post_init__(self) -> None:
        if not str(self.external_volume).strip():
            raise DbtValidationError("'external_volume' must be non-empty")
        if self.base_location_root is not None and not str(self.base_location_root).strip():
            raise DbtValidationError("'base_location_root' cannot be blank")
        if self.change_tracking is not None:
            _check_bool("change_tracking", self.change_tracking)
        if self.data_retention_time_in_days is not None:
            _check_int_range("data_retention_time_in_days", self.data_retention_time_in_days, 90)
        if self.max_data_extension_time_in_days is not None:
            _check_int_range(
                "max_data_extension_time_in_days", self.max_data_extension_time_in_days, 90
            )
        if self.storage_serialization_policy is not None:
            _check_enum(
                "storage_serialization_policy",
                self.storage_serialization_policy,
                {"compatible", "optimized"},
            )


@dataclass
class LinkedSnowflakeConfig:
    """Shared config for glue, iceberg_rest, and unity on snowflake."""

    catalog_database: str
    auto_refresh: Optional[Any] = None
    max_data_extension_time_in_days: Optional[Any] = None
    target_file_size: Optional[str] = None

    def __post_init__(self) -> None:
        if not str(self.catalog_database).strip():
            raise DbtValidationError("'catalog_database' must be non-empty")
        if self.auto_refresh is not None:
            _check_bool("auto_refresh", self.auto_refresh)
        if self.max_data_extension_time_in_days is not None:
            _check_int_range(
                "max_data_extension_time_in_days", self.max_data_extension_time_in_days, 90
            )
        if self.target_file_size is not None:
            _check_enum(
                "target_file_size",
                self.target_file_size,
                {"auto", "16mb", "32mb", "64mb", "128mb"},
            )


@dataclass
class UnityDatabricksConfig:
    file_format: str
    location_root: Optional[str] = None
    use_uniform: Optional[Any] = None

    def __post_init__(self) -> None:
        if not str(self.file_format).strip():
            raise DbtValidationError("'file_format' must be non-empty")
        use_uniform = self.use_uniform
        if use_uniform is not None:
            _check_bool("use_uniform", use_uniform)
            if isinstance(use_uniform, str):
                use_uniform = use_uniform.strip().lower() == "true"
        # fs issue #9648: file_format depends on use_uniform
        if use_uniform:
            if self.file_format.lower() != "delta":
                raise DbtValidationError("file_format must be 'delta' when 'use_uniform' is true")
        else:
            if self.file_format.lower() != "parquet":
                raise DbtValidationError(
                    "file_format must be 'parquet' when 'use_uniform' is false or unset"
                )
        if self.location_root is not None and not str(self.location_root).strip():
            raise DbtValidationError("'location_root' cannot be blank")


@dataclass
class HiveMetastoreDatabricksConfig:
    file_format: str

    def __post_init__(self) -> None:
        if str(self.file_format).lower() not in {"delta", "parquet", "hudi"}:
            raise DbtValidationError(
                f"file_format must be one of: {sorted(f.upper() for f in {'delta', 'parquet', 'hudi'})}"
            )


@dataclass
class BiglakeMetastoreBigqueryConfig:
    external_volume: str
    file_format: str
    base_location_root: Optional[str] = None

    def __post_init__(self) -> None:
        if not str(self.external_volume).strip():
            raise DbtValidationError("'external_volume' must be non-empty")
        if not self.external_volume.startswith("gs://"):
            raise DbtValidationError(
                "'external_volume' must be a path to a Cloud Storage bucket (gs://<bucket_name>)"
            )
        if str(self.file_format).lower() != "parquet":
            raise DbtValidationError("file_format must be 'parquet'")
        if self.base_location_root is not None and not str(self.base_location_root).strip():
            raise DbtValidationError("'base_location_root' cannot be blank")
