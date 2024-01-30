from dataclasses import dataclass
from dbt_common.contracts.config.base import BaseConfig
from dbt_common.dataclass_schema import StrEnum


class ExposureType(StrEnum):
    Dashboard = "dashboard"
    Notebook = "notebook"
    Analysis = "analysis"
    ML = "ml"
    Application = "application"


class MaturityType(StrEnum):
    Low = "low"
    Medium = "medium"
    High = "high"


@dataclass
class ExposureConfig(BaseConfig):
    enabled: bool = True
