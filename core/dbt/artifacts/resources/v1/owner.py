from dataclasses import dataclass
from typing import Optional

from dbt_common.contracts.config.properties import AdditionalPropertiesAllowed


@dataclass
class Owner(AdditionalPropertiesAllowed):
    email: Optional[str] = None
    name: Optional[str] = None
