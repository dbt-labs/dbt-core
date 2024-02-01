from dataclasses import dataclass
from typing import Optional

from dbt_common.contracts.config.properties import AdditionalPropertiesAllowed
from dbt_common.contracts.util import Replaceable


@dataclass
class Owner(AdditionalPropertiesAllowed, Replaceable):
    email: Optional[str] = None
    name: Optional[str] = None
