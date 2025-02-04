from dataclasses import dataclass
from typing import List, Optional, Union

from dbt_common.contracts.config.properties import AdditionalPropertiesAllowed


@dataclass
class Owner(AdditionalPropertiesAllowed):
    email: Union[str, List[str], None] = None
    name: Optional[str] = None
