from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional

from dbt.contracts.graph.unparsed import NodeVersion


@dataclass
class ModelNodeArgs:
    name: str
    package_name: str
    identifier: str
    schema: str
    database: Optional[str] = None
    relation_name: Optional[str] = None
    version: Optional[NodeVersion] = None
    latest_version: Optional[NodeVersion] = None
    deprecation_date: Optional[datetime] = None
    generated_at: datetime = field(default_factory=datetime.utcnow)
