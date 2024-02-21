from dataclasses import dataclass, field
from typing import Optional, Literal
from dbt_common.dataclass_schema import ValidationError
from dbt.artifacts.resources.types import NodeType
from dbt.artifacts.resources.v1.components import MacroDependsOn, DeferRelation, ParsedResource
from dbt.artifacts.resources.v1.config import NodeConfig


@dataclass
class SeedConfig(NodeConfig):
    materialized: str = "seed"
    delimiter: str = ","
    quote_columns: Optional[bool] = None

    @classmethod
    def validate(cls, data):
        super().validate(data)
        if data.get("materialized") and data.get("materialized") != "seed":
            raise ValidationError("A seed must have a materialized value of 'seed'")


@dataclass
class Seed(ParsedResource):  # No SQLDefaults!
    resource_type: Literal[NodeType.Seed]
    config: SeedConfig = field(default_factory=SeedConfig)
    # seeds need the root_path because the contents are not loaded initially
    # and we need the root_path to load the seed later
    root_path: Optional[str] = None
    depends_on: MacroDependsOn = field(default_factory=MacroDependsOn)
    defer_relation: Optional[DeferRelation] = None
