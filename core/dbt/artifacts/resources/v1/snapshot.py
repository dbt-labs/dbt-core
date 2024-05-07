from typing import Dict, Union, List, Optional, Literal
from dataclasses import dataclass
from dbt_common.dataclass_schema import ValidationError
from dbt.artifacts.resources.types import NodeType
from dbt.artifacts.resources.v1.components import CompiledResource, DeferRelation
from dbt.artifacts.resources.v1.config import NodeConfig


@dataclass
class SnapshotConfig(NodeConfig):
    materialized: str = "snapshot"
    strategy: Optional[str] = None
    unique_key: Optional[str] = None
    target_schema: Optional[str] = None
    target_database: Optional[str] = None
    updated_at: Optional[str] = None
    # Not using Optional because of serialization issues with a Union of str and List[str]
    check_cols: Union[str, List[str], None] = None

    @classmethod
    def validate(cls, data):
        super().validate(data)
        # Note: currently you can't just set these keys in schema.yml because this validation
        # will fail when parsing the snapshot node.
        if not data.get("strategy") or not data.get("unique_key") or not data.get("target_schema"):
            raise ValidationError(
                "Snapshots must be configured with a 'strategy', 'unique_key', "
                "and 'target_schema'."
            )
        if data.get("strategy") == "check":
            if not data.get("check_cols"):
                raise ValidationError(
                    "A snapshot configured with the check strategy must "
                    "specify a check_cols configuration."
                )
            if isinstance(data["check_cols"], str) and data["check_cols"] != "all":
                raise ValidationError(
                    f"Invalid value for 'check_cols': {data['check_cols']}. "
                    "Expected 'all' or a list of strings."
                )
        elif data.get("strategy") == "timestamp":
            if not data.get("updated_at"):
                raise ValidationError(
                    "A snapshot configured with the timestamp strategy "
                    "must specify an updated_at configuration."
                )
            if data.get("check_cols"):
                raise ValidationError("A 'timestamp' snapshot should not have 'check_cols'")
        # If the strategy is not 'check' or 'timestamp' it's a custom strategy,
        # formerly supported with GenericSnapshotConfig

        if data.get("materialized") and data.get("materialized") != "snapshot":
            raise ValidationError("A snapshot must have a materialized value of 'snapshot'")

    # Called by "calculate_node_config_dict" in ContextConfigGenerator
    def finalize_and_validate(self):
        data = self.to_dict(omit_none=True)
        self.validate(data)
        return self.from_dict(data)


@dataclass
class Snapshot(CompiledResource):
    resource_type: Literal[NodeType.Snapshot]
    config: SnapshotConfig
    defer_relation: Optional[DeferRelation] = None

    def __post_serialize__(self, dct, context: Optional[Dict] = None):
        dct = super().__post_serialize__(dct, context)
        if context and context.get("artifact") and "defer_relation" in dct:
            del dct["defer_relation"]
        return dct
