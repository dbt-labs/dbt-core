from typing import Protocol


class NodeProtocol(Protocol):
    name: str
    resource_type: str
    unique_id: str
    original_file_path: str
    schema: str
    alias: str
    is_relational: bool
    is_ephemeral_model: bool
    is_external_node: bool
