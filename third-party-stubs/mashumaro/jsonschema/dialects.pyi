from _typeshed import Incomplete

class JSONSchemaDialect:
    uri: str
    definitions_root_pointer: str
    all_refs: bool
    def __init__(self, uri, definitions_root_pointer, all_refs) -> None: ...

class JSONSchemaDraft202012Dialect(JSONSchemaDialect):
    uri: str
    definitions_root_pointer: str
    all_refs: bool
    def __init__(self, uri, definitions_root_pointer, all_refs) -> None: ...

class OpenAPISchema31Dialect(JSONSchemaDialect):
    uri: str
    definitions_root_pointer: str
    all_refs: bool
    def __init__(self, uri, definitions_root_pointer, all_refs) -> None: ...

DRAFT_2020_12: Incomplete
OPEN_API_3_1: Incomplete
