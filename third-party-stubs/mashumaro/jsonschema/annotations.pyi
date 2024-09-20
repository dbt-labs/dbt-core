from mashumaro.jsonschema.models import JSONSchema, Number
from typing import Dict, Set

class Annotation: ...
class Constraint(Annotation): ...
class NumberConstraint(Constraint): ...

class Minimum(NumberConstraint):
    value: Number
    def __init__(self, value) -> None: ...

class Maximum(NumberConstraint):
    value: Number
    def __init__(self, value) -> None: ...

class ExclusiveMinimum(NumberConstraint):
    value: Number
    def __init__(self, value) -> None: ...

class ExclusiveMaximum(NumberConstraint):
    value: Number
    def __init__(self, value) -> None: ...

class MultipleOf(NumberConstraint):
    value: Number
    def __init__(self, value) -> None: ...

class StringConstraint(Constraint): ...

class MinLength(StringConstraint):
    value: int
    def __init__(self, value) -> None: ...

class MaxLength(StringConstraint):
    value: int
    def __init__(self, value) -> None: ...

class Pattern(StringConstraint):
    value: str
    def __init__(self, value) -> None: ...

class ArrayConstraint(Constraint): ...

class MinItems(ArrayConstraint):
    value: int
    def __init__(self, value) -> None: ...

class MaxItems(ArrayConstraint):
    value: int
    def __init__(self, value) -> None: ...

class UniqueItems(ArrayConstraint):
    value: bool
    def __init__(self, value) -> None: ...

class Contains(ArrayConstraint):
    value: JSONSchema
    def __init__(self, value) -> None: ...

class MinContains(ArrayConstraint):
    value: int
    def __init__(self, value) -> None: ...

class MaxContains(ArrayConstraint):
    value: int
    def __init__(self, value) -> None: ...

class ObjectConstraint(Constraint): ...

class MaxProperties(ObjectConstraint):
    value: int
    def __init__(self, value) -> None: ...

class MinProperties(ObjectConstraint):
    value: int
    def __init__(self, value) -> None: ...

class DependentRequired(ObjectConstraint):
    value: Dict[str, Set[str]]
    def __init__(self, value) -> None: ...
