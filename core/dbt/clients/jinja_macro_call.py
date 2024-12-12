import dataclasses
from typing import Any, Dict, List, Optional, Union

import jinja2

from dbt_common.clients.jinja import MacroType, get_environment

PRIMITIVE_TYPES = ["Any", "bool", "float", "int", "str"]


@dataclasses.dataclass
class TypeCheckFailure:
    msg: str


@dataclasses.dataclass
class DbtMacroCall:
    """An instance of this class represents a jinja macro call in a template
    for the purposes of recording information for type checking."""

    name: str
    source: str
    arg_types: List[Union[MacroType, None]] = dataclasses.field(default_factory=list)
    kwarg_types: Dict[str, Optional[MacroType]] = dataclasses.field(default_factory=dict)

    @classmethod
    def from_call(cls, call: jinja2.nodes.Call, name: str) -> "DbtMacroCall":
        dbt_call = cls(name, "")
        for arg in call.args:  # type: ignore
            dbt_call.arg_types.append(cls.get_type(arg))
        for arg in call.kwargs:  # type: ignore
            dbt_call.kwarg_types[arg.key] = cls.get_type(arg.value)
        return dbt_call

    @classmethod
    def get_type(cls, param: Any) -> Optional[MacroType]:
        if isinstance(param, jinja2.nodes.Name):
            return None  # TODO: infer types from variable names

        if isinstance(param, jinja2.nodes.Call):
            return None  # TODO: infer types from function/macro calls

        if isinstance(param, jinja2.nodes.Getattr):
            return None  # TODO: infer types from . operator

        if isinstance(param, jinja2.nodes.Concat):
            return None

        if isinstance(param, jinja2.nodes.Const):
            if isinstance(param.value, str):  # type: ignore
                return MacroType("str")
            elif isinstance(param.value, bool):  # type: ignore
                return MacroType("bool")
            elif isinstance(param.value, int):  # type: ignore
                return MacroType("int")
            elif isinstance(param.value, float):  # type: ignore
                return MacroType("float")
            elif param.value is None:  # type: ignore
                return None
            else:
                return None

        if isinstance(param, jinja2.nodes.Dict):
            return None

        return None

    def is_valid_type(self, t: MacroType) -> bool:
        if len(t.type_params) == 0 and t.name in PRIMITIVE_TYPES:
            return True
        elif (
            t.name == "Dict"
            and len(t.type_params) == 2
            and t.type_params[0].name in PRIMITIVE_TYPES
            and self.is_valid_type(t.type_params[1])
        ):
            return True
        elif (
            t.name in ["List", "Optional"]
            and len(t.type_params) == 1
            and self.is_valid_type(t.type_params[0])
        ):
            return True

        return False

    def check(self, macro_text: str) -> List[TypeCheckFailure]:
        failures: List[TypeCheckFailure] = []
        template = get_environment(None, capture_macros=True).parse(macro_text)
        jinja_macro = template.body[0]

        for arg_type in jinja_macro.arg_types:
            if not self.is_valid_type(arg_type):
                failures.append(TypeCheckFailure(msg="Invalid type."))

        for i, arg_type in enumerate(self.arg_types):
            expected_type = jinja_macro.arg_types[i]
            if arg_type != expected_type:
                failures.append(TypeCheckFailure(msg="Wrong type of parameter."))

        #        if len(self.arg_types) + len(self.kwarg_types) > len(jinja_macro.args):
        #            failures.append(
        #                TypeCheckFailure(msg=f"Wrong number of parameters.")
        #            )

        return failures
