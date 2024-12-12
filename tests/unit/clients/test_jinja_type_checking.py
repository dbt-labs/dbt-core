from dbt.clients.jinja_macro_call import PRIMITIVE_TYPES, DbtMacroCall
from dbt_common.clients.jinja import MacroType

single_param_macro_text = """{% macro call_me(param: TYPE) %}
       {% endmacro %}"""


def test_primitive_type_checks():
    for type_name in PRIMITIVE_TYPES:
        macro_text = single_param_macro_text.replace("TYPE", type_name)
        call = DbtMacroCall("call_me", "call_me", [MacroType(type_name, [])], {})
        assert not any(call.check(macro_text))


def test_primitive_type_checks_wrong():
    for type_name in PRIMITIVE_TYPES:
        macro_text = single_param_macro_text.replace("TYPE", type_name)
        wrong_type = next(t for t in PRIMITIVE_TYPES if t != type_name)
        call = DbtMacroCall("call_me", "call_me", [MacroType(wrong_type, [])], {})
        assert any(call.check(macro_text))


def test_list_type_checks():
    for type_name in PRIMITIVE_TYPES:
        macro_text = single_param_macro_text.replace("TYPE", f"List[{type_name}]")
        expected_type = MacroType("List", [MacroType(type_name, [])])
        call = DbtMacroCall("call_me", "call_me", [expected_type], {})
        assert not any(call.check(macro_text))
