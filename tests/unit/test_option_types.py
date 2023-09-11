from click import Option, BadParameter
import pytest

from dbt.cli.option_types import YAML


class TestYAML:
    @pytest.mark.parametrize(
        "raw_value,expected_converted_value",
        [
            ("{}", {}),
            ("{'test_var_key': 'test_var_value'}", {"test_var_key": "test_var_value"}),
        ],
    )
    def test_yaml_init(self, raw_value, expected_converted_value):
        converted_value = YAML().convert(raw_value, Option(["--vars"]), None)
        assert converted_value == expected_converted_value

    @pytest.mark.parametrize(
        "invalid_yaml_str",
        ["{", ""],
    )
    def test_yaml_init_invalid_yaml_str(self, invalid_yaml_str):
        with pytest.raises(BadParameter) as e:
            YAML().convert(invalid_yaml_str, Option(["--vars"]), None)
        assert "--vars" in e.value.format_message()
