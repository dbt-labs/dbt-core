import unittest
from unittest.mock import MagicMock
from dbt.exceptions import ParsingError
from dbt.parser.models import verify_python_model_code
from tests.unit.utils import MockNode

class TestPythonModelVerification(unittest.TestCase):
    def test_valid_python_code(self):
        # Valid Python code
        code = """
import pandas as pd

def model(dbt, session):
    dbt.config(materialized='table')
    return pd.DataFrame()
"""
        node = MockNode(
            package="test_package",
            name="test_model",
            raw_code=code,
            original_file_path="models/test_model.py"
        )
        # Should not raise exception
        verify_python_model_code(node)

    def test_python_code_with_jinja(self):
        # Python code containing Jinja
        code = """
import pandas as pd

def model(dbt, session):
    dbt.config(materialized='{{ "table" }}')  # Jinja here
    return pd.DataFrame()
"""
        node = MockNode(
            package="test_package",
            name="test_model",
            raw_code=code,
            original_file_path="models/test_model.py"
        )

        with self.assertRaises(ParsingError):
            verify_python_model_code(node)
