import unittest

import dbt.exceptions
import dbt.utils
from dbt.parser.schema_renderer import SchemaYamlRenderer


class TestYamlRendering(unittest.TestCase):

    def test__models(self):

        context = {
            "test_var": "1234",
            "alt_var": "replaced",
        }
        renderer = SchemaYamlRenderer(context, 'models')

        # Verify description is not rendered and misc attribute is rendered
        dct = {
            "name": "my_model",
            "description": "{{ test_var }}",
            "attribute": "{{ test_var }}",
        }
        expected = {
            "name": "my_model",
            "description": "{{ test_var }}",
            "attribute": "1234",
        }
        dct = renderer.render_data(dct)
        self.assertEqual(expected, dct)

        # Verify description in columns is not rendered
        dct = {
            'name': 'my_test',
            'attribute': "{{ test_var }}",
            'columns': [
                {'description': "{{ test_var }}", 'name': 'id'},
            ]
        }
        expected = {
            'name': 'my_test',
            'attribute': "1234",
            'columns': [
                {'description': "{{ test_var }}", 'name': 'id'},
            ]
        }
        dct = renderer.render_data(dct)
        self.assertEqual(expected, dct)

    def test__sources(self):

        context = {
            "test_var": "1234",
            "alt_var": "replaced",
        }
        renderer = SchemaYamlRenderer(context, 'sources')

        # Only descriptions have jinja, none should be rendered
        dct = {
            "name": "my_source",
            "description": "{{ alt_var }}",
            "tables": [
                {
                    "name": "my_table",
                    "description": "{{ alt_var }}",
                    "columns": [
                        {
                            "name": "id",
                            "description": "{{ alt_var }}",
                        }
                    ]
                }
            ]
        }
        rendered = renderer.render_data(dct)
        self.assertEqual(dct, rendered)

    def test__macros(self):

        context = {
            "test_var": "1234",
            "alt_var": "replaced",
        }
        renderer = SchemaYamlRenderer(context, 'macros')

        # Look for description in arguments
        dct = {
            "name": "my_macro",
            "arguments": [
                {"name": "my_arg", "attr": "{{ alt_var }}"},
                {"name": "an_arg", "description": "{{ alt_var}}"}
            ]
        }
        expected = {
            "name": "my_macro",
            "arguments": [
                {"name": "my_arg", "attr": "replaced"},
                {"name": "an_arg", "description": "{{ alt_var}}"}
            ]
        }
        dct = renderer.render_data(dct)
        self.assertEqual(dct, expected)

