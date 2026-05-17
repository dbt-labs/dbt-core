import unittest

from dbt.parser.schema_renderer import SchemaYamlRenderer


class TestYamlRendering(unittest.TestCase):
    def test__models(self):

        context = {
            "test_var": "1234",
            "alt_var": "replaced",
        }
        renderer = SchemaYamlRenderer(context, "models")

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
            "name": "my_test",
            "attribute": "{{ test_var }}",
            "columns": [
                {"description": "{{ test_var }}", "name": "id"},
            ],
        }
        expected = {
            "name": "my_test",
            "attribute": "1234",
            "columns": [
                {"description": "{{ test_var }}", "name": "id"},
            ],
        }
        dct = renderer.render_data(dct)
        self.assertEqual(expected, dct)

    def test__sources(self):

        context = {
            "test_var": "1234",
            "alt_var": "replaced",
        }
        renderer = SchemaYamlRenderer(context, "sources")

        # Only descriptions have jinja, none should be rendered
        dct = {
            "name": "my_source",
            "description": "{{ alt_var }}",
            "loaded_at_query": "select max(ordered_at) from {{ this }}",
            "tables": [
                {
                    "name": "my_table",
                    "description": "{{ alt_var }}",
                    "loaded_at_query": "select max(ordered_at) from {{ this }}",
                    "columns": [
                        {
                            "name": "id",
                            "description": "{{ alt_var }}",
                        }
                    ],
                }
            ],
        }
        rendered = renderer.render_data(dct)
        self.assertEqual(dct, rendered)

    def test__macros(self):

        context = {
            "test_var": "1234",
            "alt_var": "replaced",
        }
        renderer = SchemaYamlRenderer(context, "macros")

        # Look for description in arguments
        dct = {
            "name": "my_macro",
            "arguments": [
                {"name": "my_arg", "attr": "{{ alt_var }}"},
                {"name": "an_arg", "description": "{{ alt_var}}"},
            ],
        }
        expected = {
            "name": "my_macro",
            "arguments": [
                {"name": "my_arg", "attr": "replaced"},
                {"name": "an_arg", "description": "{{ alt_var}}"},
            ],
        }
        dct = renderer.render_data(dct)
        self.assertEqual(dct, expected)

    def test__metrics(self):
        context = {"metric_name_end": "_metric"}
        renderer = SchemaYamlRenderer(context, "metrics")

        dct = {
            "name": "test{{ metric_name_end }}",
            "description": "{{ docs('my_doc') }}",
            "filter": "{{ Dimension('my_entity__my_dim') }} = false",
        }
        # We expect the expression and description will not be rendered, but
        # other fields will be
        expected = {
            "name": "test_metric",
            "description": "{{ docs('my_doc') }}",
            "filter": "{{ Dimension('my_entity__my_dim') }} = false",
        }
        dct = renderer.render_data(dct)
        self.assertEqual(dct, expected)

    def test__metrics_list_filter(self):
        """Test that list-type filters with Dimension jinja are not rendered for standalone metrics."""
        context = {"metric_name_end": "_metric"}
        renderer = SchemaYamlRenderer(context, "metrics")

        dct = {
            "name": "test{{ metric_name_end }}",
            "filter": [
                "{{ Dimension('my_entity__is_fraud') }} = false",
                "{{ Dimension('my_entity__is_employee') }} = false",
            ],
        }
        expected = {
            "name": "test_metric",
            "filter": [
                "{{ Dimension('my_entity__is_fraud') }} = false",
                "{{ Dimension('my_entity__is_employee') }} = false",
            ],
        }
        dct = renderer.render_data(dct)
        self.assertEqual(dct, expected)

    def test__metrics_nested_filter(self):
        """Test that deeply nested filters in standalone metrics (type_params) are not rendered."""
        context = {"metric_name_end": "_metric"}
        renderer = SchemaYamlRenderer(context, "metrics")

        # type_params.numerator.filter as string
        dct = {
            "name": "test{{ metric_name_end }}",
            "type_params": {
                "numerator": {
                    "name": "some_metric",
                    "filter": "{{ Dimension('my_entity__my_dim') }} > 0",
                },
            },
        }
        expected = {
            "name": "test_metric",
            "type_params": {
                "numerator": {
                    "name": "some_metric",
                    "filter": "{{ Dimension('my_entity__my_dim') }} > 0",
                },
            },
        }
        dct = renderer.render_data(dct)
        self.assertEqual(dct, expected)

        # type_params.numerator.filter as list
        dct = {
            "name": "test{{ metric_name_end }}",
            "type_params": {
                "numerator": {
                    "name": "some_metric",
                    "filter": [
                        "{{ Dimension('my_entity__is_fraud') }} = false",
                        "{{ Dimension('my_entity__is_employee') }} = false",
                    ],
                },
            },
        }
        expected = {
            "name": "test_metric",
            "type_params": {
                "numerator": {
                    "name": "some_metric",
                    "filter": [
                        "{{ Dimension('my_entity__is_fraud') }} = false",
                        "{{ Dimension('my_entity__is_employee') }} = false",
                    ],
                },
            },
        }
        dct = renderer.render_data(dct)
        self.assertEqual(dct, expected)

    def test__models_v2_metric_filter_string(self):
        """Test that string filters on v2 metrics (models key) are not rendered."""
        context = {"test_var": "1234"}
        renderer = SchemaYamlRenderer(context, "models")

        dct = {
            "name": "my_model",
            "attribute": "{{ test_var }}",
            "metrics": [
                {
                    "name": "my_metric",
                    "filter": "{{ Dimension('my_entity__my_dim') }} > 0",
                },
            ],
        }
        expected = {
            "name": "my_model",
            "attribute": "1234",
            "metrics": [
                {
                    "name": "my_metric",
                    "filter": "{{ Dimension('my_entity__my_dim') }} > 0",
                },
            ],
        }
        dct = renderer.render_data(dct)
        self.assertEqual(dct, expected)

    def test__models_v2_metric_filter_list(self):
        """Test that list-type filters on v2 metrics (models key) are not rendered.

        This is the critical regression test: before the fix, list filters
        like filter: ["{{ Dimension(...) }} = False", "{{ Dimension(...) }} = False"]
        would be rendered at parse time, causing 'Dimension is undefined' errors.
        """
        context = {"test_var": "1234"}
        renderer = SchemaYamlRenderer(context, "models")

        dct = {
            "name": "my_model",
            "attribute": "{{ test_var }}",
            "metrics": [
                {
                    "name": "my_metric",
                    "filter": [
                        "{{ Dimension('my_entity__is_fraud') }} = false",
                        "{{ Dimension('my_entity__is_employee') }} = false",
                    ],
                },
            ],
        }
        expected = {
            "name": "my_model",
            "attribute": "1234",
            "metrics": [
                {
                    "name": "my_metric",
                    "filter": [
                        "{{ Dimension('my_entity__is_fraud') }} = false",
                        "{{ Dimension('my_entity__is_employee') }} = false",
                    ],
                },
            ],
        }
        dct = renderer.render_data(dct)
        self.assertEqual(dct, expected)

    def test__models_v2_nested_metric_filters(self):
        """Test that nested metric filters (numerator, denominator, input_metrics, etc.)
        are not rendered for both string and list forms under the models key."""
        context = {"test_var": "1234"}
        renderer = SchemaYamlRenderer(context, "models")

        # numerator.filter (string)
        dct = {
            "name": "my_model",
            "metrics": [
                {
                    "name": "ratio_metric",
                    "numerator": {
                        "name": "base",
                        "filter": "{{ Dimension('my_entity__my_dim') }} > 0",
                    },
                },
            ],
        }
        expected_filter = "{{ Dimension('my_entity__my_dim') }} > 0"
        rendered = renderer.render_data(dct)
        self.assertEqual(rendered["metrics"][0]["numerator"]["filter"], expected_filter)

        # numerator.filter (list)
        dct = {
            "name": "my_model",
            "metrics": [
                {
                    "name": "ratio_metric",
                    "numerator": {
                        "name": "base",
                        "filter": [
                            "{{ Dimension('my_entity__is_fraud') }} = false",
                            "{{ Dimension('my_entity__is_employee') }} = false",
                        ],
                    },
                },
            ],
        }
        expected_filters = [
            "{{ Dimension('my_entity__is_fraud') }} = false",
            "{{ Dimension('my_entity__is_employee') }} = false",
        ]
        rendered = renderer.render_data(dct)
        self.assertEqual(rendered["metrics"][0]["numerator"]["filter"], expected_filters)

        # input_metrics[].filter (list)
        dct = {
            "name": "my_model",
            "metrics": [
                {
                    "name": "derived_metric",
                    "input_metrics": [
                        {
                            "name": "base",
                            "filter": [
                                "{{ Dimension('my_entity__is_fraud') }} = false",
                            ],
                        },
                    ],
                },
            ],
        }
        rendered = renderer.render_data(dct)
        self.assertEqual(
            rendered["metrics"][0]["input_metrics"][0]["filter"],
            ["{{ Dimension('my_entity__is_fraud') }} = false"],
        )

        # base_metric.filter (list, conversion metric)
        dct = {
            "name": "my_model",
            "metrics": [
                {
                    "name": "conversion_metric",
                    "base_metric": {
                        "name": "base",
                        "filter": [
                            "{{ Dimension('my_entity__is_fraud') }} = false",
                        ],
                    },
                },
            ],
        }
        rendered = renderer.render_data(dct)
        self.assertEqual(
            rendered["metrics"][0]["base_metric"]["filter"],
            ["{{ Dimension('my_entity__is_fraud') }} = false"],
        )

    def test__versioned_model_data_tests(self):
        """Test that data_tests inside version blocks are not rendered.

        Version-level data_tests may contain Jinja expressions like {{ ref() }}
        that are not available in the schema rendering context. These must be
        skipped and rendered later in the test compilation phase.
        """
        context = {"test_var": "1234"}
        renderer = SchemaYamlRenderer(context, "models")

        # Version-level data_tests should not be rendered
        dct = {
            "name": "my_model",
            "attribute": "{{ test_var }}",
            "versions": [
                {
                    "v": 1,
                    "data_tests": [
                        {
                            "compare_datasets": {
                                "source_query": "select * from {{ ref('other_model') }}",
                                "target_query": "select * from {{ ref('my_model') }}",
                            }
                        }
                    ],
                }
            ],
        }
        expected = {
            "name": "my_model",
            "attribute": "1234",
            "versions": [
                {
                    "v": 1,
                    "data_tests": [
                        {
                            "compare_datasets": {
                                "source_query": "select * from {{ ref('other_model') }}",
                                "target_query": "select * from {{ ref('my_model') }}",
                            }
                        }
                    ],
                }
            ],
        }
        dct = renderer.render_data(dct)
        self.assertEqual(expected, dct)

        # Version-level descriptions should not be rendered
        dct = {
            "name": "my_model",
            "versions": [
                {
                    "v": 1,
                    "description": "{{ test_var }}",
                }
            ],
        }
        rendered = renderer.render_data(dct)
        self.assertEqual(rendered["versions"][0]["description"], "{{ test_var }}")

        # Version-level column data_tests and descriptions should not be rendered
        dct = {
            "name": "my_model",
            "versions": [
                {
                    "v": 1,
                    "columns": [
                        {
                            "name": "id",
                            "description": "{{ test_var }}",
                            "data_tests": [{"not_null": {}}],
                        }
                    ],
                }
            ],
        }
        rendered = renderer.render_data(dct)
        self.assertEqual(rendered["versions"][0]["columns"][0]["description"], "{{ test_var }}")
        self.assertEqual(rendered["versions"][0]["columns"][0]["data_tests"], [{"not_null": {}}])

    def test__derived_semantics_descriptions(self):
        context = {
            "test_var": "1234",
        }
        renderer = SchemaYamlRenderer(context, "models")

        # Verify descriptions inside derived_semantics.dimensions and
        # derived_semantics.entities are not rendered (they contain doc()
        # Jinja that is resolved later in process_docs)
        dct = {
            "name": "my_model",
            "attribute": "{{ test_var }}",
            "derived_semantics": {
                "dimensions": [
                    {
                        "name": "my_dim",
                        "description": "{{ test_var }}",
                        "type": "categorical",
                    },
                ],
                "entities": [
                    {
                        "name": "my_entity",
                        "description": "{{ test_var }}",
                        "type": "foreign",
                    },
                ],
            },
        }
        expected = {
            "name": "my_model",
            "attribute": "1234",
            "derived_semantics": {
                "dimensions": [
                    {
                        "name": "my_dim",
                        "description": "{{ test_var }}",
                        "type": "categorical",
                    },
                ],
                "entities": [
                    {
                        "name": "my_entity",
                        "description": "{{ test_var }}",
                        "type": "foreign",
                    },
                ],
            },
        }
        dct = renderer.render_data(dct)
        self.assertEqual(expected, dct)
