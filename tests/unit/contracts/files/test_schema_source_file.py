from dbt.contracts.files import SchemaSourceFile


def test_fix_metrics_from_measure():
    # This is a test for converting "generated_metrics" to "metrics_from_measures"
    schema_source_file = {
        "path": {
            "searched_path": "models",
            "relative_path": "schema.yml",
            "modification_time": 1721228094.7544806,
            "project_root": "/Users/a_user/sample_project",
        },
        "checksum": {
            "name": "sha256",
            "checksum": "63130d480a44a481aa0adc0a8469dccbb72ea36cc09f06683a584a31339f362e",
        },
        "project_name": "test",
        "parse_file_type": "schema",
        "dfy": {
            "models": [{"name": "fct_revenue", "description": "This is the model fct_revenue."}],
            "semantic_models": [
                {
                    "name": "revenue",
                    "description": "This is the FIRST semantic model.",
                    "model": "ref('fct_revenue')",
                    "defaults": {"agg_time_dimension": "ds"},
                    "measures": [
                        {
                            "name": "txn_revenue",
                            "expr": "revenue",
                            "agg": "sum",
                            "agg_time_dimension": "ds",
                            "create_metric": True,
                        },
                        {
                            "name": "sum_of_things",
                            "expr": 2,
                            "agg": "sum",
                            "agg_time_dimension": "ds",
                        },
                    ],
                    "dimensions": [
                        {
                            "name": "ds",
                            "type": "time",
                            "expr": "created_at",
                            "type_params": {"time_granularity": "day"},
                        }
                    ],
                    "entities": [
                        {"name": "user", "type": "foreign", "expr": "user_id"},
                        {"name": "id", "type": "primary"},
                    ],
                },
                {
                    "name": "alt_revenue",
                    "description": "This is the second revenue semantic model.",
                    "model": "ref('fct_revenue')",
                    "defaults": {"agg_time_dimension": "ads"},
                    "measures": [
                        {
                            "name": "alt_txn_revenue",
                            "expr": "revenue",
                            "agg": "sum",
                            "agg_time_dimension": "ads",
                            "create_metric": True,
                        },
                        {
                            "name": "alt_sum_of_things",
                            "expr": 2,
                            "agg": "sum",
                            "agg_time_dimension": "ads",
                        },
                    ],
                    "dimensions": [
                        {
                            "name": "ads",
                            "type": "time",
                            "expr": "created_at",
                            "type_params": {"time_granularity": "day"},
                        }
                    ],
                    "entities": [
                        {"name": "user", "type": "foreign", "expr": "user_id"},
                        {"name": "id", "type": "primary"},
                    ],
                },
            ],
            "metrics": [
                {
                    "name": "simple_metric",
                    "label": "Simple Metric",
                    "type": "simple",
                    "type_params": {"measure": "sum_of_things"},
                }
            ],
        },
        "data_tests": {},
        "metrics": ["metric.test.simple_metric"],
        "generated_metrics": ["metric.test.txn_revenue", "metric.test.alt_txn_revenue"],
        "metrics_from_measures": {},
        "ndp": ["model.test.fct_revenue"],
        "semantic_models": ["semantic_model.test.revenue", "semantic_model.test.alt_revenue"],
        "mcp": {},
        "env_vars": {},
    }

    expected_metrics_from_measures = {
        "revenue": ["metric.test.txn_revenue"],
        "alt_revenue": ["metric.test.alt_txn_revenue"],
    }
    ssf = SchemaSourceFile.from_dict(schema_source_file)
    assert ssf
    ssf.fix_metrics_from_measures()
    assert ssf.generated_metrics == []
    assert ssf.metrics_from_measures == expected_metrics_from_measures
