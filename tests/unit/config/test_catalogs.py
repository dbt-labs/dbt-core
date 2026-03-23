from dbt.config.catalogs import load_catalogs
from dbt.tests.util import write_config_file

write_integration_1 = {
    "name": "write_integration_1",
    "external_volume": "write_external_volume",
    "table_format": "write_format",
    "catalog_type": "write",
    "adapter_properties": {"my_custom_property": "foo_1"},
}

write_integration_2 = {
    "name": "write_integration_2",
    "external_volume": "write_external_volume",
    "table_format": "write_format",
    "catalog_type": "write",
    "adapter_properties": {"my_custom_property": "foo_2"},
}


def test_load_catalogs_from_schema_yml(tmp_path):
    (tmp_path / "models").mkdir()
    write_config_file(
        {
            "version": 2,
            "catalogs": [{"name": "schema_catalog", "write_integrations": [write_integration_1]}],
        },
        tmp_path,
        "models",
        "schema.yml",
    )

    catalogs = load_catalogs(str(tmp_path), "test", ["models"], {})

    assert [catalog.name for catalog in catalogs] == ["schema_catalog"]


def test_load_catalogs_combines_root_and_schema_yml_files(tmp_path):
    (tmp_path / "models").mkdir()
    write_config_file(
        {"catalogs": [{"name": "root_catalog", "write_integrations": [write_integration_1]}]},
        tmp_path,
        "catalogs.yml",
    )
    write_config_file(
        {
            "version": 2,
            "catalogs": [{"name": "schema_catalog", "write_integrations": [write_integration_2]}],
        },
        tmp_path,
        "models",
        "schema.yml",
    )

    catalogs = load_catalogs(str(tmp_path), "test", ["models"], {})

    assert [catalog.name for catalog in catalogs] == ["root_catalog", "schema_catalog"]
