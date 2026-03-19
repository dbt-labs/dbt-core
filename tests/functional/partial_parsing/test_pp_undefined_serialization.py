"""
Functional tests for handling jinja2.Undefined objects during manifest msgpack
serialization (write_manifest_for_partial_parse).

Reproduces: TypeError: can not serialize 'Undefined' object
The fix adds isinstance(obj, jinja2.Undefined) -> None handling to
extended_msgpack_encoder in core/dbt/parser/manifest.py.
"""

import pytest

from dbt.tests.util import get_manifest, run_dbt, write_file

# A model whose meta references a Jinja variable that is not in the schema
# rendering context.  The SchemaYamlRenderer renders these values with
# native=True, so the result is a raw jinja2.Undefined object stored in the
# node's meta dict rather than a string.
model_with_undefined_meta_sql = """
select 1 as id
"""

# The value "{{ undefined_jinja_var }}" is NOT in the schema YAML rendering
# context, so it evaluates to jinja2.Undefined and ends up stored in the
# manifest node's meta dict.
schema_with_undefined_meta_yml = """
version: 2

models:
  - name: model_with_undefined_meta
    meta:
      key: "{{ undefined_jinja_var }}"
"""


class TestUndefinedMetaSerializationInPartialParse:
    """
    When a schema.yml meta value resolves to jinja2.Undefined during parse
    time, write_manifest_for_partial_parse must not raise TypeError.
    Without the fix (the isinstance(obj, jinja2.Undefined) branch in
    extended_msgpack_encoder), this test fails with:
        TypeError: can not serialize 'Undefined' object
    """

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "model_with_undefined_meta.sql": model_with_undefined_meta_sql,
            "schema.yml": schema_with_undefined_meta_yml,
        }

    def test_parse_with_undefined_meta_does_not_raise(self, project):
        # First parse - also triggers write_manifest_for_partial_parse which
        # is where the TypeError would previously be raised.
        run_dbt(["parse"])
        manifest = get_manifest(project.project_root)
        assert manifest is not None

        # The manifest was written successfully; the model node exists.
        assert "model.test.model_with_undefined_meta" in manifest.nodes

        # The meta value that was jinja2.Undefined should have been serialized
        # as None (the fix converts Undefined -> None before msgpack packing).
        node = manifest.nodes["model.test.model_with_undefined_meta"]
        assert node.meta.get("key") is None

    def test_partial_parse_with_undefined_meta_does_not_raise(self, project):
        # Write a trivial change to a different file to trigger the partial
        # parse path, which re-invokes write_manifest_for_partial_parse.
        write_file(
            model_with_undefined_meta_sql + "\n-- trigger partial reparse\n",
            project.project_root,
            "models",
            "model_with_undefined_meta.sql",
        )
        run_dbt(["--partial-parse", "parse"])
        manifest = get_manifest(project.project_root)
        assert manifest is not None
        assert "model.test.model_with_undefined_meta" in manifest.nodes
