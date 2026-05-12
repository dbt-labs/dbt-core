"""
Functional tests for partial_parse.msgpack deserialization when `meta`
contains integer keys.

Reproduces: https://github.com/dbt-labs/dbt-core/issues/12578

Root cause
----------
PyYAML's SafeLoader resolves bare integer keys (e.g. ``0: foo``) in YAML
maps to Python ``int`` objects.  When dbt writes the manifest to
``partial_parse.msgpack`` via msgpack.packb, integer keys are accepted
silently.  However on the subsequent run, ``msgpack.unpackb`` was called
with ``strict_map_key=True`` (the library default), which raises::

    ValueError: int is not allowed for map key when strict_map_key=True

This caused dbt to discard the cache and fall back to a full re-parse on
every run — silently, because the failure event was fired at DebugLevel.

The fix
-------
* ``core/dbt/parser/manifest.py`` — pass ``strict_map_key=False`` to
  ``msgpack.unpackb`` so that the decoder accepts integer keys, mirroring
  what the packer already allows.
* ``core/dbt/events/types.py`` — promote ``ParsedFileLoadFailed`` from
  ``DebugLevel`` to ``WarnLevel`` so users are notified when the cache
  cannot be loaded.
"""

import pytest

from dbt.tests.util import get_manifest, run_dbt, write_file

# --------------------------------------------------------------------------- #
# Fixtures                                                                     #
# --------------------------------------------------------------------------- #

_MODEL_SQL = "select 1 as id"

# PyYAML SafeLoader parses bare integers as Python int objects, so keys
# ``0`` and ``1`` below are stored in the manifest node's meta dict as
# int(0) and int(1) — exactly the values that triggered the bug.
_SCHEMA_WITH_INT_META_KEYS_YML = """
version: 2

models:
  - name: my_model
    description: "model with integer keys in meta"
    meta:
      0: first
      1: second
      label: also_a_string_key
"""

_SCHEMA_UPDATED_YML = """
version: 2

models:
  - name: my_model
    description: "updated description to trigger partial re-parse"
    meta:
      0: first
      1: second
      label: also_a_string_key
"""


# --------------------------------------------------------------------------- #
# Tests                                                                        #
# --------------------------------------------------------------------------- #


class TestPartialParseIntegerMetaKeys:
    """
    Verifies that partial_parse.msgpack round-trips correctly when a node's
    ``meta`` dict contains integer keys.

    Without the fix, the second ``dbt parse`` call raises
    ``ValueError: int is not allowed for map key when strict_map_key=True``
    internally and falls back to a full re-parse (or raises, depending on
    dbt version).  With the fix, the cache is loaded successfully on every
    subsequent run.
    """

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "my_model.sql": _MODEL_SQL,
            "schema.yml": _SCHEMA_WITH_INT_META_KEYS_YML,
        }

    def test_first_parse_writes_msgpack_with_int_keys(self, project):
        """Initial parse should succeed and write partial_parse.msgpack."""
        run_dbt(["parse"])
        manifest = get_manifest(project.project_root)
        assert manifest is not None
        assert "model.test.my_model" in manifest.nodes
        node = manifest.nodes["model.test.my_model"]
        # Confirm integer keys survived the parse round-trip
        assert node.meta.get(0) == "first"
        assert node.meta.get(1) == "second"
        assert node.meta.get("label") == "also_a_string_key"

    def test_second_parse_loads_msgpack_without_error(self, project):
        """
        A second parse must load the cached msgpack without raising
        ``ValueError: int is not allowed for map key``.

        Before the fix, dbt would silently discard the cache (or raise) and
        perform a full re-parse.  With the fix, the cache is reused.
        """
        run_dbt(["parse"])
        manifest = get_manifest(project.project_root)
        assert manifest is not None
        node = manifest.nodes["model.test.my_model"]
        assert node.meta.get(0) == "first"
        assert node.meta.get(1) == "second"

    def test_partial_reparse_after_schema_change_preserves_int_keys(self, project):
        """
        Editing the schema YAML (triggers a partial re-parse) must also
        round-trip integer meta keys correctly.
        """
        write_file(
            _SCHEMA_UPDATED_YML,
            project.project_root,
            "models",
            "schema.yml",
        )
        run_dbt(["--partial-parse", "parse"])
        manifest = get_manifest(project.project_root)
        assert manifest is not None
        node = manifest.nodes["model.test.my_model"]
        assert node.meta.get(0) == "first"
        assert node.meta.get(1) == "second"
        assert node.description == "updated description to trigger partial re-parse"
