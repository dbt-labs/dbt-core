import json
import os
import tempfile
from unittest.mock import MagicMock, patch

import pytest

from dbt.artifacts.schemas.base import Writable


class SimpleWritable(Writable):
    """Minimal concrete Writable for testing."""

    def to_dict(self, omit_none=False, context=None):
        return {"key": "value", "nested": {"a": 1}}


class TestWritable:
    @pytest.fixture
    def tmp_path(self):
        with tempfile.TemporaryDirectory() as d:
            yield d

    def test_write_compact_by_default(self, tmp_path):
        """Without --write-json-indent the file must NOT contain indentation."""
        mock_flags = MagicMock()
        mock_flags.WRITE_JSON_INDENT = False

        obj = SimpleWritable()
        dest = os.path.join(tmp_path, "manifest.json")

        # patch at the source module since the import is lazy
        with patch("dbt.flags.get_flags", return_value=mock_flags):
            obj.write(dest)

        with open(dest) as f:
            raw = f.read()

        # Compact JSON has no leading spaces on internal lines
        assert "\n  " not in raw

    def test_write_indented_when_flag_enabled(self, tmp_path):
        """With --write-json-indent the output must be pretty-printed."""
        mock_flags = MagicMock()
        mock_flags.WRITE_JSON_INDENT = True

        obj = SimpleWritable()
        dest = os.path.join(tmp_path, "manifest.json")

        with patch("dbt.flags.get_flags", return_value=mock_flags):
            obj.write(dest)

        with open(dest) as f:
            raw = f.read()

        # Pretty-printed JSON has indentation
        assert "\n  " in raw
        # Should be valid JSON
        data = json.loads(raw)
        assert data == {"key": "value", "nested": {"a": 1}}

    def test_write_indented_creates_parent_dirs(self, tmp_path):
        """write() with indent must create missing parent directories."""
        mock_flags = MagicMock()
        mock_flags.WRITE_JSON_INDENT = True

        obj = SimpleWritable()
        dest = os.path.join(tmp_path, "deep", "nested", "manifest.json")

        with patch("dbt.flags.get_flags", return_value=mock_flags):
            obj.write(dest)

        assert os.path.exists(dest)

    def test_write_falls_back_to_compact_on_flag_error(self, tmp_path):
        """If get_flags() raises, write() must fall back to compact JSON without crashing."""
        obj = SimpleWritable()
        dest = os.path.join(tmp_path, "manifest.json")

        with patch("dbt.flags.get_flags", side_effect=Exception("no flags")):
            with patch("dbt.artifacts.schemas.base.write_json") as mock_write:
                obj.write(dest)
                mock_write.assert_called_once()

    def test_write_indented_uses_json_encoder(self, tmp_path):
        """write() with indent must use JSONEncoder so that Decimal and datetime
        values are serialised correctly, matching the compact write_json() path."""
        import datetime
        import decimal

        class SpecialWritable(Writable):
            def to_dict(self, omit_none=False, context=None):
                return {
                    "price": decimal.Decimal("9.99"),
                    "created_at": datetime.datetime(2024, 1, 1, 12, 0, 0),
                }

        mock_flags = MagicMock()
        mock_flags.WRITE_JSON_INDENT = True

        obj = SpecialWritable()
        dest = os.path.join(tmp_path, "manifest.json")

        with patch("dbt.flags.get_flags", return_value=mock_flags):
            obj.write(dest)

        with open(dest) as f:
            data = json.load(f)

        assert data["price"] == 9.99, "Decimal must be serialized as a float by JSONEncoder"
        assert data["created_at"] == "2024-01-01T12:00:00", "datetime must be ISO-formatted by JSONEncoder"
