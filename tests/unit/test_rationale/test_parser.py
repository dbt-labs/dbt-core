"""Tests for rationale YAML parser."""

import os
from pathlib import Path

import pytest

from dbt_rationale.parser import (
    ParseResult,
    RationaleEntry,
    find_yaml_files,
    parse_project,
    parse_yaml_file,
)

FIXTURES_DIR = os.path.join(os.path.dirname(__file__), "fixtures")


class TestFindYamlFiles:
    def test_finds_yaml_files(self):
        files = find_yaml_files(FIXTURES_DIR)
        assert len(files) > 0
        assert all(f.suffix in (".yml", ".yaml") for f in files)

    def test_excludes_dbt_project_yml(self, tmp_path):
        (tmp_path / "dbt_project.yml").write_text("name: test")
        (tmp_path / "models.yml").write_text("models: []")
        files = find_yaml_files(str(tmp_path))
        names = [f.name for f in files]
        assert "dbt_project.yml" not in names
        assert "models.yml" in names

    def test_excludes_target_dir(self, tmp_path):
        target = tmp_path / "target"
        target.mkdir()
        (target / "manifest.yml").write_text("data: 1")
        (tmp_path / "schema.yml").write_text("models: []")
        files = find_yaml_files(str(tmp_path))
        filenames = [f.name for f in files]
        assert "manifest.yml" not in filenames
        assert "schema.yml" in filenames


class TestParseYamlFile:
    def test_valid_full(self):
        entries, uncovered, error = parse_yaml_file(Path(FIXTURES_DIR) / "valid_full.yml")
        assert error is None
        assert len(entries) == 3  # 2 models + 1 metric
        names = {e.resource_name for e in entries}
        assert "fct_revenue" in names
        assert "dim_customers" in names
        assert "monthly_revenue" in names

    def test_valid_minimal(self):
        entries, uncovered, error = parse_yaml_file(Path(FIXTURES_DIR) / "valid_minimal.yml")
        assert error is None
        assert len(entries) == 1
        assert entries[0].resource_name == "stg_orders"
        assert entries[0].rationale["intent"] == "Staging layer for raw orders"

    def test_no_rationale(self):
        entries, uncovered, error = parse_yaml_file(Path(FIXTURES_DIR) / "no_rationale.yml")
        assert error is None
        assert len(entries) == 0
        assert len(uncovered) == 2

    def test_mixed_coverage(self):
        entries, uncovered, error = parse_yaml_file(Path(FIXTURES_DIR) / "mixed.yml")
        assert error is None
        # covered_model + raw_payments source + charges table = 3
        assert len(entries) == 3
        # uncovered_model + refunds table = 2
        assert len(uncovered) == 2

    def test_sources_nested(self):
        entries, uncovered, error = parse_yaml_file(Path(FIXTURES_DIR) / "sources_nested.yml")
        assert error is None
        # source (stripe) + 2 covered tables = 3
        assert len(entries) == 3
        # 1 uncovered table (disputes) = 1
        assert len(uncovered) == 1
        assert uncovered[0]["resource_name"] == "disputes"

    def test_invalid_yaml(self, tmp_path):
        bad_file = tmp_path / "bad.yml"
        bad_file.write_text("invalid: yaml: [broken")
        entries, uncovered, error = parse_yaml_file(bad_file)
        assert error is not None
        assert "bad.yml" in error["file"]


class TestParseProject:
    def test_parse_fixtures_directory(self):
        result = parse_project(FIXTURES_DIR)
        assert isinstance(result, ParseResult)
        assert result.total_resources > 0
        assert len(result.entries) > 0

    def test_empty_directory(self, tmp_path):
        result = parse_project(str(tmp_path))
        assert result.total_resources == 0
        assert len(result.entries) == 0
        assert len(result.uncovered) == 0

    def test_rationale_entry_has_file_path(self):
        result = parse_project(FIXTURES_DIR)
        for entry in result.entries:
            assert entry.file_path != ""
            assert entry.resource_type != ""
            assert entry.resource_name != ""
