"""Tests for rationale CLI."""

import json
import os

import pytest

from dbt_rationale.cli import _emit_github_annotations, _render_text, main
from dbt_rationale.config import Config, EnforcementLevel

FIXTURES_DIR = os.path.join(os.path.dirname(__file__), "fixtures")


class TestCLI:
    def test_text_output(self, capsys):
        exit_code = main([FIXTURES_DIR])
        captured = capsys.readouterr()
        assert "Rationale Analysis Report" in captured.out
        assert "Coverage:" in captured.out
        assert "Score:" in captured.out
        assert exit_code == 0

    def test_json_output(self, capsys):
        exit_code = main([FIXTURES_DIR, "--format", "json"])
        captured = capsys.readouterr()
        data = json.loads(captured.out)
        assert "aggregate_score" in data
        assert "coverage" in data
        assert "validation" in data
        assert exit_code == 0

    def test_invalid_path(self, capsys):
        exit_code = main(["/nonexistent/path"])
        assert exit_code == 2

    def test_enforce_off(self, capsys):
        exit_code = main([FIXTURES_DIR, "--enforce", "off"])
        assert exit_code == 0

    def test_enforce_soft_always_exits_0(self, capsys):
        exit_code = main([FIXTURES_DIR, "--enforce", "soft"])
        assert exit_code == 0

    def test_enforce_hard_with_high_score_threshold(self, capsys):
        # Fixtures have errors, so hard enforcement with high threshold should fail
        exit_code = main([FIXTURES_DIR, "--enforce", "hard", "--min-score", "100"])
        assert exit_code == 1

    def test_enforce_hard_with_low_threshold_passes(self, tmp_path):
        """A project with valid rationale and no thresholds passes hard mode."""
        models_dir = tmp_path / "models"
        models_dir.mkdir()
        (models_dir / "schema.yml").write_text("""
models:
  - name: good_model
    meta:
      rationale:
        intent: "Well documented"
        owner: "team@co.com"
""")
        exit_code = main([str(tmp_path), "--enforce", "hard"])
        assert exit_code == 0

    def test_enforce_hard_fails_on_validation_errors(self, tmp_path):
        """Hard enforcement fails when there are validation errors."""
        models_dir = tmp_path / "models"
        models_dir.mkdir()
        (models_dir / "schema.yml").write_text("""
models:
  - name: bad_model
    meta:
      rationale:
        domain: "Missing required fields"
""")
        exit_code = main([str(tmp_path), "--enforce", "hard"])
        assert exit_code == 1

    def test_min_coverage_enforcement(self, tmp_path):
        models_dir = tmp_path / "models"
        models_dir.mkdir()
        (models_dir / "schema.yml").write_text("""
models:
  - name: covered
    meta:
      rationale:
        intent: "Has rationale"
        owner: "team@co.com"
  - name: uncovered
    description: "No rationale"
""")
        # 50% coverage, requiring 80%
        exit_code = main([str(tmp_path), "--enforce", "hard", "--min-coverage", "80"])
        assert exit_code == 1

        # 50% coverage, requiring 50%
        exit_code = main([str(tmp_path), "--enforce", "hard", "--min-coverage", "50"])
        assert exit_code == 0

    def test_empty_project_passes(self, tmp_path):
        exit_code = main([str(tmp_path)])
        assert exit_code == 0

    def test_exception_warn_days_flag(self, tmp_path):
        models_dir = tmp_path / "models"
        models_dir.mkdir()
        (models_dir / "schema.yml").write_text("""
models:
  - name: m1
    meta:
      rationale:
        intent: "Intent"
        owner: "owner"
""")
        exit_code = main([str(tmp_path), "--exception-warn-days", "7"])
        assert exit_code == 0

    def test_text_output_enforcement_off(self, capsys, tmp_path):
        models_dir = tmp_path / "models"
        models_dir.mkdir()
        (models_dir / "schema.yml").write_text("""
models:
  - name: m1
    meta:
      rationale:
        intent: "Intent"
        owner: "owner"
""")
        exit_code = main([str(tmp_path), "--enforce", "off"])
        captured = capsys.readouterr()
        assert "OFF" in captured.out
        assert exit_code == 0

    def test_text_output_enforcement_passed(self, capsys, tmp_path):
        models_dir = tmp_path / "models"
        models_dir.mkdir()
        (models_dir / "schema.yml").write_text("""
models:
  - name: m1
    meta:
      rationale:
        intent: "Intent"
        owner: "owner"
""")
        exit_code = main([str(tmp_path), "--enforce", "hard"])
        captured = capsys.readouterr()
        assert "PASSED" in captured.out
        assert exit_code == 0

    def test_text_output_with_parse_errors(self, capsys, tmp_path):
        models_dir = tmp_path / "models"
        models_dir.mkdir()
        (models_dir / "bad.yml").write_text("invalid: yaml: [broken")
        exit_code = main([str(tmp_path)])
        captured = capsys.readouterr()
        assert "Parse Errors" in captured.out

    def test_text_output_enforcement_failed_reasons(self, capsys, tmp_path):
        models_dir = tmp_path / "models"
        models_dir.mkdir()
        (models_dir / "schema.yml").write_text("""
models:
  - name: bad
    meta:
      rationale:
        domain: "Missing required"
""")
        exit_code = main([str(tmp_path), "--enforce", "hard", "--min-score", "80", "--min-coverage", "100"])
        captured = capsys.readouterr()
        assert "FAILED" in captured.out
        assert "validation error" in captured.out
        assert exit_code == 1

    def test_github_annotations(self, capsys, tmp_path, monkeypatch):
        monkeypatch.setenv("GITHUB_ACTIONS", "true")
        models_dir = tmp_path / "models"
        models_dir.mkdir()
        (models_dir / "schema.yml").write_text("""
models:
  - name: bad
    meta:
      rationale:
        domain: "Missing required"
        exceptions:
          - rule: "POLICY"
            justification: "reason"
            approved_by: "someone"
            expires: "2025-01-01"
""")
        exit_code = main([str(tmp_path)])
        captured = capsys.readouterr()
        assert "::error" in captured.err

    def test_github_annotations_with_expiring(self, capsys, tmp_path, monkeypatch):
        monkeypatch.setenv("GITHUB_ACTIONS", "true")
        models_dir = tmp_path / "models"
        models_dir.mkdir()
        (models_dir / "schema.yml").write_text("""
models:
  - name: m1
    meta:
      rationale:
        intent: "Intent"
        owner: "owner"
        exceptions:
          - rule: "POLICY"
            justification: "reason"
            approved_by: "someone"
            expires: "2026-03-01"
""")
        exit_code = main([str(tmp_path)])
        captured = capsys.readouterr()
        assert "::warning" in captured.err
        assert "expires in" in captured.err


class TestConfigFile:
    def test_reads_config_from_project(self, tmp_path):
        (tmp_path / ".rationale.yml").write_text("""
enforcement: hard
min_score: 90
min_coverage: 80
exception_warn_days: 14
""")
        models_dir = tmp_path / "models"
        models_dir.mkdir()
        (models_dir / "schema.yml").write_text("""
models:
  - name: model1
    meta:
      rationale:
        intent: "Intent"
        owner: "owner"
""")
        # Score will be 60, min_score is 90 -> fail
        exit_code = main([str(tmp_path)])
        assert exit_code == 1

    def test_cli_overrides_config(self, tmp_path):
        (tmp_path / ".rationale.yml").write_text("""
enforcement: hard
min_score: 90
""")
        models_dir = tmp_path / "models"
        models_dir.mkdir()
        (models_dir / "schema.yml").write_text("""
models:
  - name: model1
    meta:
      rationale:
        intent: "Intent"
        owner: "owner"
""")
        # Config says hard with min_score 90, but CLI overrides to off
        exit_code = main([str(tmp_path), "--enforce", "off"])
        assert exit_code == 0

    def test_invalid_yaml_config(self, tmp_path):
        (tmp_path / ".rationale.yml").write_text("invalid: yaml: [broken")
        exit_code = main([str(tmp_path)])
        assert exit_code == 0  # Falls back to defaults

    def test_non_dict_config(self, tmp_path):
        (tmp_path / ".rationale.yml").write_text("just a string")
        exit_code = main([str(tmp_path)])
        assert exit_code == 0  # Falls back to defaults

    def test_invalid_enforcement_value_in_config(self, tmp_path):
        (tmp_path / ".rationale.yml").write_text("""
enforcement: invalid_value
""")
        exit_code = main([str(tmp_path)])
        assert exit_code == 0  # Falls back to soft

    def test_config_with_resource_types_and_exclude(self, tmp_path):
        (tmp_path / ".rationale.yml").write_text("""
enforcement: soft
resource_types:
  - models
  - metrics
exclude_paths:
  - staging/
""")
        exit_code = main([str(tmp_path)])
        assert exit_code == 0
