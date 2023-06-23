import pytest

from dbt import deprecations
import dbt.exceptions
from dbt.tests.util import run_dbt


models__already_exists_sql = """
select 1 as id

{% if adapter.already_exists(this.schema, this.identifier) and not should_full_refresh() %}
    where id > (select max(id) from {{this}})
{% endif %}
"""

models_trivial__model_sql = """
select 1 as id
"""


bad_name_yaml = """
version: 2

exposures:
  - name: simple exposure spaced!!
    type: dashboard
    depends_on:
      - ref('model')
    owner:
      email: something@example.com
"""


class TestConfigPathDeprecation:
    @pytest.fixture(scope="class")
    def models(self):
        return {"already_exists.sql": models_trivial__model_sql}

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "config-version": 2,
            "data-paths": ["data"],
            "log-path": "customlogs",
            "target-path": "customtarget",
        }

    def test_data_path(self, project):
        deprecations.reset_deprecations()
        assert deprecations.active_deprecations == set()
        run_dbt(["debug"])
        expected = {
            "project-config-data-paths",
            "project-config-log-path",
            "project-config-target-path",
        }
        assert expected == deprecations.active_deprecations

    def test_data_path_fail(self, project):
        deprecations.reset_deprecations()
        assert deprecations.active_deprecations == set()
        with pytest.raises(dbt.exceptions.CompilationError) as exc:
            run_dbt(["--warn-error", "debug"])
        exc_str = " ".join(str(exc.value).split())  # flatten all whitespace
        expected_msg = "The `data-paths` config has been renamed"
        assert expected_msg in exc_str


class TestAdapterDeprecations:
    @pytest.fixture(scope="class")
    def models(self):
        return {"already_exists.sql": models__already_exists_sql}

    def test_adapter(self, project):
        deprecations.reset_deprecations()
        assert deprecations.active_deprecations == set()
        run_dbt(["run"])
        expected = {"adapter:already_exists"}
        assert expected == deprecations.active_deprecations

    def test_adapter_fail(self, project):
        deprecations.reset_deprecations()
        assert deprecations.active_deprecations == set()
        run_dbt(["--warn-error", "run"], expect_pass=False)


class TestPackageInstallPathDeprecation:
    @pytest.fixture(scope="class")
    def models_trivial(self):
        return {"model.sql": models_trivial__model_sql}

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"config-version": 2, "clean-targets": ["dbt_modules"]}

    def test_package_path(self, project):
        deprecations.reset_deprecations()
        assert deprecations.active_deprecations == set()
        run_dbt(["clean"])
        expected = {"install-packages-path"}
        assert expected == deprecations.active_deprecations

    def test_package_path_not_set(self, project):
        deprecations.reset_deprecations()
        assert deprecations.active_deprecations == set()
        with pytest.raises(dbt.exceptions.CompilationError) as exc:
            run_dbt(["--warn-error", "clean"])
        exc_str = " ".join(str(exc.value).split())  # flatten all whitespace
        expected_msg = "path has changed from `dbt_modules` to `dbt_packages`."
        assert expected_msg in exc_str


class TestPackageRedirectDeprecation:
    @pytest.fixture(scope="class")
    def models(self):
        return {"already_exists.sql": models_trivial__model_sql}

    @pytest.fixture(scope="class")
    def packages(self):
        return {"packages": [{"package": "fishtown-analytics/dbt_utils", "version": "0.7.0"}]}

    def test_package_redirect(self, project):
        deprecations.reset_deprecations()
        assert deprecations.active_deprecations == set()
        run_dbt(["deps"])
        expected = {"package-redirect"}
        assert expected == deprecations.active_deprecations

    # if this test comes before test_package_redirect it will raise an exception as expected
    def test_package_redirect_fail(self, project):
        deprecations.reset_deprecations()
        assert deprecations.active_deprecations == set()
        with pytest.raises(dbt.exceptions.CompilationError) as exc:
            run_dbt(["--warn-error", "deps"])
        exc_str = " ".join(str(exc.value).split())  # flatten all whitespace
        expected_msg = "The `fishtown-analytics/dbt_utils` package is deprecated in favor of `dbt-labs/dbt_utils`"
        assert expected_msg in exc_str


class TestExposureNameDeprecation:
    @pytest.fixture(scope="class")
    def models(self):
        return {"model.sql": models_trivial__model_sql, "bad_name.yml": bad_name_yaml}

    def test_exposure_name(self, project):
        deprecations.reset_deprecations()
        assert deprecations.active_deprecations == set()
        run_dbt(["parse"])
        expected = {"exposure-name"}
        assert expected == deprecations.active_deprecations

    def test_exposure_name_fail(self, project):
        deprecations.reset_deprecations()
        assert deprecations.active_deprecations == set()
        with pytest.raises(dbt.exceptions.CompilationError) as exc:
            run_dbt(["--warn-error", "--no-partial-parse", "parse"])
        exc_str = " ".join(str(exc.value).split())  # flatten all whitespace
        expected_msg = "Starting in v1.3, the 'name' of an exposure should contain only letters, numbers, and underscores."
        assert expected_msg in exc_str
