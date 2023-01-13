
import pytest

from dbt.tests.util import run_dbt, check_relations_equal
from tests.functional.ref_overwrite.fixtures import models, macros, seeds, project_files  # noqa: F401


class TestRefOverride():
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            'config-version': 2,
            'seed-paths': ['seeds'],
            "macro-paths": ["macros"],
            'seeds': {
                'quote_columns': False,
            },
        }

    def test_ref_override(self, project, ):
        run_dbt(['seed'])
        run_dbt(['run'])
        # We want it to equal seed_2 and not seed_1. If it's
        # still pointing at seed_1 then the override hasn't worked.
        check_relations_equal(project.adapter, ['ref_override', 'seed_2'])
