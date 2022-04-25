import pytest

from dbt.exceptions import CompilationException, ParsingException

from dbt.tests.util import (
    run_dbt,
)

# from `test/integration/011_invalid_model_tests`, invalid_model_tests

#
# Seeds
#

seeds__base_seed = """
first_name,last_name,email,gender,ip_address
Jack,Hunter,jhunter0@pbs.org,Male,59.80.20.168
Kathryn,Walker,kwalker1@ezinearticles.com,Female,194.121.179.35
Gerald,Ryan,gryan2@com.com,Male,11.3.212.243
Bonnie,Spencer,bspencer3@ameblo.jp,Female,216.32.196.175
Harold,Taylor,htaylor4@people.com.cn,Male,253.10.246.136
Jacqueline,Griffin,jgriffin5@t.co,Female,16.13.192.220
Wanda,Arnold,warnold6@google.nl,Female,232.116.150.64
Craig,Ortiz,cortiz7@sciencedaily.com,Male,199.126.106.13
Gary,Day,gday8@nih.gov,Male,35.81.68.186
Rose,Wright,rwright9@yahoo.co.jp,Female,236.82.178.100
"""

#
# Properties
#

properties__seed_types_yml = """
version: 2
seeds:
  - name: seeds__base_seed
    config:
      +column_types:
        first_name: varchar(50),
        last_name:  varchar(50),
        email:      varchar(50),
        gender:     varchar(50),
        ip_address: varchar(20)

"""

#
# Models
#

models__view_bad_enabled_value = """
{{
  config(
    enabled = 'false'
  )
}}

select * from {{ this.schema }}.seed
"""

models__view_disabled = """
{{
  config(
    enabled = False
  )
}}

select * from {{ this.schema }}.seed
"""

models__dependent_on_view = """
select * from {{ ref('models__view_disabled') }}
"""

#
# Tests
#


class InvalidModelBase(object):
    @pytest.fixture(scope="class")
    def seeds(self):
        return {
            "seeds__base_seed.csv": seeds__base_seed,
        }

    @pytest.fixture(scope="class")
    def properties(self):
        return {
            "properties__seed_types.yml": properties__seed_types_yml,
        }


class TestMalformedEnabledParam(InvalidModelBase):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "models__view_bad_enabled_value.sql": models__view_bad_enabled_value,
        }

    def test_view_disabled(self, project):
        with pytest.raises(ParsingException) as exc:
            run_dbt(["seed"])

        assert "enabled" in str(exc.value)


class TestReferencingDisabledModel(InvalidModelBase):
    """Expects that the upstream model is disabled"""

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "models__view_disabled.sql": models__view_disabled,
            "models__dependent_on_view.sql": models__dependent_on_view,
        }

    def test_referencing_disabled_model(self, project):
        with pytest.raises(CompilationException) as exc:
            run_dbt()

        assert "which is disabled" in str(exc.value)


class TestMissingModelReference(InvalidModelBase):
    """Expects that the upstream model is not found"""

    @pytest.fixture(scope="class")
    def models(self):
        return {"models__dependent_on_view.sql": models__dependent_on_view}

    def test_models_not_found(self, project):
        with pytest.raises(CompilationException) as exc:
            run_dbt()

        assert "which was not found" in str(exc.value)


# class TestInvalidMacroCall(DBTIntegrationTest):
#     @property
#     def schema(self):
#         return "invalid_models_011"

#     @property
#     def models(self):
#         return "models-invalid-macro"

#     @staticmethod
#     def dir(path):
#         return path.lstrip("/")

#     @property
#     def project_config(self):
#         return {
#             'config-version': 2,
#             'macro-paths': [self.dir('bad-macros')],
#         }

#     @use_profile('postgres')
#     def test_postgres_call_invalid(self):
#         with self.assertRaises(Exception) as exc:
#             self.run_dbt(['compile'])

#         macro_path = os.path.join('bad-macros', 'macros.sql')
#         model_path = os.path.join('models-invalid-macro', 'bad_macro.sql')

#         self.assertIn(f'> in macro some_macro ({macro_path})', str(exc.exception))
#         self.assertIn(f'> called by model bad_macro ({model_path})', str(exc.exception))


# class TestInvalidDisabledSource(DBTIntegrationTest):
#     def setUp(self):
#         super().setUp()
#         self.run_sql_file("seed.sql")

#     @property
#     def schema(self):
#         return "invalid_models_011"

#     @property
#     def models(self):
#         return 'sources-disabled'

#     @property
#     def project_config(self):
#         return {
#             'config-version': 2,
#             'sources': {
#                 'test': {
#                     'enabled': False,
#                 }
#             }
#         }

#     @use_profile('postgres')
#     def test_postgres_source_disabled(self):
#         with self.assertRaises(RuntimeError) as exc:
#             self.run_dbt()

#         self.assertIn('which is disabled', str(exc.exception))


# class TestInvalidMissingSource(DBTIntegrationTest):
#     def setUp(self):
#         super().setUp()
#         self.run_sql_file("seed.sql")

#     @property
#     def schema(self):
#         return "invalid_models_011"

#     @property
#     def models(self):
#         return 'sources-missing'

#     @use_profile('postgres')
#     def test_postgres_source_missing(self):
#         with self.assertRaises(RuntimeError) as exc:
#             self.run_dbt()

#         self.assertIn('which was not found', str(exc.exception))
