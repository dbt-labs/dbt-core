import pytest

from pathlib import Path

from dbt.exceptions import CompilationError, ParsingError

from dbt.tests.util import (
    run_dbt,
    write_file,
)

from tests.functional.hooks.fixtures import (
    models__hooked,
    models__hooks,
    models__hooks_configured,
    models__hooks_error,
    models__hooks_kwargs,
    models__post,
    models__pre,
    properties__seed_models,
    properties__test_snapshot_models,
    seeds__example_seed_csv,
    snapshots__test_snapshot,
)

MODEL_PRE_HOOK = """
   insert into {{this.schema}}.on_model_hook (
        "state",
        "target.dbname",
        "target.host",
        "target.name",
        "target.schema",
        "target.type",
        "target.user",
        "target.pass",
        "target.port",
        "target.threads",
        "run_started_at",
        "invocation_id"
   ) VALUES (
    'start',
    '{{ target.dbname }}',
    '{{ target.host }}',
    '{{ target.name }}',
    '{{ target.schema }}',
    '{{ target.type }}',
    '{{ target.user }}',
    '{{ target.get("pass", "") }}',
    {{ target.port }},
    {{ target.threads }},
    '{{ run_started_at }}',
    '{{ invocation_id }}'
   )
"""

MODEL_POST_HOOK = """
   insert into {{this.schema}}.on_model_hook (
        "state",
        "target.dbname",
        "target.host",
        "target.name",
        "target.schema",
        "target.type",
        "target.user",
        "target.pass",
        "target.port",
        "target.threads",
        "run_started_at",
        "invocation_id"
   ) VALUES (
    'end',
    '{{ target.dbname }}',
    '{{ target.host }}',
    '{{ target.name }}',
    '{{ target.schema }}',
    '{{ target.type }}',
    '{{ target.user }}',
    '{{ target.get("pass", "") }}',
    {{ target.port }},
    {{ target.threads }},
    '{{ run_started_at }}',
    '{{ invocation_id }}'
   )
"""


class BaseTestPrePost(object):
    @pytest.fixture(scope="class", autouse=True)
    def setUp(self, project):
        project.run_sql_file(project.test_data_dir / Path("seed_model.sql"))

    def get_ctx_vars(self, state, count, project):
        fields = [
            "state",
            "target.dbname",
            "target.host",
            "target.name",
            "target.port",
            "target.schema",
            "target.threads",
            "target.type",
            "target.user",
            "target.pass",
            "run_started_at",
            "invocation_id",
        ]
        field_list = ", ".join(['"{}"'.format(f) for f in fields])
        query = "select {field_list} from {schema}.on_model_hook where state = '{state}'".format(
            field_list=field_list, schema=project.test_schema, state=state
        )

        vals = project.run_sql(query, fetch="all")
        assert len(vals) != 0, "nothing inserted into hooks table"
        assert len(vals) >= count, "too few rows in hooks table"
        assert len(vals) <= count, "too many rows in hooks table"
        return [{k: v for k, v in zip(fields, val)} for val in vals]

    def check_hooks(self, state, project, host, count=1):
        ctxs = self.get_ctx_vars(state, count=count, project=project)
        for ctx in ctxs:
            assert ctx["state"] == state
            assert ctx["target.dbname"] == "dbt"
            assert ctx["target.host"] == host
            assert ctx["target.name"] == "default"
            assert ctx["target.port"] == 5432
            assert ctx["target.schema"] == project.test_schema
            assert ctx["target.threads"] == 4
            assert ctx["target.type"] == "postgres"
            assert ctx["target.user"] == "root"
            assert ctx["target.pass"] == ""

            assert (
                ctx["run_started_at"] is not None and len(ctx["run_started_at"]) > 0
            ), "run_started_at was not set"
            assert (
                ctx["invocation_id"] is not None and len(ctx["invocation_id"]) > 0
            ), "invocation_id was not set"


class TestPrePostModelHooks(BaseTestPrePost):
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "models": {
                "test": {
                    "pre-hook": [
                        # inside transaction (runs second)
                        MODEL_PRE_HOOK,
                        # outside transaction (runs first)
                        {"sql": "vacuum {{ this.schema }}.on_model_hook", "transaction": False},
                    ],
                    "post-hook": [
                        # outside transaction (runs second)
                        {"sql": "vacuum {{ this.schema }}.on_model_hook", "transaction": False},
                        # inside transaction (runs first)
                        MODEL_POST_HOOK,
                    ],
                }
            }
        }

    @pytest.fixture(scope="class")
    def models(self):
        return {"hooks.sql": models__hooks}

    def test_pre_and_post_run_hooks(self, project, dbt_profile_target):
        run_dbt()

        self.check_hooks("start", project, dbt_profile_target["host"])
        self.check_hooks("end", project, dbt_profile_target["host"])


class TestPrePostModelHooksUnderscores(TestPrePostModelHooks):
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "models": {
                "test": {
                    "pre_hook": [
                        # inside transaction (runs second)
                        MODEL_PRE_HOOK,
                        # outside transaction (runs first)
                        {"sql": "vacuum {{ this.schema }}.on_model_hook", "transaction": False},
                    ],
                    "post_hook": [
                        # outside transaction (runs second)
                        {"sql": "vacuum {{ this.schema }}.on_model_hook", "transaction": False},
                        # inside transaction (runs first)
                        MODEL_POST_HOOK,
                    ],
                }
            }
        }


class TestHookRefs(BaseTestPrePost):
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "models": {
                "test": {
                    "hooked": {
                        "post-hook": [
                            """
                        insert into {{this.schema}}.on_model_hook select
                        state,
                        '{{ target.dbname }}' as "target.dbname",
                        '{{ target.host }}' as "target.host",
                        '{{ target.name }}' as "target.name",
                        '{{ target.schema }}' as "target.schema",
                        '{{ target.type }}' as "target.type",
                        '{{ target.user }}' as "target.user",
                        '{{ target.get("pass", "") }}' as "target.pass",
                        {{ target.port }} as "target.port",
                        {{ target.threads }} as "target.threads",
                        '{{ run_started_at }}' as "run_started_at",
                        '{{ invocation_id }}' as "invocation_id"
                    from {{ ref('post') }}""".strip()
                        ],
                    }
                },
            }
        }

    @pytest.fixture(scope="class")
    def models(self):
        return {"hooked.sql": models__hooked, "post.sql": models__post, "pre.sql": models__pre}

    def test_pre_post_model_hooks_refed(self, project, dbt_profile_target):
        run_dbt()

        self.check_hooks("start", project, dbt_profile_target["host"], count=1)
        self.check_hooks("end", project, dbt_profile_target["host"], count=1)


class TestPrePostModelHooksOnSeeds(object):
    @pytest.fixture(scope="class")
    def seeds(self):
        return {"example_seed.csv": seeds__example_seed_csv}

    @pytest.fixture(scope="class")
    def models(self):
        return {"schema.yml": properties__seed_models}

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "seed-paths": ["seeds"],
            "models": {},
            "seeds": {
                "post-hook": [
                    "alter table {{ this }} add column new_col int",
                    "update {{ this }} set new_col = 1",
                    # call any macro to track dependency: https://github.com/dbt-labs/dbt-core/issues/6806
                    "select null::{{ dbt.type_int() }} as id",
                ],
                "quote_columns": False,
            },
        }

    def test_hooks_on_seeds(self, project):
        res = run_dbt(["seed"])
        assert len(res) == 1, "Expected exactly one item"
        res = run_dbt(["test"])
        assert len(res) == 1, "Expected exactly one item"


class TestHooksRefsOnSeeds:
    """
    This should not succeed, and raise an explicit error
    https://github.com/dbt-labs/dbt-core/issues/6806
    """

    @pytest.fixture(scope="class")
    def seeds(self):
        return {"example_seed.csv": seeds__example_seed_csv}

    @pytest.fixture(scope="class")
    def models(self):
        return {"schema.yml": properties__seed_models, "post.sql": models__post}

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "seeds": {
                "post-hook": [
                    "select * from {{ ref('post') }}",
                ],
            },
        }

    def test_hook_with_ref_on_seeds(self, project):
        with pytest.raises(ParsingError) as excinfo:
            run_dbt(["parse"])
        assert "Seeds cannot depend on other nodes" in str(excinfo.value)


class TestPrePostModelHooksOnSeedsPlusPrefixed(TestPrePostModelHooksOnSeeds):
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "seed-paths": ["seeds"],
            "models": {},
            "seeds": {
                "+post-hook": [
                    "alter table {{ this }} add column new_col int",
                    "update {{ this }} set new_col = 1",
                ],
                "quote_columns": False,
            },
        }


class TestPrePostModelHooksOnSeedsPlusPrefixedWhitespace(TestPrePostModelHooksOnSeeds):
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "seed-paths": ["seeds"],
            "models": {},
            "seeds": {
                "+post-hook": [
                    "alter table {{ this }} add column new_col int",
                    "update {{ this }} set new_col = 1",
                ],
                "quote_columns": False,
            },
        }


class TestPrePostModelHooksOnSnapshots(object):
    @pytest.fixture(scope="class", autouse=True)
    def setUp(self, project):
        path = Path(project.project_root) / "test-snapshots"
        Path.mkdir(path)
        write_file(snapshots__test_snapshot, path, "snapshot.sql")

    @pytest.fixture(scope="class")
    def models(self):
        return {"schema.yml": properties__test_snapshot_models}

    @pytest.fixture(scope="class")
    def seeds(self):
        return {"example_seed.csv": seeds__example_seed_csv}

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "seed-paths": ["seeds"],
            "snapshot-paths": ["test-snapshots"],
            "models": {},
            "snapshots": {
                "post-hook": [
                    "alter table {{ this }} add column new_col int",
                    "update {{ this }} set new_col = 1",
                ]
            },
            "seeds": {
                "quote_columns": False,
            },
        }

    def test_hooks_on_snapshots(self, project):
        res = run_dbt(["seed"])
        assert len(res) == 1, "Expected exactly one item"
        res = run_dbt(["snapshot"])
        assert len(res) == 1, "Expected exactly one item"
        res = run_dbt(["test"])
        assert len(res) == 1, "Expected exactly one item"


class PrePostModelHooksInConfigSetup(BaseTestPrePost):
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "macro-paths": ["macros"],
        }

    @pytest.fixture(scope="class")
    def models(self):
        return {"hooks.sql": models__hooks_configured}


class TestPrePostModelHooksInConfig(PrePostModelHooksInConfigSetup):
    def test_pre_and_post_model_hooks_model(self, project, dbt_profile_target):
        run_dbt()

        self.check_hooks("start", project, dbt_profile_target["host"])
        self.check_hooks("end", project, dbt_profile_target["host"])


class TestPrePostModelHooksInConfigWithCount(PrePostModelHooksInConfigSetup):
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "models": {
                "test": {
                    "pre-hook": [
                        # inside transaction (runs second)
                        MODEL_PRE_HOOK,
                        # outside transaction (runs first)
                        {"sql": "vacuum {{ this.schema }}.on_model_hook", "transaction": False},
                    ],
                    "post-hook": [
                        # outside transaction (runs second)
                        {"sql": "vacuum {{ this.schema }}.on_model_hook", "transaction": False},
                        # inside transaction (runs first)
                        MODEL_POST_HOOK,
                    ],
                }
            }
        }

    def test_pre_and_post_model_hooks_model_and_project(self, project, dbt_profile_target):
        run_dbt()

        self.check_hooks("start", project, dbt_profile_target["host"], count=2)
        self.check_hooks("end", project, dbt_profile_target["host"], count=2)


class TestPrePostModelHooksInConfigKwargs(TestPrePostModelHooksInConfig):
    @pytest.fixture(scope="class")
    def models(self):
        return {"hooks.sql": models__hooks_kwargs}


class TestPrePostSnapshotHooksInConfigKwargs(TestPrePostModelHooksOnSnapshots):
    @pytest.fixture(scope="class", autouse=True)
    def setUp(self, project):
        path = Path(project.project_root) / "test-kwargs-snapshots"
        Path.mkdir(path)
        write_file(snapshots__test_snapshot, path, "snapshot.sql")

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "seed-paths": ["seeds"],
            "snapshot-paths": ["test-kwargs-snapshots"],
            "models": {},
            "snapshots": {
                "post-hook": [
                    "alter table {{ this }} add column new_col int",
                    "update {{ this }} set new_col = 1",
                ]
            },
            "seeds": {
                "quote_columns": False,
            },
        }


class TestDuplicateHooksInConfigs(object):
    @pytest.fixture(scope="class")
    def models(self):
        return {"hooks.sql": models__hooks_error}

    def test_run_duplicate_hook_defs(self, project):
        with pytest.raises(CompilationError) as exc:
            run_dbt()
        assert "pre_hook" in str(exc.value)
        assert "pre-hook" in str(exc.value)
