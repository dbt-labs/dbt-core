from nose.plugins.attrib import attr
from test.integration.base import DBTIntegrationTest
import mock
import hashlib

import dbt.version
import dbt.tracking
import dbt.utils

import event_deps
import event_seed
import event_compile
import event_run_models
import event_run_model_error
import event_test

import event_model_compilation_error

class TestEventTracking(DBTIntegrationTest):
    @property
    def profile_config(self):
        return {
            'config': {
                'send_anonymous_usage_stats': True
            }
        }

    @property
    def schema(self):
        return "event_tracking_033"

    @staticmethod
    def dir(path):
        return "test/integration/033_event_tracking_test/" + path.lstrip("/")

    @property
    def models(self):
        return self.dir("models")

    # TODO : Handle the subject. Should be the same every time!
    # TODO : Regex match a uuid for user_id, invocation_id?

    @mock.patch('dbt.tracking.tracker.track_struct_event')
    def run_event_test(self, cmd, event_schema, track_fn, expect_pass=True):
        track_fn.reset_mock()
        project_id = hashlib.md5(self.project['name'].encode('utf-8')).hexdigest()
        version = str(dbt.version.get_installed_version())

        self.run_dbt(cmd, expect_pass=expect_pass)
        user_id = dbt.tracking.active_user.id
        invocation_id = dbt.tracking.active_user.invocation_id

        self.assertTrue(len(user_id) > 0)
        self.assertTrue(len(invocation_id) > 0)

        expected = event_schema.expected(
            user_id=user_id,
            invocation_id=invocation_id,
            project_id=project_id,
            version=version
        )

        # Parse event data out of sp objects
        actual = []
        for event in track_fn.call_args_list:
            posargs, kwargs = event

            context = [c.__dict__ for c in kwargs['context']]
            actual.append([
                posargs,
                dbt.utils.merge(kwargs, {"context": context})
            ])

        self.assertEqual(
            event_schema.transform(actual),
            event_schema.transform(expected)
        )

class TestEventTrackingSuccess(TestEventTracking):
    @property
    def project_config(self):
        return {
            "data-paths": [self.dir("data")],
            "test-paths": [self.dir("test")],
            "repositories": [
                'https://github.com/fishtown-analytics/dbt-integration-project'
            ]
        }

    @attr(type="postgres")
    def test__event_tracking_deps(self):
        cmd = ["deps"]
        self.run_event_test(cmd, event_deps)

    @attr(type="postgres")
    def test__event_tracking_seed(self):
        cmd = ["seed"]
        self.run_event_test(cmd, event_seed)

    @attr(type="postgres")
    def test__event_tracking_compile(self):
        cmd = ["compile"]
        self.run_event_test(cmd, event_compile)

    @attr(type="postgres")
    def test__event_tracking_models(self):
        cmd = ["run", "--model", "example", "example_2"]
        self.run_event_test(cmd, event_run_models)

    @attr(type="postgres")
    def test__event_tracking_model_error(self):
        cmd = ["run", "--model", "model_error"]
        self.run_event_test(cmd, event_run_model_error, expect_pass=False)

    @attr(type="postgres")
    def test__event_tracking_model_error(self):
        self.run_dbt(['run', '--model', 'example', 'example_2'])

        cmd = ["test"]
        self.run_event_test(cmd, event_test, expect_pass=False)

class TestEventTrackingCompilationError(TestEventTracking):
    @property
    def project_config(self):
        return {
            "model-paths": [self.dir("model-compilation-error")],
        }

    @attr(type="postgres")
    def test__event_tracking_with_compilation_error(self):
        cmd = ["compile"]
        self.run_event_test(cmd, event_model_compilation_error, expect_pass=False)
