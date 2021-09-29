from test.integration.base import DBTIntegrationTest,  use_profile
import io
import json
import os

import dbt.exceptions
from dbt.version import __version__ as dbt_version
from dbt.logger import log_manager


class TestDefaultQueryComments(DBTIntegrationTest):
    def matches_comment(self, msg) -> bool:
        if not msg.startswith('/* '):
            return False
        # our blob is the first line of the query comments, minus the comment
        json_str = msg.split('\n')[0][3:-3]
        data = json.loads(json_str)
        return (
            data['app'] == 'dbt' and
            data['dbt_version'] == dbt_version and
            data['node_id'] == 'model.test.x'
        )

    @property
    def project_config(self):
        return {
            'config-version': 2,
            'macro-paths': ['macros']
        }

    @property
    def schema(self):
        return 'dbt_query_comments_051'

    @staticmethod
    def dir(value):
        return os.path.normpath(value)

    @property
    def models(self):
        return self.dir('models')

    def setUp(self):
        super().setUp()
        self.initial_stdout = log_manager.stdout
        self.initial_stderr = log_manager.stderr
        self.stringbuf = io.StringIO()
        log_manager.set_output_stream(self.stringbuf)

    def tearDown(self):
        log_manager.set_output_stream(self.initial_stdout, self.initial_stderr)
        super().tearDown()

    def run_get_json(self, expect_pass=True):
        self.run_dbt(
            ['--debug', '--log-format=json', 'run'],
            expect_pass=expect_pass
        )
        logs = []
        for line in self.stringbuf.getvalue().split('\n'):
            try:
                log = json.loads(line)
            except ValueError:
                continue

            if log['extra'].get('run_state') != 'running':
                continue
            logs.append(log)
        self.assertGreater(len(logs), 0)
        return logs

    def query_comment(self, model_name, log):
        prefix = 'On {}: '.format(model_name)

        if log['message'].startswith(prefix):
            msg = log['message'][len(prefix):]
            if msg in {'COMMIT', 'BEGIN', 'ROLLBACK'}:
                return None
            return msg
        return None

    def run_assert_comments(self):
        logs = self.run_get_json()

        seen = False
        for log in logs:
            msg = self.query_comment('model.test.x', log)
            if msg is not None and self.matches_comment(msg):
                seen = True

        self.assertTrue(seen, 'Never saw a matching log message! Logs:\n{}'.format('\n'.join(l['message'] for l in logs)))

    @use_profile('postgres')
    def test_postgres_comments(self):
        self.run_assert_comments()


class TestQueryComments(TestDefaultQueryComments):
    @property
    def project_config(self):
        cfg = super().project_config
        cfg.update({'query-comment': 'dbt\nrules!\n'})
        return cfg

    def matches_comment(self, msg) -> bool:
        return msg.startswith('/* dbt\nrules! */\n')


class TestMacroQueryComments(TestDefaultQueryComments):
    @property
    def project_config(self):
        cfg = super().project_config
        cfg.update({'query-comment': '{{ query_header_no_args() }}'})
        return cfg

    def matches_comment(self, msg) -> bool:
        start_with = '/* dbt macros\nare pretty cool */\n'
        return msg.startswith(start_with)


class TestMacroArgsQueryComments(TestDefaultQueryComments):
    @property
    def project_config(self):
        cfg = super().project_config
        cfg.update(
            {'query-comment': '{{ return(ordered_to_json(query_header_args(target.name))) }}'}
        )
        return cfg

    def matches_comment(self, msg) -> bool:
        expected_dct = {'app': 'dbt++', 'dbt_version': dbt_version, 'macro_version': '0.1.0', 'message': 'blah: default2'}
        expected = '/* {} */\n'.format(json.dumps(expected_dct, sort_keys=True))
        return msg.startswith(expected)


class TestMacroInvalidQueryComments(TestDefaultQueryComments):
    @property
    def project_config(self):
        cfg = super().project_config
        cfg.update({'query-comment': '{{ invalid_query_header() }}'})
        return cfg

    def run_assert_comments(self):
        with self.assertRaises(dbt.exceptions.RuntimeException):
            self.run_get_json(expect_pass=False)


class TestNullQueryComments(TestDefaultQueryComments):
    @property
    def project_config(self):
        cfg = super().project_config
        cfg.update({'query-comment': ''})
        return cfg

    def matches_comment(self, msg) -> bool:
        return not ('/*' in msg or '*/' in msg)


class TestEmptyQueryComments(TestDefaultQueryComments):
    @property
    def project_config(self):
        cfg = super().project_config
        cfg.update({'query-comment': ''})
        return cfg

    def matches_comment(self, msg) -> bool:
        return not ('/*' in msg or '*/' in msg)
