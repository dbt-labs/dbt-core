from test.integration.base import DBTIntegrationTest,  use_profile
import io
import json
import os
import re

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
        res, raw_logs = self.run_dbt_and_capture(
            ['--debug', '--log-format=json', 'run'],
            expect_pass=expect_pass
        )

        parsed_logs = []
        for line in raw_logs.split('\n'):
            try:
                log = json.loads(line)
            except ValueError:
                continue

            parsed_logs.append(log)
        
        # empty lists evaluate as False
        self.assertTrue(parsed_logs)
        return parsed_logs

    def query_comment(self, model_name, log):
        # N.B: a temporary string replacement regex to strip the HH:MM:SS from the log line if present.
        # TODO: make this go away when structured logging is stable 
        log_msg = re.sub("(?:[01]\d|2[0123]):(?:[012345]\d):(?:[012345]\d \| )", "", log['msg'])
        prefix = 'On {}: '.format(model_name)
        if log_msg.startswith(prefix):
            msg = log_msg[len(prefix):]
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

        self.assertTrue(seen, 'Never saw a matching log message! Logs:\n{}'.format('\n'.join(l['msg'] for l in logs)))

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
