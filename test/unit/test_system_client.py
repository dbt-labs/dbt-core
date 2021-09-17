import os
import shutil
import stat
import unittest
from tempfile import mkdtemp, NamedTemporaryFile

from dbt.exceptions import ExecutableError, WorkingDirectoryError
import dbt.clients.system


class SystemClient(unittest.TestCase):
    def setUp(self):
        super().setUp()
        self.tmp_dir = mkdtemp()
        self.profiles_path = '{}/profiles.yml'.format(self.tmp_dir)

    def set_up_profile(self):
        with open(self.profiles_path, 'w') as f:
            f.write('ORIGINAL_TEXT')

    def get_profile_text(self):
        with open(self.profiles_path, 'r') as f:
            return f.read()

    def tearDown(self):
        try:
            shutil.rmtree(self.tmp_dir)
        except:
            pass

    def test__make_file_when_exists(self):
        self.set_up_profile()
        written = dbt.clients.system.make_file(self.profiles_path, contents='NEW_TEXT')

        self.assertFalse(written)
        self.assertEqual(self.get_profile_text(), 'ORIGINAL_TEXT')

    def test__make_file_when_not_exists(self):
        written = dbt.clients.system.make_file(self.profiles_path, contents='NEW_TEXT')

        self.assertTrue(written)
        self.assertEqual(self.get_profile_text(), 'NEW_TEXT')

    def test__make_file_with_overwrite(self):
        self.set_up_profile()
        written = dbt.clients.system.make_file(self.profiles_path, contents='NEW_TEXT', overwrite=True)

        self.assertTrue(written)
        self.assertEqual(self.get_profile_text(), 'NEW_TEXT')


class TestRunCmd(unittest.TestCase):
    """Test `run_cmd`.

    Don't mock out subprocess, in order to expose any OS-level differences.
    """
    not_a_file = 'zzzbbfasdfasdfsdaq'
    def setUp(self):
        self.tempdir = mkdtemp()
        self.run_dir = os.path.join(self.tempdir, 'run_dir')
        self.does_not_exist = os.path.join(self.tempdir, 'does_not_exist')
        self.empty_file = os.path.join(self.tempdir, 'empty_file')
        if os.name == 'nt':
            self.exists_cmd = ['cmd', '/C', 'echo', 'hello']
        else:
            self.exists_cmd = ['echo', 'hello']


        os.mkdir(self.run_dir)
        with open(self.empty_file, 'w') as fp:
            pass  # "touch"

    def tearDown(self):
        shutil.rmtree(self.tempdir)

    def test__executable_does_not_exist(self):
        with self.assertRaises(ExecutableError) as exc:
            dbt.clients.system.run_cmd(self.run_dir, [self.does_not_exist])

        msg = str(exc.exception).lower()

        self.assertIn('path', msg)
        self.assertIn('could not find', msg)
        self.assertIn(self.does_not_exist.lower(), msg)

    def test__not_exe(self):
        with self.assertRaises(ExecutableError) as exc:
            dbt.clients.system.run_cmd(self.run_dir, [self.empty_file])

        msg = str(exc.exception).lower()
        if os.name == 'nt':
            # on windows, this means it's not an executable at all!
            self.assertIn('not executable', msg)
        else:
            # on linux, this means you don't have executable permissions on it
            self.assertIn('permissions', msg)
        self.assertIn(self.empty_file.lower(), msg)

    def test__cwd_does_not_exist(self):
        with self.assertRaises(WorkingDirectoryError) as exc:
            dbt.clients.system.run_cmd(self.does_not_exist, self.exists_cmd)
        msg = str(exc.exception).lower()
        self.assertIn('does not exist', msg)
        self.assertIn(self.does_not_exist.lower(), msg)

    def test__cwd_not_directory(self):
        with self.assertRaises(WorkingDirectoryError) as exc:
            dbt.clients.system.run_cmd(self.empty_file, self.exists_cmd)

        msg = str(exc.exception).lower()
        self.assertIn('not a directory', msg)
        self.assertIn(self.empty_file.lower(), msg)

    def test__cwd_no_permissions(self):
        # it would be nice to add a windows test. Possible path to that is via
        # `psexec` (to get SYSTEM privs), use `icacls` to set permissions on
        # the directory for the test user. I'm pretty sure windows users can't
        # create files that they themselves cannot access.
        if os.name == 'nt':
            return

        # read-only -> cannot cd to it
        os.chmod(self.run_dir, stat.S_IRUSR)

        with self.assertRaises(WorkingDirectoryError) as exc:
            dbt.clients.system.run_cmd(self.run_dir, self.exists_cmd)

        msg = str(exc.exception).lower()
        self.assertIn('permissions', msg)
        self.assertIn(self.run_dir.lower(), msg)

    def test__ok(self):
        out, err = dbt.clients.system.run_cmd(self.run_dir, self.exists_cmd)
        self.assertEqual(out.strip(), b'hello')
        self.assertEqual(err.strip(), b'')


class TestFindMatching(unittest.TestCase):

    def setUp(self):
        self.base_dir = mkdtemp()
        self.tempdir = mkdtemp(dir=self.base_dir)

    def test_find_matching_lowercase_file_pattern(self):
        with NamedTemporaryFile(
            prefix='sql-files', suffix='.sql', dir=self.tempdir
        ) as named_file:
            file_path = os.path.dirname(named_file.name)
            relative_path = os.path.basename(file_path)
            out = dbt.clients.system.find_matching(
                self.base_dir, [relative_path], '*.sql'
            )
            expected_output = [{
                'searched_path': relative_path,
                'absolute_path': named_file.name,
                'relative_path': os.path.basename(named_file.name),
                'modification_time': out[0]['modification_time'],
            }]
            self.assertEqual(out, expected_output)

    def test_find_matching_uppercase_file_pattern(self):
        with NamedTemporaryFile(prefix='sql-files', suffix='.SQL', dir=self.tempdir) as named_file:
            file_path = os.path.dirname(named_file.name)
            relative_path = os.path.basename(file_path)
            out = dbt.clients.system.find_matching(
                self.base_dir, [relative_path], '*.sql'
            )
            expected_output = [{
                'searched_path': relative_path,
                'absolute_path': named_file.name,
                'relative_path': os.path.basename(named_file.name),
                'modification_time': out[0]['modification_time'],
            }]
            self.assertEqual(out, expected_output)

    def test_find_matching_file_pattern_not_found(self):
        with NamedTemporaryFile(
            prefix='sql-files', suffix='.SQLT', dir=self.tempdir
        ):
            out = dbt.clients.system.find_matching(self.tempdir, [''], '*.sql')
            self.assertEqual(out, [])

    def tearDown(self):
        try:
            shutil.rmtree(self.base_dir)
        except:
            pass
