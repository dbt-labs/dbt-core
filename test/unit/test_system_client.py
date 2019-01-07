import os
import shutil
import stat
import sys
import unittest
from tempfile import mkdtemp

from dbt.exceptions import ExecutableError, WorkingDirectoryError, \
    CommandResultError
import dbt.clients.system

if os.name == 'nt':
    TMPDIR = 'c:/Windows/TEMP'
else:
    TMPDIR = '/tmp'

profiles_path = '{}/profiles.yml'.format(TMPDIR)

class SystemClient(unittest.TestCase):

    def set_up_profile(self):
        with open(profiles_path, 'w') as f:
            f.write('ORIGINAL_TEXT')

    def get_profile_text(self):
        with open(profiles_path, 'r') as f:
            return f.read()

    def tearDown(self):
        try:
            os.remove(profiles_path)
        except:
            pass

    def test__make_file_when_exists(self):
        self.set_up_profile()
        written = dbt.clients.system.make_file(profiles_path, contents='NEW_TEXT')

        self.assertFalse(written)
        self.assertEqual(self.get_profile_text(), 'ORIGINAL_TEXT')

    def test__make_file_when_not_exists(self):
        written = dbt.clients.system.make_file(profiles_path, contents='NEW_TEXT')

        self.assertTrue(written)
        self.assertEqual(self.get_profile_text(), 'NEW_TEXT')

    def test__make_file_with_overwrite(self):
        self.set_up_profile()
        written = dbt.clients.system.make_file(profiles_path, contents='NEW_TEXT', overwrite=True)

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
