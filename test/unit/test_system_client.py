import os
import unittest

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
