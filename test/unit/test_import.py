import unittest

class ImportTest(unittest.TestCase):

    def test_import_dbt_main(self):
        "just test that the project can be imported"
        import dbt.main
