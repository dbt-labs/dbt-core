import re
import unittest


class SnowflakeConnectionsTest(unittest.TestCase):

    def test_comment_stripping_regex(self):
        pattern = r'(\".*?\"|\'.*?\')|(/\*.*?\*/|--[^\r\n]*$)'
        comment1 = '-- just comment'
        comment2 = '/* just comment */'
        query1 = 'select 1; -- comment'
        query2 = 'select 1; /* comment */'
        query3 = 'select 1; -- comment\nselect 2; /* comment */ '
        query4 = 'select \n1; -- comment\nselect \n2; /* comment */ '

        stripped_comment1 = re.sub(re.compile(pattern, re.MULTILINE),
                                   '', comment1).strip()

        stripped_comment2 = re.sub(re.compile(pattern, re.MULTILINE),
                                   '', comment2).strip()

        stripped_query1 = re.sub(re.compile(pattern, re.MULTILINE),
                                 '', query1).strip()

        stripped_query2 = re.sub(re.compile(pattern, re.MULTILINE),
                                 '', query2).strip()

        stripped_query3 = re.sub(re.compile(pattern, re.MULTILINE),
                                 '', query3).strip()

        stripped_query4 = re.sub(re.compile(pattern, re.MULTILINE),
                                 '', query4).strip()

        expected_query_3 = 'select 1; \nselect 2;'
        expected_query_4 = 'select \n1; \nselect \n2;'

        self.assertEqual('', stripped_comment1)
        self.assertEqual('', stripped_comment2)
        self.assertEqual('select 1;', stripped_query1)
        self.assertEqual('select 1;', stripped_query2)
        self.assertEqual(expected_query_3, stripped_query3)
        self.assertEqual(expected_query_4, stripped_query4)


if __name__ == '__main__':
    unittest.main()
