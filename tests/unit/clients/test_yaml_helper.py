from dbt.clients.checked_load import checked_load

no_dupe__yml = """
a:
  b: 1

b:
  a: 1
"""

top_level_dupe__yml = """
a:
  b: 1

a:
  c: 1
  d: 2
  e: 3
"""

nested_dupe__yml = """
a:
  b: 1

c:
  d: 1
  e: 2
  d: 3
"""

multiple_dupes__yml = """
a:
  b:
    c: 1

d:
  e:
    f: 1
    g: 2
    f: 3
    h: 4
    f: 5
"""


def test_checked_load():

    no_dupe_issues = checked_load(no_dupe__yml)[1]
    assert no_dupe_issues == []

    top_level_dupe_issues = checked_load(top_level_dupe__yml)[1]
    assert len(top_level_dupe_issues) == 1
    nested_dupe_issues = checked_load(nested_dupe__yml)[1]
    assert len(nested_dupe_issues) == 1
    multiple_dupes_issues = checked_load(multiple_dupes__yml)[1]
    assert len(multiple_dupes_issues) == 2
