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

# Overrides should not cause duplicate key warnings on the data_tests key below.
override__yml = """
version: 2

models:
  - name: my_first_dbt_model
    description: &description
      "A starter dbt model"
    columns:
      - &copy_me
        name: id
        description: The ID.
        data_tests:
          - not_null

  - name: my_second_dbt_model
    description: *description
    columns:
      - <<: *copy_me
        data_tests:
          - unique
"""

override_with_issue__yml = """
version: 2

models:
  - name: my_first_dbt_model
    description: &description
      "A starter dbt model"
    columns:
      - &copy_me
        name: id
        description: The ID.
        data_tests:
          - not_null

  - name: my_second_dbt_model
    description: *description
    columns:
      - <<: *copy_me
        data_tests:
          - unique
        data_tests:
          - unique
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

    override_dupes_issues = checked_load(override__yml)[1]
    assert len(override_dupes_issues) == 0

    # This currently fails. We are not checking for genuine duplicate keys
    # in override anchors.
    # real_override_dupes_issues = checked_load(override__yml)[1]
    # assert len(real_override_dupes_issues) == 1
