import pytest

from dbt.tests.util import run_dbt

wizards_csv = """id,w_name,email,email_tld,phone,world
1,Albus Dumbledore,a.dumbledore@gmail.com,gmail.com,813-456-9087,1
2,Gandalf,gandy811@yahoo.com,yahoo.com,551-329-8367,2
3,Winifred Sanderson,winnie@hocuspocus.com,hocuspocus.com,,6
4,Marnie Piper,cromwellwitch@gmail.com,gmail.com,,5
5,Grace Goheen,grace.goheen@dbtlabs.com,dbtlabs.com,,3
6,Glinda,glinda_good@hotmail.com,hotmail.com,912-458-3289,4
"""

top_level_email_domains_csv = """tld
gmail.com
yahoo.com
hocuspocus.com
dbtlabs.com
hotmail.com
"""

worlds_csv = """id,name
1,The Wizarding World
2,Middle-earth
3,dbt Labs
4,Oz
5,Halloweentown
6,Salem
"""

stg_wizards_sql = """
select
    id as wizard_id,
    w_name as wizard_name,
    email,
    email_tld as email_top_level_domain,
    phone as phone_number,
    world as world_id
from {{ ref('wizards') }}
"""

stg_worlds_sql = """
select
    id as world_id,
    name as world_name
from {{ ref('worlds') }}
"""

dim_wizards_sql = """
with wizards as (

    select * from {{ ref('stg_wizards') }}

),

worlds as (

    select * from {{ ref('stg_worlds') }}

),

accepted_email_domains as (

    select * from {{ ref('top_level_email_domains') }}

),

check_valid_emails as (

    select
        wizards.wizard_id,
        wizards.wizard_name,
        wizards.email,
        wizards.phone_number,
        wizards.world_id,

        coalesce (
            wizards.email ~ '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}$'
        = true
        and accepted_email_domains.tld is not null,
        false) as is_valid_email_address

    from wizards
    left join accepted_email_domains
        on wizards.email_top_level_domain = lower(accepted_email_domains.tld)

)

select
    check_valid_emails.wizard_id,
    check_valid_emails.wizard_name,
    check_valid_emails.email,
    check_valid_emails.is_valid_email_address,
    check_valid_emails.phone_number,
    worlds.world_name
from check_valid_emails
left join worlds
    on check_valid_emails.world_id = worlds.world_id
"""

orig_schema_yml = """
unit_tests:
  - name: test_valid_email_address
    model: dim_wizards
    given:
      - input: ref('stg_wizards')
        rows:
          - {email: cool@example.com,     email_top_level_domain: example.com}
          - {email: cool@unknown.com,     email_top_level_domain: unknown.com}
          - {email: badgmail.com,         email_top_level_domain: gmail.com}
          - {email: missingdot@gmailcom,  email_top_level_domain: gmail.com}
      - input: ref('top_level_email_domains')
        rows:
          - {tld: example.com}
          - {tld: gmail.com}
      - input: ref('stg_worlds')
        rows: []
    expect:
      rows:
        - {email: cool@example.com,    is_valid_email_address: true}
        - {email: cool@unknown.com,    is_valid_email_address: false}
        - {email: badgmail.com,        is_valid_email_address: false}
        - {email: missingdot@gmailcom, is_valid_email_address: false}
"""

schema_yml = """
unit_tests:
  - name: test_valid_email_address
    model: dim_wizards
    given:
      - input: ref('stg_wizards')
        format: sql
        rows: |
          select 1 as wizard_id, 'joe' as wizard_name, 'cool@example.com' as email, 'example.com' as email_top_level_domain, '123' as phone_number, 1 as world_id  union all
          select 2 as wizard_id, 'don' as wizard_name, 'cool@unknown.com' as email, 'unknown.com' as email_top_level_domain, '456' as phone_number, 2 as world_id  union all
          select 3 as wizard_id, 'mary' as wizard_name, 'badgmail.com' as email, 'gmail.com' as email_top_level_domain, '789' as phone_number, 3 as world_id union all
          select 4 as wizard_id, 'jane' as wizard_name, 'missingdot@gmailcom' as email, 'gmail.com' as email_top_level_domain, '102' as phone_number, 4 as world_id
      - input: ref('top_level_email_domains')
        format: sql
        rows: |
          select 'example.com' as tld union all
          select 'gmail.com' as tld
      - input: ref('stg_worlds')
        rows: []
    expect:
      format: sql
      rows: |
        select 1 as wizard_id, 'joe' as wizard_name, 'cool@example.com' as email, true as is_valid_email_address, '123' as phone_number, null as world_name union all
        select 2 as wizard_id, 'don' as wizard_name, 'cool@unknown.com' as email, false as is_valid_email_address, '456' as phone_number, null as world_name  union all
        select 3 as wizard_id, 'mary' as wizard_name, 'badgmail.com' as email, false as is_valid_email_address, '789' as phone_number, null as world_name union all
        select 4 as wizard_id, 'jane' as wizard_name, 'missingdot@gmailcom' as email, false as is_valid_email_address, '102' as phone_number, null as world_name
"""


class TestSQLFormat:
    @pytest.fixture(scope="class")
    def seeds(self):
        return {
            "wizards.csv": wizards_csv,
            "top_level_email_domains.csv": top_level_email_domains_csv,
            "worlds.csv": worlds_csv,
        }

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "stg_wizards.sql": stg_wizards_sql,
            "stg_worlds.sql": stg_worlds_sql,
            "dim_wizards.sql": dim_wizards_sql,
            "schema.yml": schema_yml,
        }

    def test_sql_format(self, project):
        results = run_dbt(["build"])
        assert len(results) == 7


stg_wizards_fixture_sql = """
    select 1 as wizard_id, 'joe' as wizard_name, 'cool@example.com' as email, 'example.com' as email_top_level_domain, '123' as phone_number, 1 as world_id  union all
    select 2 as wizard_id, 'don' as wizard_name, 'cool@unknown.com' as email, 'unknown.com' as email_top_level_domain, '456' as phone_number, 2 as world_id  union all
    select 3 as wizard_id, 'mary' as wizard_name, 'badgmail.com' as email, 'gmail.com' as email_top_level_domain, '789' as phone_number, 3 as world_id union all
    select 4 as wizard_id, 'jane' as wizard_name, 'missingdot@gmailcom' as email, 'gmail.com' as email_top_level_domain, '102' as phone_number, 4 as world_id
"""

top_level_email_domains_fixture_sql = """
    select 'example.com' as tld union all
    select 'gmail.com' as tld
"""

test_valid_email_address_fixture_sql = """
    select 1 as wizard_id, 'joe' as wizard_name, 'cool@example.com' as email, true as is_valid_email_address, '123' as phone_number, null as world_name union all
    select 2 as wizard_id, 'don' as wizard_name, 'cool@unknown.com' as email, false as is_valid_email_address, '456' as phone_number, null as world_name  union all
    select 3 as wizard_id, 'mary' as wizard_name, 'badgmail.com' as email, false as is_valid_email_address, '789' as phone_number, null as world_name union all
    select 4 as wizard_id, 'jane' as wizard_name, 'missingdot@gmailcom' as email, false as is_valid_email_address, '102' as phone_number, null as world_name
"""

fixture_schema_yml = """
unit_tests:
  - name: test_valid_email_address
    model: dim_wizards
    given:
      - input: ref('stg_wizards')
        format: sql
        fixture: stg_wizards_fixture
      - input: ref('top_level_email_domains')
        format: sql
        fixture: top_level_email_domains_fixture
      - input: ref('stg_worlds')
        rows: []
    expect:
      format: sql
      fixture: test_valid_email_address_fixture
"""


class TestSQLFormatFixtures:
    @pytest.fixture(scope="class")
    def tests(self):
        return {
            "fixtures": {
                "test_valid_email_address_fixture.sql": test_valid_email_address_fixture_sql,
                "top_level_email_domains_fixture.sql": top_level_email_domains_fixture_sql,
                "stg_wizards_fixture.sql": stg_wizards_fixture_sql,
            }
        }

    @pytest.fixture(scope="class")
    def seeds(self):
        return {
            "wizards.csv": wizards_csv,
            "top_level_email_domains.csv": top_level_email_domains_csv,
            "worlds.csv": worlds_csv,
        }

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "stg_wizards.sql": stg_wizards_sql,
            "stg_worlds.sql": stg_worlds_sql,
            "dim_wizards.sql": dim_wizards_sql,
            "schema.yml": fixture_schema_yml,
        }

    def test_sql_format_fixtures(self, project):
        results = run_dbt(["build"])
        assert len(results) == 7
