import os

import dbt.targets

import psycopg2
import logging
import time


QUERY_VALIDATE_NOT_NULL = """
with validation as (
  select {field} as f
  from "{schema}"."{table}"
)
select count(*) from validation where f is null
"""

QUERY_VALIDATE_UNIQUE = """
with validation as (
  select {field} as f
  from "{schema}"."{table}"
),
validation_errors as (
    select f from validation group by f having count(*) > 1
)
select count(*) from validation_errors
"""

QUERY_VALIDATE_ACCEPTED_VALUES = """
with all_values as (
  select distinct {field} as f
  from "{schema}"."{table}"
),
validation_errors as (
    select f from all_values where f not in ({values_csv})
)
select count(*) from validation_errors
"""

QUERY_VALIDATE_REFERENTIAL_INTEGRITY = """
with parent as (
  select {parent_field} as id
  from "{schema}"."{parent_table}"
), child as (
  select {child_field} as id
  from "{schema}"."{child_table}"
)
select count(*) from child
where id not in (select id from parent) and id is not null
"""



class SchemaTester(object):
    def __init__(self, project):
        self.logger = logging.getLogger(__name__)
        self.project = project

    def get_target(self):
        target_cfg = self.project.run_environment()
        return dbt.targets.get_target(target_cfg)

    def execute_query(self, model, sql):
        target = self.get_target()

        with target.get_handle() as handle:
            with handle.cursor() as cursor:
                try:
                    self.logger.debug("SQL: %s", sql)
                    pre = time.time()
                    cursor.execute(sql)
                    post = time.time()
                    self.logger.debug("SQL status: %s in %d seconds", cursor.statusmessage, post-pre)
                except psycopg2.ProgrammingError as e:
                    self.logger.exception('programming error: %s', sql)
                    return e.diag.message_primary
                except Exception as e:
                    self.logger.exception('encountered exception while running: %s', sql)
                    e.model = model
                    raise e

                result = cursor.fetchone()
                if len(result) != 1:
                    self.logger.error("SQL: %s", sql)
                    self.logger.error("RESULT: %s", result)
                    raise RuntimeError("Unexpected validation result. Expected 1 record, got {}".format(len(result)))
                else:
                    return result[0]

    def validate_schema(self, schema_test):
            sql = schema_test.render()
            num_rows = self.execute_query(model, sql)
            if num_rows == 0:
                print("  OK")
                yield True
            else:
                print("  FAILED ({})".format(num_rows))
                yield False

    def test(self):
        pass
