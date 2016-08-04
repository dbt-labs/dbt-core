
class BaseCreateTemplate(object):
    template = """
    create {materialization} "{schema}"."{identifier}" {dist_qualifier} {sort_qualifier} as (
        {query}
    );"""

    incremental_template = """
    create temporary table "{identifier}__dbt_incremental_tmp" {dist_qualifier} {sort_qualifier} as (
        SELECT * FROM (
            {query}
        ) as tmp LIMIT 0
    );

    create table if not exists "{schema}"."{identifier}" (like "{identifier}__dbt_incremental_tmp");

    insert into "{schema}"."{identifier}" (
        with dbt_inc_sbq as (
            select max({sql_field}) as __dbt_max from "{schema}"."{identifier}"
        ), dbt_raw_sbq as (
            {query}
        )
        select dbt_raw_sbq.* from dbt_raw_sbq
        join dbt_inc_sbq on {sql_field} > dbt_inc_sbq.__dbt_max or dbt_inc_sbq.__dbt_max is null
        order by {sql_field}
    );
    """

    label = "build"

    @classmethod
    def model_name(cls, base_name):
        return base_name

    def wrap(self, opts):
        if opts['materialization'] in ('table', 'view'):
            return self.template.format(**opts)
        elif opts['materialization'] == 'incremental':
            return self.incremental_template.format(**opts)
        else:
            raise RuntimeError("Invalid materialization parameter ({})".format(opts['materialization']))

class TestCreateTemplate(object):
    template = """
    create view "{schema}"."{identifier}" {dist_qualifier} {sort_qualifier} as (
        SELECT * FROM (
            {query}
        ) as tmp LIMIT 0
    );"""

    label = "test"

    @classmethod
    def model_name(cls, base_name):
        return 'test_{}'.format(base_name)

    def wrap(self, opts):
        return self.template.format(**opts)


