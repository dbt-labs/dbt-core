
class BaseCreateTemplate(object):
    template = """
create {materialization} "{schema}"."{identifier}" {dist_qualifier} {sort_qualifier} as (
    {query}
);"""

    incremental_template = """
create temporary table "{identifier}__dbt_incremental_tmp" {dist_qualifier} {sort_qualifier} as (
    select * from (
        {query}
    ) as tmp limit 0
);

create table if not exists "{schema}"."{identifier}" (like "{identifier}__dbt_incremental_tmp");

insert into "{schema}"."{identifier}" (
    with dbt_incr_sbq as (
        {query}
    )
    select * from dbt_incr_sbq
    where ({sql_where}) or ({sql_where}) is null
);
    """

    label = "build"

    @classmethod
    def model_name(cls, base_name):
        return base_name

    def wrap(self, opts):
        sql = ""
        if opts['materialization'] in ('table', 'view'):
            sql = self.template.format(**opts)
        elif opts['materialization'] == 'incremental':
            sql = self.incremental_template.format(**opts)
        elif opts['materialization'] == 'ephemeral':
            sql = opts['query']
        else:
            raise RuntimeError("Invalid materialization parameter ({})".format(opts['materialization']))

        return "{}\n\n{}".format(opts['prologue'], sql)

class TestCreateTemplate(object):
    template = """
create view "{schema}"."{identifier}" as (
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


