
class BaseCreateTemplate(object):
    template = u"""
create {materialization} "{schema}"."{identifier}" {dist_qualifier} {sort_qualifier} as (
    {query}
);"""

    incremental_template = u"""
{{% if not already_exists("{schema}", "{identifier}") %}}

create table "{schema}"."{identifier}" {dist_qualifier} {sort_qualifier} as (
    {query}
);


{{% else %}}

create temporary table "{identifier}__dbt_incremental_tmp" as (
    with dbt_incr_sbq as (
        {query}
    )
    select * from dbt_incr_sbq
    where ({sql_where}) or ({sql_where}) is null
);

-- DBT_OPERATION {{ function: expand_column_types_if_needed, args: {{ temp_table: "{identifier}__dbt_incremental_tmp", to_schema: "{schema}", to_table: "{identifier}"}} }}

{{% set dest_columns = get_columns_in_table("{schema}", "{identifier}") %}}
{{% set dest_cols_csv = dest_columns | map(attribute='quoted') | join(', ') %}}

{incremental_delete_statement}

insert into "{schema}"."{identifier}" ({{{{ dest_cols_csv }}}})
(
    select {{{{ dest_cols_csv }}}}
    from "{identifier}__dbt_incremental_tmp"
);

{{% endif %}}

"""

    incremental_delete_template = u"""
delete from "{schema}"."{identifier}" where  ({unique_key}) in (
    select ({unique_key}) from "{identifier}__dbt_incremental_tmp"
);
"""

    extras_template = u"""
{prologue}

-- Pre-model hooks
{pre_hooks}

-- Model SQL
{sql}

-- Post-model hooks
{post_hooks}
"""

    label = "build"

    @classmethod
    def model_name(cls, base_name):
        return base_name

    def add_extras(self, opts, sql):
        pre_hooks = ';\n\n'.join(opts['pre-hooks'])
        post_hooks = ';\n\n'.join(opts['post-hooks'])

        if len(pre_hooks) > 0:
            pre_hooks = pre_hooks + ';'

        if len(post_hooks) > 0:
            post_hooks = post_hooks + ';'

        extras = {
            'prologue': opts['prologue'],
            'pre_hooks': pre_hooks,
            'sql': sql,
            'post_hooks': post_hooks,
        }

        return self.extras_template.format(**extras)

    def wrap(self, opts):
        sql = ""

        if opts['materialization'] == 'view':
            sql = self.template.format(**opts)

        elif (opts['materialization'] == 'table' and
              not opts['non_destructive']):
            sql = self.template.format(**opts)

        elif opts['materialization'] == 'table' and opts['non_destructive']:
            opts['incremental_delete_statement'] = "-- non-destructive insert... skippping delete"
            opts['sql_where'] = 'TRUE'

            sql = self.incremental_template.format(**opts)

        elif opts['materialization'] == 'incremental':
            if opts.get('unique_key') is not None:
                delete_sql = self.incremental_delete_template.format(**opts)
            else:
                delete_sql = "-- no unique key provided... skipping delete"

            opts['incremental_delete_statement'] = delete_sql
            sql = self.incremental_template.format(**opts)


        elif opts['materialization'] == 'ephemeral':
            sql = opts['query']
        else:
            raise RuntimeError("Invalid materialization parameter ({})".format(opts['materialization']))

        return self.add_extras(opts, sql)


class DryCreateTemplate(object):
    base_template = u"""
create view "{schema}"."{identifier}" as (
    SELECT * FROM (
        {query}
    ) as tmp LIMIT 0
);"""


    incremental_template = u"""
create table "{schema}"."{identifier}" {dist_qualifier} {sort_qualifier} as (
    select * from (
        {query}
    ) s limit 0
);
    """

    incremental_delete_template = u"""
delete from "{schema}"."{identifier}" where  ({unique_key}) in (
    select ({unique_key}) from "{identifier}__dbt_incremental_tmp"
);
"""

    label = "test"

    @classmethod
    def model_name(cls, base_name):
        return 'test_{}'.format(base_name)

    def wrap(self, opts):
        sql = ""
        if opts['materialization'] in ('table', 'view'):
            sql = self.base_template.format(**opts)
        elif opts['materialization'] == 'incremental':
            if opts.get('unique_key') is not None:
                delete_sql = self.incremental_delete_template.format(**opts)
            else:
                delete_sql = "-- no unique key provided... skipping delete"

            opts['incremental_delete_statement'] = delete_sql
            sql = self.incremental_template.format(**opts)

        elif opts['materialization'] == 'ephemeral':
            sql = opts['query']
        else:
            raise RuntimeError("Invalid materialization parameter ({})".format(opts['materialization']))

        return "{}\n\n{}".format(opts['prologue'], sql)


SCDArchiveTemplate = u"""

    with current_data as (

        select
            {% raw %}
                {% for col in get_columns_in_table(source_schema, source_table) %}
                    "{{ col.name }}" {% if not loop.last %},{% endif %}
                {% endfor %},
            {% endraw %}
            {{ updated_at }} as dbt_updated_at,
            {{ unique_key }} as dbt_pk,
            {{ updated_at }} as valid_from,
            null::timestamp as tmp_valid_to
        from "{{ source_schema }}"."{{ source_table }}"

    ),

    archived_data as (

        select
            {% raw %}
                {% for col in get_columns_in_table(source_schema, source_table) %}
                    "{{ col.name }}" {% if not loop.last %},{% endif %}
                {% endfor %},
            {% endraw %}
            {{ updated_at }} as dbt_updated_at,
            {{ unique_key }} as dbt_pk,
            valid_from,
            valid_to as tmp_valid_to
        from "{{ target_schema }}"."{{ target_table }}"

    ),

    insertions as (

        select
            current_data.*,
            null::timestamp as valid_to
        from current_data
        left outer join archived_data on archived_data.dbt_pk = current_data.dbt_pk
        where archived_data.dbt_pk is null or (
          archived_data.dbt_pk is not null and
          current_data.dbt_updated_at > archived_data.dbt_updated_at and
          archived_data.tmp_valid_to is null
        )
    ),

    updates as (

        select
            archived_data.*,
            current_data.dbt_updated_at as valid_to
        from current_data
        left outer join archived_data on archived_data.dbt_pk = current_data.dbt_pk
        where archived_data.dbt_pk is not null
          and archived_data.dbt_updated_at < current_data.dbt_updated_at
          and archived_data.tmp_valid_to is null
    ),

    merged as (

      select *, 'update' as change_type from updates
      union all
      select *, 'insert' as change_type from insertions

    )

    select *,
        md5(dbt_pk || '|' || dbt_updated_at) as scd_id
    from merged
"""


class ArchiveInsertTemplate(object):

    label = "archive"


    # missing_columns : columns in source_table that are missing from target_table (used for the ALTER)
    # dest_columns    : columns in the dest table (post-alter!)
    definitions = u"""
{% set missing_columns = get_missing_columns(source_schema, source_table, target_schema, target_table) %}
{% set dest_columns = get_columns_in_table(target_schema, target_table) + missing_columns %}
"""

    alter_template = u"""
{% for col in missing_columns %}
    alter table "{{ target_schema }}"."{{ target_table }}" add column "{{ col.name }}" {{ col.data_type }};
{% endfor %}
"""

    dest_cols = u"""
{% for col in dest_columns %}
    "{{ col.name }}" {% if not loop.last %},{% endif %}
{% endfor %}
"""

    archival_template = u"""

{definitions}

{alter_template}

create temporary table "{identifier}__dbt_archival_tmp" as (
    with dbt_archive_sbq as (
        {query}
    )
    select * from dbt_archive_sbq
);

-- DBT_OPERATION {{ function: expand_column_types_if_needed, args: {{ temp_table: "{identifier}__dbt_archival_tmp", to_schema: "{schema}", to_table: "{identifier}"}} }}

update "{schema}"."{identifier}" set valid_to = tmp.valid_to
from "{identifier}__dbt_archival_tmp" as tmp
where tmp.scd_id = "{schema}"."{identifier}".scd_id
  and change_type = 'update';

insert into "{schema}"."{identifier}" (
    {dest_cols}
)
select {dest_cols} from "{identifier}__dbt_archival_tmp"
where change_type = 'insert';
"""

    def wrap(self, schema, table, query, unique_key):
        sql = self.archival_template.format(schema=schema, identifier=table, query=query, unique_key=unique_key, alter_template=self.alter_template, dest_cols=self.dest_cols, definitions=self.definitions)
        return sql
