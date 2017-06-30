SCDArchiveTemplate = u"""

    with "current_data" as (

        select
            {% for col in get_columns_in_table(source_schema, source_table) %}
                "{{ col.name }}" {% if not loop.last %},{% endif %}
            {% endfor %},
            {{ updated_at }} as "dbt_updated_at",
            {{ unique_key }} as "dbt_pk",
            {{ updated_at }} as "valid_from",
            null::timestamp as "tmp_valid_to"
        from "{{ source_schema }}"."{{ source_table }}"

    ),

    "archived_data" as (

        select
            {% for col in get_columns_in_table(source_schema, source_table) %}
                "{{ col.name }}" {% if not loop.last %},{% endif %}
            {% endfor %},
            {{ updated_at }} as "dbt_updated_at",
            {{ unique_key }} as "dbt_pk",
            "valid_from",
            "valid_to" as "tmp_valid_to"
        from "{{ target_schema }}"."{{ target_table }}"

    ),

    "insertions" as (

        select
            "current_data".*,
            null::timestamp as "valid_to"
        from "current_data"
        left outer join "archived_data"
          on "archived_data"."dbt_pk" = "current_data"."dbt_pk"
        where "archived_data"."dbt_pk" is null or (
          "archived_data"."dbt_pk" is not null and
          "current_data"."dbt_updated_at" > "archived_data"."dbt_updated_at" and
          "archived_data"."tmp_valid_to" is null
        )
    ),

    "updates" as (

        select
            "archived_data".*,
            "current_data"."dbt_updated_at" as "valid_to"
        from "current_data"
        left outer join "archived_data"
          on "archived_data"."dbt_pk" = "current_data"."dbt_pk"
        where "archived_data"."dbt_pk" is not null
          and "archived_data"."dbt_updated_at" < "current_data"."dbt_updated_at"
          and "archived_data"."tmp_valid_to" is null
    ),

    "merged" as (

      select *, 'update' as "change_type" from "updates"
      union all
      select *, 'insert' as "change_type" from "insertions"

    )

    select *,
        md5("dbt_pk" || '|' || "dbt_updated_at") as "scd_id"
    from "merged"
"""


class ArchiveInsertTemplate(object):

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

update "{schema}"."{identifier}" set "valid_to" = "tmp"."valid_to"
from "{identifier}__dbt_archival_tmp" as "tmp"
where "tmp"."scd_id" = "{schema}"."{identifier}"."scd_id"
  and "change_type" = 'update';

insert into "{schema}"."{identifier}" (
    {dest_cols}
)
select {dest_cols} from "{identifier}__dbt_archival_tmp"
where "change_type" = 'insert';
"""

    def wrap(self, schema, table, query, unique_key):
        sql = self.archival_template.format(schema=schema, identifier=table, query=query, unique_key=unique_key, alter_template=self.alter_template, dest_cols=self.dest_cols, definitions=self.definitions)
        return sql
