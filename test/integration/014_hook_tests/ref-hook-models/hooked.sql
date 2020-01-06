{{
    config({
        "pre_hook": "\
            insert into {{this.schema}}.on_model_hook select
                state,
                '{{ target.dbname }}' as \"target.dbname\",\
                '{{ target.host }}' as \"target.host\",\
                '{{ target.name }}' as \"target.name\",\
                '{{ target.schema }}' as \"target.schema\",\
                '{{ target.type }}' as \"target.type\",\
                '{{ target.user }}' as \"target.user\",\
                '{{ target.get(\"pass\", \"\") }}' as \"target.pass\",\
                {{ target.port }} as \"target.port\",\
                {{ target.threads }} as \"target.threads\",\
                '{{ run_started_at }}' as \"run_started_at\",\
                '{{ invocation_id }}' as \"invocation_id\"\
                from {{ ref('pre') }}\
        "
    })
}}
select 1 as id
