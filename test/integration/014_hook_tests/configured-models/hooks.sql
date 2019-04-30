
{{
    config({
        "pre_hook": "\
            insert into {{this.schema}}.on_model_hook (\
                \"state\",\
                \"target.dbname\",\
                \"target.host\",\
                \"target.name\",\
                \"target.schema\",\
                \"target.type\",\
                \"target.user\",\
                \"target.pass\",\
                \"target.port\",\
                \"target.threads\",\
                \"run_started_at\",\
                \"invocation_id\"\
            ) VALUES (\
                'start',\
                '{{ target.dbname }}',\
                '{{ target.host }}',\
                '{{ target.name }}',\
                '{{ target.schema }}',\
                '{{ target.type }}',\
                '{{ target.user }}',\
                '{{ target.get(\"pass\", \"\") }}',\
                {{ target.port }},\
                {{ target.threads }},\
                '{{ run_started_at }}',\
                '{{ invocation_id }}'\
        )",
        "post-hook": "\
            insert into {{this.schema}}.on_model_hook (\
                \"state\",\
                \"target.dbname\",\
                \"target.host\",\
                \"target.name\",\
                \"target.schema\",\
                \"target.type\",\
                \"target.user\",\
                \"target.pass\",\
                \"target.port\",\
                \"target.threads\",\
                \"run_started_at\",\
                \"invocation_id\"\
            ) VALUES (\
                'end',\
                '{{ target.dbname }}',\
                '{{ target.host }}',\
                '{{ target.name }}',\
                '{{ target.schema }}',\
                '{{ target.type }}',\
                '{{ target.user }}',\
                '{{ target.get(\"pass\", \"\") }}',\
                {{ target.port }},\
                {{ target.threads }},\
                '{{ run_started_at }}',\
                '{{ invocation_id }}'\
            )"
    })
}}

select 3 as id
