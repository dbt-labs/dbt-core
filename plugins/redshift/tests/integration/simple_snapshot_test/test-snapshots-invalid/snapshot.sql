{# make sure to never name this anything with `target_schema` in the name, or the test will be invalid! #}
{% snapshot missing_field_target_underscore_schema %}
	{# missing the mandatory target_schema parameter #}
    {{
        config(
            unique_key='id || ' ~ "'-'" ~ ' || first_name',
            strategy='timestamp',
            updated_at='updated_at',
        )
    }}
    select * from {{target.database}}.{{schema}}.seed

{% endsnapshot %}
