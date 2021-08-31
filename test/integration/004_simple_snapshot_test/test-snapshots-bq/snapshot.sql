{% snapshot snapshot_actual %}

    {{
        config(
            target_project=var('target_database', database),
            target_dataset=var('target_schema', schema),
            unique_key='concat(cast(id as string) , "-", first_name)',
            strategy='timestamp',
            updated_at='updated_at',
        )
    }}

    {% if var('invalidate_hard_deletes', 'false') | as_bool %}
        {{ config(invalidate_hard_deletes=True) }}
    {% endif %}

    select * from `{{target.database}}`.`{{schema}}`.seed

{% endsnapshot %}
