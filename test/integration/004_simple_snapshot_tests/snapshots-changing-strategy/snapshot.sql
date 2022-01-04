
{#
    REPRO:
        1. Run with check strategy
        2. Add a new ts column and run with check strategy
        3. Run with timestamp strategy on new ts column

        Expect: new entry is added for changed rows in (3)
#}


{% snapshot my_snapshot %}

    {#--------------- Configuration ------------ #}

    {{ config(
        target_schema=schema,
        unique_key='id'
    ) }}

    {% if var('strategy') == 'timestamp' %}
        {{ config(strategy='timestamp', updated_at='updated_at') }}
    {% else %}
        {{ config(strategy='check', check_cols=['color']) }}
    {% endif %}

    {#--------------- Test setup ------------ #}

    {% if var('step') == 1 %}

        select 1 as id, 'blue' as color
        union all
        select 2 as id, 'red' as color

    {% elif var('step') == 2 %}

        -- change id=1 color from blue to green
        -- id=2 is unchanged when using the check strategy
        select 1 as id, 'green' as color, '2020-01-01'::date as updated_at
        union all
        select 2 as id, 'red' as color, '2020-01-01'::date as updated_at

    {% elif var('step') == 3 %}

        -- bump timestamp for both records. Expect that after this runs
        -- using the timestamp strategy, both ids should have the color
        -- 'pink' in the database. This should be in the future b/c we're
        -- going to compare to the check timestamp, which will be _now_
        select 1 as id, 'pink' as color, (now() + interval '1 day')::date as updated_at
        union all
        select 2 as id, 'pink' as color, (now() + interval '1 day')::date as updated_at

    {% endif %}

{% endsnapshot %}
