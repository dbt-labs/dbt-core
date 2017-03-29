
{% macro dbt__wrap(sql, pre_hooks, post_hooks) -%}

    -- Compiled by DBT

    {% for hook in pre_hooks %}

        {{ hook }};

    {% endfor %}

    {{ sql }}

    {% for hook in post_hooks -%}

        {{ hook }};

    {% endfor %}

{% endmacro %}
