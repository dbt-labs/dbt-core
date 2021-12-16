{# surely there is a better way to do this! #}

{% for _ in range(1, 5) %}
select {{ loop.index }} as id
{% if not loop.last %}union all{% endif %}
{% endfor %}
