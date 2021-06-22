{% macro datediff(first_date, second_date, datepart) %}
  {{ return(adapter.dispatch('datediff', 'local_utils')(first_date, second_date, datepart)) }}
{% endmacro %}


{% macro default__datediff(first_date, second_date, datepart) %}

    datediff(
        {{ datepart }},
        {{ first_date }},
        {{ second_date }}
        )

{% endmacro %}


{% macro postgres__datediff(first_date, second_date, datepart) %}

    {% if datepart == 'year' %}
        (date_part('year', ({{second_date}})::date) - date_part('year', ({{first_date}})::date))
    {% elif datepart == 'quarter' %}
        ({{ local_utils.datediff(first_date, second_date, 'year') }} * 4 + date_part('quarter', ({{second_date}})::date) - date_part('quarter', ({{first_date}})::date))
    {% else %}
        ( 1000 )
    {% endif %}

{% endmacro %}

