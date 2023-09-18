{% test even_rows(model, column_name) %}

WITH row_count as (

    SELECT
        COUNT({{ column_name }}) cnt
    FROM
        {{ model }}
)
SELECT
  1
FROM
  row_count
WHERE
  MOD(cnt, 2) != 0

{% endtest %}
