{{
    config(
        tags=["im_code"]
    )
}}

SELECT
    sum(id) id_sum
FROM
    {{ ref('my_first_dbt_model') }}
WHERE
    id != 1
HAVING
    MOD(id_sum, 2) != 0
