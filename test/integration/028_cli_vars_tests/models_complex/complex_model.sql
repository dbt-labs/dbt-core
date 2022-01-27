
select
    '{{ var("variable_1") }}'::varchar as var_1,
    '{{ var("variable_2")[0] }}'::varchar as var_2,
    '{{ var("variable_3")["value"] }}'::varchar as var_3

