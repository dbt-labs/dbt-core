{{ config(tags = ['data_test_tag']) }}

select * from {{ ref('model_a') }}
where false
