select * from {{ ref('original') }}, {{ source('test_copy_several_tables', 'additional') }}
