select * from {{ source('test_source', 'test_table') }}
