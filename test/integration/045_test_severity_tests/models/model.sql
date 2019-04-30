select * from {{ source('source', 'nulls') }}
