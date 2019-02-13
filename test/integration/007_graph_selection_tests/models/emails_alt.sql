select distinct email from {{ ref('users') }}
