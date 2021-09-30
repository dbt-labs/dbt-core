select * from {{ ref('seed') }}

-- establish a macro dependency that trips infinite recursion if not handled
-- depends on: {{ my_infinitely_recursive_macro() }}