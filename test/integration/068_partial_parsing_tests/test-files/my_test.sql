select
   * from {{ ref('customers') }} where first_name = '{{ macro_something() }}'
