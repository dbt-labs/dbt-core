select
   * from {{ ref('customers') }} where customer_id > 100
