{% set invalid = None %}
select '{{ invalid.value or "hello" }}' as value
