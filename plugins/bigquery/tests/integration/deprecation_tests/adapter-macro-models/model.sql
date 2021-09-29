{% if some_macro('foo', 'bar') != 'foobar' %}
  {% do exceptions.raise_compiler_error('invalid foobar') %}
{% endif %}
select 1 as id
