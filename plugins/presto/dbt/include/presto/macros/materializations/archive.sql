
{% materialization archive, adapter='presto' -%}
  {{ exceptions.raise_not_implemented(
    'archive materialization not implemented for '+adapter.type())
  }}
{% endmaterialization %}
