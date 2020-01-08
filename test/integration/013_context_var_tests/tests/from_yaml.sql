{% set simplest = (fromyaml('a: 1') == {'a': 1}) %}
{% set nested_data %}
a:
  b:
   - c: 1
     d: 2
   - c: 3
     d: 4
{% endset %}
{% set nested = (fromyaml(nested_data) == {'a': {'b': [{'c': 1, 'd': 2}, {'c': 3, 'd': 4}]}}) %}

(select 'simplest' as name {% if simplest %}limit 0{% endif %})
union all
(select 'nested' as name {% if nested %}limit 0{% endif %})
