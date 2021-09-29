{% set blacklist = ['pass', 'password', 'keyfile', 'keyfile.json', 'password', 'private_key_passphrase'] %}
{% for key in blacklist %}
  {% if key in blacklist and blacklist[key] %}
  	{% do exceptions.raise_compiler_error('invalid target, found banned key "' ~ key ~ '"') %}
  {% endif %}
{% endfor %}

{% if 'type' not in target %}
  {% do exceptions.raise_compiler_error('invalid target, missing "type"') %}
{% endif %}

{% set required = ['name', 'schema', 'type', 'threads'] %}

{# Require what we document at https://docs.getdbt.com/docs/target #}
	{% do required.extend(['project']) %}
{% endif %}

{% for value in required %}
	{% if value not in target %}
  		{% do exceptions.raise_compiler_error('invalid target, missing "' ~ value ~ '"') %}
	{% endif %}
{% endfor %}

{% do run_query('select 2 as inner_id') %}
select 1 as outer_id
