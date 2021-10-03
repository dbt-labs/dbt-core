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

{# Require what we docuement at https://docs.getdbt.com/docs/target #}
{% if target.type == 'postgres' %}
	{% do required.extend(['dbname', 'host', 'user', 'port']) %}
{% else %}
  {% do exceptions.raise_compiler_error('invalid target, got unknown type "' ~ target.type ~ '"') %}
{% endif %}

{% for value in required %}
	{% if value not in target %}
  		{% do exceptions.raise_compiler_error('invalid target, missing "' ~ value ~ '"') %}
	{% endif %}
{% endfor %}

{% do run_query('select 2 as inner_id') %}
select 1 as outer_id
